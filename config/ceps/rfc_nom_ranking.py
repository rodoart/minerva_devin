import os

from libs.data_engineering_toolbox.path import HivePath
from ..job import sbx as job_config   # config del job (raíz HDFS de salidas, cohorte, lag, is_dynamic)
from pyspark.sql import Window
from pyspark.sql.functions import col


# Máximo de meses de historia CEP (SPEI) que maneja este pipeline; se usa como
# "history" en los dicts input/output: el intervalo de particiones cubre los
# últimos `history` meses terminando en (vintage - lag).
CEPS_MAXIMUM_HISTORY_IN_MONTHS = 3

# Meses de historia del catálogo Banxico `bxico_rfc_curp_cat`: ventana más
# amplia que la de CEP porque el catálogo se sube con poca frecuencia y el
# dedup por `cta` ya se queda con la `fecha_de_subida` más reciente.
BXICO_HISTORY_IN_MONTHS = 12

# ---------------------------------------------------------------------------
# Patrones regex para clasificar el identificador de ordenante/beneficiario
# (columnas rfc_curp_* ya normalizadas). Los usa add_id_validation_flags en
# pipelines/ceps/rfc_nom_ranking.py para generar los flags is_* y la
# etiqueta id_kind_*.
# ---------------------------------------------------------------------------
CURP_PATTERN    = r"^[A-ZÑ&]{4}[0-9]{6}[HM][A-Z]{5}[A-Z0-9]{2}$"    # CURP: 4 letras + fecha aammdd + sexo H/M + entidad/consonantes (5) + homoclave (2)
RFC_FISICA_PATTERN = r"^[A-ZÑ&]{4}[0-9]{6}[A-Z0-9]{3}$"             # RFC persona física: 4 letras + fecha aammdd + homoclave (3)
RFC_FISICA_SIN_HOMOCLAVE_PATTERN = r"^[A-ZÑ&]{4}[0-9]{6}$"          # RFC física truncado: 4 letras + fecha aammdd, sin homoclave
RFC_MORAL_PATTERN = r"^[A-ZÑ&]{3}[0-9]{6}[A-Z0-9]{3}$"              # RFC persona moral: 3 letras + fecha aammdd + homoclave (3)
TC_PATTERN    = r"^[0-9]{16}$"                                      # tarjeta (16 dígitos): aparece en rfc_curp cuando el banco reporta el plástico en vez del RFC
CLABE_PATTERN    = r"^[0-9]{18}$"                                   # CLABE interbancaria (18 dígitos): idem, reporta la cuenta en vez del RFC



# Valores literales que equivalen a "sin dato": el pipeline los pasa a null
# antes de clasificar/rankear (ver s264_ceps_cleaned en pipelines/ceps/rfc_nom_ranking.py).
RFC_NULL_SYNONYMS = ["", "ND", "RFC NO DISPONIBLE"]  # placeholders de RFC/CURP vacío
NOM_NULL_SYNONYMS = ["", "ND"]                       # placeholders de nombre vacío


# Prioridad del tipo de identificador para escoger el RFC/CURP canónico
# (menor = mejor). Se materializa como la columna RFC_CURP_KIND_PRIORITY
# mediante create_map(id_kind -> prioridad) en el pipeline.
RFC_CURP_KIND_PRIORITY = {
"rfc_moral": 0,                  # RFC de persona moral: identificador fiscal completo, el preferido
"rfc_fisica": 1,                 # RFC de persona física con homoclave
"curp": 2,                       # CURP: identifica a la persona pero no es RFC
"rfc_fisica_sin_homoclave": 3,   # RFC física incompleto (sin homoclave)
"clabe": 4,                      # CLABE: no es identificador fiscal, solo una cuenta
"tc":5,                          # tarjeta de 16 dígitos: aproximación más débil de identidad
"invalid": 6                     # no matchea ningún patrón: el peor candidato
}


# ---------------------------------------------------------------------------
# Origen de la fila dentro del histórico aplanado (columna `source`):
#   - "s264_ceps": transacciones CEP/SPEI observadas en la tabla s264.
#   - "bxico_rfc_curp_cat": catálogo oficial de clientes que provee Banxico.
# ---------------------------------------------------------------------------
S264_SOURCE = "s264_ceps"
BXICO_SOURCE = "bxico_rfc_curp_cat"

# Prioridad del origen al escoger el RFC/CURP canónico (menor = mejor). El
# catálogo Banxico es información oficial y gana a cualquier candidato de
# s264_ceps; el resto de criterios de la ventana siguen gobernando entre
# alternativas del mismo origen. Se materializa como columna SOURCE_PRIORITY.
SOURCE_PRIORITY = {
BXICO_SOURCE: 0,                 # información oficial de Banxico: máxima prioridad
S264_SOURCE: 1                   # lo observado en transacciones CEP
}


# ranking for RFC and CURP by cta, prioritizing the following criteria:
# Ventana que ordena los candidatos a RFC/CURP canónico dentro de cada cuenta
# `cta`; con row_number()=1 se elige el "mejor" identificador observado para la
# cuenta (ver s264_ceps_flattened_ranks en el pipeline). Los criterios primero
# empujan al final los candidatos "malos" (nulos, enmascarados, tipo de id
# débil) y luego premian frecuencia/monto.
RFC_BY_CTA_PRIORITY_WINDOW = (Window.partitionBy("cta")  # un ranking independiente por cada cuenta
.orderBy(
col("rfc_curp").isNull().asc(), # bad: RFC/CURP nulo al final (isNull()=False ordena primero)
col("nom").isNull().asc(), # bad: candidatos con nombre conocido primero
col("SOURCE_PRIORITY").asc_nulls_last(), # bad: catálogo Banxico (0) antes que s264_ceps (1); null (parquets antiguos) al final
col("rfc_curp_ends_with_xxx").isNull().asc(), # bad: flag "termina en XX" nulo al final
col("RFC_CURP_KIND_PRIORITY").isNull().asc(), # bad: sin tipo de id clasificado (prioridad nula) al final
col("rfc_curp_ends_with_xxx").asc(), # bad: prefiera RFC que NO termina en "XX" (0 antes que 1; sufijo XX = RFC genérico/enmascarado)
col("RFC_CURP_KIND_PRIORITY").asc(), # bad: mejor tipo de identificador primero (rfc_moral=0 ... invalid=6)
col("cnt_rfc_by_cta").desc(), # candidato observado en más operaciones dentro de la cuenta primero
col("tot_oper_mto").desc(),        # good: mayor monto total operado primero
col("lst_fec_informacion_hora_oper").asc(), # good: desempate por fecha-hora de la última operación (ascendente)
col("rfc_curp").asc()              # desempate final determinista (orden alfabético del RFC)
)
)

# Priority for RFC and CURP by nom, prioritizing the following criteria:
# Análoga a la anterior pero particionada por `nom`: elige el RFC/CURP canónico
# de cada nombre (vía alternativa cuando la cuenta falta o no discrimina).
RFC_BY_NOM_PRIORITY_WINDOW = (Window.partitionBy("nom")  # un ranking independiente por cada nombre
.orderBy(
col("rfc_curp").isNull().asc(), # bad: RFC/CURP nulo al final
col("nom").isNull().asc(), # bad: nombre nulo al final
col("SOURCE_PRIORITY").asc_nulls_last(), # bad: catálogo Banxico (0) antes que s264_ceps (1); null (parquets antiguos) al final
col("rfc_curp_ends_with_xxx").asc(),  # bad: prefiera RFC que NO termina en "XX"
col("RFC_CURP_KIND_PRIORITY").asc(),  # bad: mejor tipo de identificador primero
col("cnt_rfc_by_cta").desc(), # candidato visto en más operaciones de su cuenta primero
col("cnt_rfc_by_nom").desc(),         # good: candidato visto en más operaciones del nombre primero
col("tot_oper_mto").desc(),           # good: mayor monto total operado primero
col("lst_fec_informacion_hora_oper").asc(), # good: desempate por fecha-hora de la última operación
col("rfc_curp").asc()                 # desempate final determinista
)
)


# Raíz HDFS del job (definida en config/job.py, ya incluye el vintage) y
# subdirectorio propio de este pipeline donde se materializan sus salidas.
_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("ceps/rfc_nom_raking")


# ---------------------------------------------------------------------------
# INPUTS: fuentes que lee el step. Cada entrada describe UNA fuente
# particionada; standard_load_parquet_or_table / SparkLoadPartitionedTableOrParquet
# (libs/framework + partition_lags.py) resuelven las particiones a leer a
# partir de vintage_date + lag + history.
# ---------------------------------------------------------------------------
input = {
# Historial crudo de CEPs (Comprobantes Electrónicos de Pago, SPEI): tabla
# Hive productiva particionada por fecha de información.
"s264_ceps" : {
"table_or_hdfs": "gcpdlkmvpsd_prd_db.fz2s264_bxic0_t_d",   # tabla Hive origen (CEPs s264)
"information_date_column": "fec_informacion",            # columna-partición de fecha de información (corte del dato)
"lag": 0,                                                # meses de desfase sobre vintage_date; 0 = el intervalo termina en el mes vintage
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,               # meses de historia a leer (3: vintage y los 2 anteriores)
"information_date_mode":"all",                           # leer TODOS los meses del intervalo ("first"/"last" leerían solo el primero/último)
"minimum_required_history": round(CEPS_MAXIMUM_HISTORY_IN_MONTHS/2)  # mínimo de meses exigidos (2); con menos -> error "Not enough history"; con menos de `history` -> solo warning
},
# Catálogo oficial de clientes de Banxico (nombres, CURP, RFC, cta, banco).
# INPUT OPCIONAL: si la tabla/path no existe, el pipeline sigue sin él
# (ver `optional_input` en pipelines/ceps/rfc_nom_ranking.py).
# TODO: actualizar con el nombre real cuando Banxico publique la tabla;
# sobreescribible vía MINERVA_BXICO_RFC_CURP_CAT_TABLE.
"bxico_rfc_curp_cat" : {
"table_or_hdfs": os.environ.get("MINERVA_BXICO_RFC_CURP_CAT_TABLE",
    "gcpdlkmvpsd_prd_db.bxico_rfc_curp_cat"),  # tabla Hive del catálogo Banxico
"information_date_column": "fecha_de_subida",# columna-partición: fecha de carga del catálogo
"lag": 0,                                    # meses de desfase sobre vintage_date
"history": BXICO_HISTORY_IN_MONTHS,          # ventana amplia: el catálogo puede subir con poca frecuencia
"information_date_mode":"each",              # toda la ventana; el dedup por cta elige la más reciente
"minimum_required_history": False            # sin exigencia: cualquier partición disponible sirve
}
}

# ---------------------------------------------------------------------------
# OUTPUTS: datasets que el step persiste. Dos formatos de entrada:
#  * dict "largo" -> tabla/parquet particionado (mis_date + process_date); el
#    decorador dynamic_partitioned_table_or_parquet intenta recargar las
#    particiones del intervalo y, si no existen, recomputa y sobreescribe solo
#    la partición (mis_date=vintage, process_date=hoy).
#  * dict "simple" de 2 claves (table_or_hdfs + keep_or_delete) -> parquet NO
#    particionado gestionado por dynamic_unpartitioned_parquet.
# Claves del dict largo:
#  * information_date_column: columna-partición de fecha de información (mis_date = yyyyMM).
#  * process_date_column: columna-partición de fecha de proceso/carga.
#  * lag / history: desfase (meses atrás sobre vintage) y meses del intervalo
#    usados al RELEER el output.
#  * information_date_mode "each": todos los meses del intervalo.
#  * process_date_mode "last": por cada mes, solo la process_date más reciente.
# ---------------------------------------------------------------------------
output = {
# CEP depurado con el tipo de identificador clasificado (is_*/id_kind_* por
# ordenante y beneficiario); entrada del aplanado y del reemplazo posterior.
"rfc_curp_analysis_s264_ceps": {"table_or_hdfs": current_hdfs.joinpath("rfc_curp_analysis_s264_ceps"),
"information_date_column": "mis_date",      # columna-partición de fecha de información (yyyyMM)
"process_date_column": "process_date",      # columna-partición de fecha de carga
"lag": 0,                                   # sin desfase: el intervalo termina en el mes vintage
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,  # al releer, cubre los 3 meses de historia
"information_date_mode":"each",             # todos los meses del intervalo
"process_date_mode":"last"                  # por cada mes, solo la carga (process_date) más reciente
},
# Historial aplanado a formato largo: una fila por entidad (ordenante o
# beneficiario) con esquema común nom/cta/id_ban/tipo_cta/rfc_curp/...
"s264_ceps_flattened": {"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened"),
"information_date_column": "mis_date",      # columna-partición de fecha de información (yyyyMM)
"process_date_column": "process_date",      # columna-partición de fecha de carga
"lag": 0,                                   # sin desfase sobre vintage
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,  # al releer, cubre los 3 meses de historia
"information_date_mode":"each",             # todos los meses del intervalo
"process_date_mode":"last"                  # por cada mes, solo la carga más reciente
},
# Agregado por (cta, nom, id_ban, tipo_cta, rfc_curp, rfc_curp_kind) con
# cnt / tot_oper_mto / lst_fec_informacion_hora_oper / flags de validez.
"s264_ceps_flattened_groupby": {"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_groupby"),
"information_date_column": "mis_date",      # columna-partición de fecha de información (yyyyMM)
"process_date_column": "process_date",      # columna-partición de fecha de carga
"lag": 0,                                   # sin desfase sobre vintage
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,  # al releer, cubre los 3 meses de historia
"information_date_mode":"each",             # todos los meses del intervalo
"process_date_mode":"last"                  # por cada mes, solo la carga más reciente
},
# El agregado anterior más los rankings rank_rfc_by_cta / rank_rfc_by_nom
# (row_number sobre las ventanas de prioridad) y los conteos cnt_rfc_by_*.
"s264_ceps_flattened_ranks": {"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_ranks"),
"information_date_column": "mis_date",      # columna-partición de fecha de información (yyyyMM)
"process_date_column": "process_date",      # columna-partición de fecha de carga
"lag": 0,                                   # sin desfase sobre vintage
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,  # al releer, cubre los 3 meses de historia
"information_date_mode":"each",             # todos los meses del intervalo
"process_date_mode":"last"                  # por cada mes, solo la carga más reciente
},
"s264_ceps_flattened_rank_rfc_by_cta_cases_replace": { #simple
"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_rank_rfc_by_cta_cases_replace"),  # casos de reemplazo por cuenta (mejor RFC por cta/nom/id_ban); parquet sin particionar
"keep_or_delete": "keep"                    # conservar tras el run ("delete" lo registraría en tmp_paths para borrado al final)
},
"s264_ceps_flattened_rank_rfc_by_nom_cases_replace": { #simple
"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_rank_rfc_by_nom_cases_replace"),  # casos de reemplazo por nombre (mejor RFC por nom/id_ban); parquet sin particionar
"keep_or_delete": "keep"                    # conservar tras el run ("delete" lo registraría en tmp_paths para borrado al final)
},
# CEP final con RFC/CURP reemplazados usando los casos anteriores; la consume
# el step de txn_replacement y los pipelines de grafo.
"rfc_curp_analysis_s264_ceps_replaced": {"table_or_hdfs": current_hdfs.joinpath("rfc_curp_analysis_s264_ceps_replaced"),
"information_date_column": "mis_date",      # columna-partición de fecha de información (yyyyMM)
"process_date_column": "process_date",      # columna-partición de fecha de carga
"lag": 0,                                   # sin desfase sobre vintage
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,  # al releer, cubre los 3 meses de historia
"information_date_mode":"each",             # todos los meses del intervalo
"process_date_mode":"last"                  # por cada mes, solo la carga más reciente
}
}
