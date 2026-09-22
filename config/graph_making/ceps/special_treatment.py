from libs.data_engineering_toolbox.path import HivePath
from ...job import sbx as job_config                                        # config del job sandbox (rutas raíz HDFS)
from pyspark.sql.functions import col,lpad
from pyspark.sql.types import StringType
from ...ceps.txn_replacement import output as ceps_txn_replacement_output    # salidas del step previo (txn_replacement)
from .. import GRAPH_TOTAL_HISTORY_IN_MONTHS, GRAPH_GLOBAL_LAG               # ventana del grafo: meses de historia y lag global

# Raíz HDFS general del job (sandbox); base para las rutas de salida del grafo.
_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
# Directorio HDFS donde este step persistirá sus salidas.
current_hdfs = _general_root_hdfs.joinpath("graph/ceps/special_treatment")

# Tratamiento estándar de nulos aplicado tras la carga (libs.functions.missing_treatment).
# Cada clave es una columna y el valor indica el tipo de imputación y sus argumentos:
#   ["value", X] -> rellena nulos con el literal X; "mean"/"mean_with_nulls" -> media.
STANDARD_MISSING_TREATMENT_VARS = {
    "oper_mto": ["value", 0]   # importe de la operación: nulos -> 0
}



# Insumos del step: cada entrada describe una tabla/parquet particionado a leer.
#   "table_or_hdfs"            -> tabla Hive o ruta HDFS del parquet.
#   "information_date_column"  -> columna de partición con la fecha de información (mes).
#   "process_date_column"      -> columna de partición con la fecha de proceso/carga (opcional).
#   "lag"                      -> meses de retraso de la ventana respecto a vintage_date (>0 = hacia atrás).
#   "history"                  -> nº de meses de historia a leer.
#   "information_date_mode"    -> meses del intervalo a leer: "all"/"each" = todos, "first"/"last" = solo el primero/último.
#   "process_date_mode"        -> fecha(s) de proceso por mes: "last" = la más reciente, "first" = la primera,
#                                 "all" = todas, "each" = exige exactamente una por mes.
#   "minimum_required_history" -> nº mínimo de meses con datos exigido; si se cargan menos, error (False/0 = sin exigencia).
#   "select"                   -> proyección (columnas/expresiones) aplicada tras la carga.
input = {
    # Transacciones CEPS aplanadas/rankeadas con RFC-CURP sustituidos (salida persistida del step txn_replacement).
    "s264_ceps_flattened_rank_rfc_by_cta_cases_replace" : {
        "table_or_hdfs": ceps_txn_replacement_output["rfc_curp_analysis_s264_ceps_replaced"]["table_or_hdfs"],             # se lee la salida del step previo
        "information_date_column": ceps_txn_replacement_output["rfc_curp_analysis_s264_ceps_replaced"]["information_date_column"],  # hereda la columna de fecha ("mis_date")
        "lag": GRAPH_GLOBAL_LAG,                                # sin desfase adicional: usa el lag global del grafo
        "history": GRAPH_TOTAL_HISTORY_IN_MONTHS,               # se leen los 12 meses de historia del grafo
        "information_date_mode":ceps_txn_replacement_output["rfc_curp_analysis_s264_ceps_replaced"]["information_date_mode"],  # "each": todos los meses del intervalo
        "minimum_required_history": False,                      # no se exige historia mínima; solo avisa si faltan meses
        "select":                                               # proyección tras la carga: renombra/normaliza a la convención _src/_dst

        [
            col("customer_id").alias("numcliente"),                # id interno de cliente -> numcliente
            col("rfc_curp_ord").alias("id_src"),                   # RFC/CURP ordenante -> id del nodo origen
            col("rfc_curp_ben").alias("id_dst"),                   # RFC/CURP beneficiario -> id del nodo destino
            col("cta_ord").alias("cta_src"),                       # cuenta ordenante
            col("cta_ben").alias("cta_dst"),                       # cuenta beneficiaria
            col("nom_ord").alias("nom_src"),                       # nombre ordenante
            col("nom_ben").alias("nom_dst"),                       # nombre beneficiario
            col("id_ban_ord").alias("id_ban_src"),                 # banco ordenante
            col("id_ban_ben").alias("id_ban_dst"),                 # banco beneficiario
            col("fec_informacion").cast(StringType()).alias("information_date"),          # fecha de información normalizada a string
            lpad(col("hora_oper").cast(StringType()), 6, "0").alias("hora_oper"),         # hora de operación con padding a 6 dígitos (HHmmss)
            "cve_tipo_orden",                                      # clave de tipo de ordenante ("E" emisor / "R" receptor)
            "oper_mto"                                             # importe de la operación
        ]
    }
}


# Salidas del step: mismas claves que los insumos, pero describen dónde se persiste
# cada DataFrame y cómo se relee después (recarga de caché vía `dynamic_partitioned_table_or_parquet`).
output = {
    # Transacciones con tratamiento de faltantes e ids de nodos resueltos.
    "missing_treatment": {"table_or_hdfs": current_hdfs.joinpath("missing_treatment"),  # parquet de salida del step
        "information_date_column": "mis_date",    # partición de fecha de información (YYYY-MM) escrita/releída
        "process_date_column": "process_date",    # partición de fecha de proceso escrita/releída
        "lag": 0,                                 # al releer la salida no se aplica desfase
        "history": GRAPH_TOTAL_HISTORY_IN_MONTHS, # al releer se cubre la ventana completa del grafo
        "information_date_mode":"each",           # todos los meses del intervalo
        "process_date_mode":"last"                # por cada mes se usa la fecha de proceso más reciente
    }
}
