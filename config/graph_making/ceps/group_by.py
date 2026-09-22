##########################################################################################
# Libraries
##########################################################################################

# --------------------------------------------------------------------------------------
# Pyspark
# --------------------------------------------------------------------------------------
from pyspark.sql.functions import (col,
    sum as spark_sum, last, collect_set
)

# --------------------------------------------------------------------------------------
# CUSTOM
# --------------------------------------------------------------------------------------

from libs.data_engineering_toolbox.path import HivePath
import libs.data_engineering_toolbox.pyspark.tools as pdt
from libs.functions.aggregations import (recency_auxiliary_columns,
    select_group_by_features)

import config.job as c_j
import config.ceps.txn_replacement as cc_tr
import config.graph_making.ceps.special_treatment as c_gmc_st
import config.graph_making as c_gmc

##########################################################################################
# CONFIGURATION
##########################################################################################
job_config = c_j.sbx                            # config del job sandbox (rutas raíz HDFS)
ceps_txn_replacement_output = cc_tr.output      # salidas del step txn_replacement (config)
special_treatment_output = c_gmc_st.output      # salidas del step special_treatment (insumos de este step)
special_treatment_input = c_gmc_st.input        # insumos del step special_treatment (se reutiliza su "select")


# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# GROUP BY TXN STEP

# Columnas que definen una arista del grafo: las transacciones con el mismo
# (id_src, id_dst) se agregan en una única arista.
GROUP_BY_TXN_GROUPING_VARS = ["id_src", "id_dst"]

# Columnas auxiliares creadas antes del groupBy y eliminadas del resultado
# (standard_group_by_txn las añade con withColumn y luego las descarta):
#   "_max_tfrom_days" -> antigüedad máxima de la ventana (txn más antigua).
#   "_recency_weight" -> peso por recencia (1 en la más antigua, max+1 en la más
#   reciente); las agregaciones weighted_* lo usan para ponderar.
GROUP_BY_AUXILIARY_COLS = recency_auxiliary_columns(tfrom_column="tfrom_days")

# Funciones de agregación habilitadas para este job. El catálogo estándar vive en
# libs.functions.aggregations.standard_group_by_features; para añadir/quitar
# agregaciones disponibles basta editar esta lista (o pasar un dict propio).
GROUP_BY_ENABLED_FEATURES = [
    "min", "max", "mean", "weighted_mean", "weighted_sum", "std",
    "count", "weighted_count", "countDistinct", "sum", "last", "first",
    "curt", "skew", "so",
]
# Cada entrada se invoca como func(nombre_variable) -> Column y el resultado se
# renombra "{func_name}_{variable}" (ver GROUP_TXN_AGGREGATIONS y
# CepsGroupBySubStep).
GROUP_BY_TXN_FEATURES = select_group_by_features(GROUP_BY_ENABLED_FEATURES)


# Variables agregables de las transacciones: de la combinación
# GROUP_BY_TXN_FEATURES x GROUP_BY_TXN_VARIABLES salen los nombres
# "{func}_{variable}" que se listan en GROUP_TXN_AGGREGATIONS.
GROUP_BY_TXN_VARIABLES = [
    "oper_mto",        # importe de la operación
    "id_ban_src",      # banco ordenante
    "tfrom_days"       # días desde la transacción hasta la fecha de corte
]


# set of aggregations to be applied to the group by step. Each aggregation is a tuple of (function, variable).
# Cada string "{func}_{variable}" se resuelve buscando el prefijo de función más
# largo que case en GROUP_BY_TXN_FEATURES y aplicándolo a la variable restante
# (ver CepsGroupBySubStep.group_by_txn).
GROUP_TXN_AGGREGATIONS = [
    # func(variable).alias(f"{func_name}_{variable}") for variable in GROUP_BY_TXN_VARIABLES for func_name, func in GROUP_BY_TXN_FEATURES.items()
    "min_oper_mto",              # importe mínimo de las txns de la arista
    "max_oper_mto",              # importe máximo de las txns de la arista
    "mean_oper_mto",             # importe medio (requerido por EDGES_VARS y WEIGHT_COLUMNS)
    "sum_oper_mto",              # requerido por EDGES_VARS
    "count_oper_mto",            # -> count_txn en edges
    "max_tfrom_days",            # requerido por EDGES_VARS
    "weighted_mean_oper_mto",    # requerido por EDGES_VARS y WEIGHT_COLUMNS
    "weighted_sum_oper_mto",     # requerido por EDGES_VARS y WEIGHT_COLUMNS
    "weighted_count_oper_mto",   # -> weighted_count_txn en edges
    "so_oper_mto",               # outlier-ness de montos (la columna txn_number no existe)
]


# set of variables to group by in the group by step. These variables are used to define a transaction between two entities. In this case, the source

# GROUP BY IDs
# Currently, the input columns are extracted from the special treatment input configuration. The source and destination columns are identified by th
# Columnas disponibles tras el "select" del insumo de special_treatment (strings o
# alias de expresiones Column); de ellas se derivan las columnas _src/_dst/comunes.
INPUT_COLUMNS = [column if isinstance(column, str) else pdt.get_column_alias(column) for column in special_treatment_input["s264_ceps_flattened_rank_rfc_by_cta_cases_replace"]["select"]]
ID_SRC_COLUMNS = "id_src"   # columna(s) identificadora del nodo origen (al quitar el sufijo queda como "id")
ID_DST_COLUMNS = "id_dst"   # columna(s) identificadora del nodo destino

# Columnas propias de cada lado de la transacción (llevan el sufijo y no son ids de
# agrupación): al proyectar cada lado se elimina el sufijo (p.ej. "cta_src" -> "cta").
SRC_COLUMNS = [column for column in INPUT_COLUMNS if column.endswith("_src") and column not in GROUP_BY_TXN_GROUPING_VARS]
DST_COLUMNS = [column for column in INPUT_COLUMNS if column.endswith("_dst") and column not in GROUP_BY_TXN_GROUPING_VARS]
# Columnas comunes a ambos lados (sin sufijo _src/_dst): se conservan tal cual en cada proyección.
COMMON_COLUMNS = [column for column in INPUT_COLUMNS if not column.endswith("_src") and not column.endswith("_dst") and column not in GROUP_BY_TXN_GROUPING_VARS]


# Agregaciones del group-by por nodo ("id"): consolidan los atributos del nodo a
# partir de la unión de los lados origen y destino de las transacciones.
GROUP_ID_AGGREGATIONS = [
    collect_set("numcliente").alias("numcliente"),      # conjunto de ids de cliente asociados al nodo
    collect_set("nom").alias("nom"),                    # conjunto de nombres asociados al nodo
    collect_set("cta").alias("cta"),                    # conjunto de cuentas asociadas al nodo
    collect_set("id_ban").alias("id_ban"),              # conjunto de bancos asociados al nodo
    last("information_date").alias("information_date"), # fecha de información más reciente registrada en el nodo
    spark_sum("oper_mto").alias("oper_mto")             # importe total operado por el nodo
]


# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------


# Raíz HDFS general del job y directorio donde este step persiste sus salidas.
_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("graph/ceps/group_by")


# Insumos del step: reutiliza la definición de salida del step special_treatment
# (misma ruta y mismas claves de particionado/lectura; ver comentarios en special_treatment.input).
input = {
    "missing_treatment" : special_treatment_output["missing_treatment"]
}


# Salidas del step: mismas claves que los insumos; describen dónde se persiste cada
# DataFrame y cómo se relee después (recarga de caché vía `dynamic_partitioned_table_or_parquet`).
output = {
    # Transacciones agregadas por arista (id_src, id_dst).
    "group_by_txn": {"table_or_hdfs": current_hdfs.joinpath("group_by_txn"),  # parquet de salida
        "information_date_column": "mis_date",            # partición de fecha de información (YYYY-MM) escrita/releída
        "process_date_column": "process_date",            # partición de fecha de proceso escrita/releída
        "lag": 0,                                         # sin desfase al releer
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,   # al releer se cubre la ventana completa del grafo
        "information_date_mode":"each",                   # todos los meses del intervalo
        "process_date_mode":"last"                        # por cada mes se usa la fecha de proceso más reciente
    },
    # Nodos agregados por "id" (unión de los lados origen y destino de las txns).
    "group_by_id": {"table_or_hdfs": current_hdfs.joinpath("group_by_id"),    # parquet de salida
        "information_date_column": "mis_date",            # partición de fecha de información (YYYY-MM) escrita/releída
        "process_date_column": "process_date",            # partición de fecha de proceso escrita/releída
        "lag": 0,                                         # sin desfase al releer
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,   # al releer se cubre la ventana completa del grafo
        "information_date_mode":"each",                   # todos los meses del intervalo
        "process_date_mode":"last"                        # por cada mes se usa la fecha de proceso más reciente
    }
}
