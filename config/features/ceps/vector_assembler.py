##########################################################################################
# Libraries
##########################################################################################

# --------------------------------------------------------------------------------------
# Pyspark
# --------------------------------------------------------------------------------------

from pyspark.sql.functions import col, lit

# --------------------------------------------------------------------------------------
# CUSTOM
# --------------------------------------------------------------------------------------

from libs.data_engineering_toolbox.path import HivePath

import config.job as c_j
import config.graph_making as c_gmc
import config.graph_making.ceps.edges_and_nodes as ccen
import config.features.ceps.graph_features as cfgf
import config.features.ceps.target_propagation_features as cfcf
import config.target_propagation.lovelace.special_treatment as ctp_l_st

##########################################################################################
# CONFIGURATION
##########################################################################################

job_config = c_j.sbx

# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# Columna del pivote externo con la lista de numclientes (se renombra a "numcliente").
PIVOT_COLUMN = "num_cliente"

# Columna array de los nodos que contiene los numclientes asociados a cada id.
NODE_ID_ARRAY_COLUMN = "numcliente"

# Columnas de los nodos que pueden intervenir en el peso de agregación.
NODE_WEIGHT_COLUMNS = ["oper_mto"]

# Peso de la agregación a nivel numcliente: monto x decaimiento por antigüedad en días.
# Parametrizable: cualquier expresión Column evaluable sobre los nodos explotados.
AGGREGATION_WEIGHT = col("oper_mto").cast("double") / (col("tfrom_days") + lit(1.0))

# Estadística de agregación por feature: string global o {"default","<col>"} overrides.
FEATURE_AGGREGATION = {"default": "weighted_mean"}

# Columnas de metadatos que no se agregan ni se ensamblan.
EXCLUDE_COLUMNS = [
    "mis_date", "process_date", "vintage", "cohort", "seed_score",
    # Nombres de partición-param del merge_schema de contagion: no son features
    "weight_type", "target_column", "max_iter", "alpha", "keep_seed_floor",
]

# Prefijos excluidos SOLO del vector (se agregan a numcliente pero no son
# feature de entrada): las etiquetas `target_*` quedan fuera del vector y
# disponibles como columna para entrenamiento.
EXCLUDE_PREFIXES = ["target_"]

# Relleno de nulos antes del VectorAssembler (None para no rellenar).
FILL_NULLS_VALUE = 0.0

# VectorAssembler: política ante valores inválidos y nombre de la columna vector.
HANDLE_INVALID = "keep"
VECTOR_OUTPUT_COLUMN = "features"


# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("features/ceps/vector_assembler")


input = {
    "pivot": {"table_or_hdfs": ctp_l_st.input["lovelace_target"]["table_or_hdfs"],
        "pivot_column": PIVOT_COLUMN},
    "nodes": ccen.output["nodes"],
    "nodes_join_target_lovelace": cfcf.output["nodes_join_target_lovelace"],
}


# Fuentes de features a nivel nodo (`id`). Modos:
#   "simple"        -> parquet plano en "path"
#   "partitioned"   -> clave de `input` con mis_date/process_date (lag/history)
#   "merge_schema"  -> parquet padre con subdirs por parámetros (p.ej. contagion_*)
_GRAPH_FEATURE_KEYS = [
    "pagerank", "degrees", "components", "triangle_count",
    "weighted_pagerank", "weighted_degrees", "weighted_edge_stats",
    "weighted_triangle_count",
]
FEATURE_SOURCES = {
    key: {"mode": "simple", "path": cfgf.output[key]["table_or_hdfs"]}
    for key in _GRAPH_FEATURE_KEYS
}
FEATURE_SOURCES["contagion"] = {
    "mode": "merge_schema",
    "path": cfcf.output["target_propagation"]["table_or_hdfs"],
}
FEATURE_SOURCES["nodes_join_target_lovelace"] = {
    "mode": "partitioned",
    "key": "nodes_join_target_lovelace",
}


output = {
    "numcliente_features": {"table_or_hdfs": current_hdfs.joinpath("numcliente_features"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    },
    "numcliente_features_vector": {"table_or_hdfs": current_hdfs.joinpath("numcliente_features_vector"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    }
}
