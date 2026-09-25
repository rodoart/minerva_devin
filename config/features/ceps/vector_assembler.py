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
import config.features.ceps.cluster_features as cclf
import config.target_propagation.lovelace.special_treatment as ctp_l_st

##########################################################################################
# CONFIGURATION
##########################################################################################

job_config = c_j.sbx  # config del job (raíz HDFS, fechas, cohortes)

# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# Columna del pivote externo con la lista de numclientes (se renombra a "numcliente").
PIVOT_COLUMN = "numcliente"

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
    "mis_date", "process_date", "vintage", "cohort",  # metadatos de partición/cohorte
    "seed_score",                                     # auxiliar de la propagación (semilla)
    # Columnas de grupo del step de clustering: identifican el grupo, no son features
    "component_id", "scc",
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


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))      # raíz HDFS de las salidas del job
current_hdfs = _general_root_hdfs.joinpath("features/ceps/vector_assembler")  # directorio de salida del step


input = {
    # Pivote externo: misma fuente que lovelace_target; de ella solo se toma la
    # lista de numclientes (cast a string + distinct) para los que se genera vector.
    "pivot": {"table_or_hdfs": ctp_l_st.output["target_numcliente"]["table_or_hdfs"],
        "pivot_column": PIVOT_COLUMN},     # columna que se renombra a "numcliente"
    "nodes": ccen.output["nodes"],          # nodos del grafo: aportan el array `numcliente` y oper_mto (peso)
    "nodes_join_target_lovelace": cfcf.output["nodes_join_target_lovelace"],  # nodos + etiqueta target_lovelace
}


# Fuentes de features a nivel nodo (`id`). Modos:
#   "simple"        -> parquet plano en "path"
#   "partitioned"   -> clave de `input` con mis_date/process_date (lag/history)
#   "merge_schema"  -> parquet padre con subdirs por parámetros (p.ej. contagion_*)
#                      leído con mergeSchema; las variantes se colapsan a una fila
#                      por `id` (max de cada columna contagion_*).
_GRAPH_FEATURE_KEYS = [  # claves de `output` de graph_features leídas como fuentes "simple"
    "pagerank", "degrees", "components",
    # "triangle_count",
    "weighted_pagerank", "weighted_degrees", "weighted_edge_stats",
    # "weighted_triangle_count",
]
FEATURE_SOURCES = {
    key: {"mode": "merge_schema", "path": cfgf.output[key]["table_or_hdfs"]}
    for key in _GRAPH_FEATURE_KEYS
}
# Scores de contagio: el padre target_propagation contiene un subdir por
# combinación de parámetros y cada uno aporta su columna contagion_<weight_type>.
FEATURE_SOURCES["contagion"] = {
    "mode": "merge_schema",
    "path": cfcf.output["target_propagation"]["table_or_hdfs"],
}
# Nodos con el target Lovelace unido: lectura particionada (mis_date/process_date)
# vía la clave homónima de `input`; aporta las columnas target_lovelace_*.
FEATURE_SOURCES["nodes_join_target_lovelace"] = {
    "mode": "partitioned",
    "key": "nodes_join_target_lovelace",
}
# Stats intra-grupo del step de clustering (una fila por `id`).
FEATURE_SOURCES["cluster_stats"] = {
    "mode": "simple",
    "path": cclf.output["cluster_stats"]["table_or_hdfs"],
}


# Ambas salidas son tablas/parquets particionados por mis_date/process_date.
output = {
    # Features agregadas a nivel numcliente (una fila por numcliente y mes).
    "numcliente_features": {"table_or_hdfs": current_hdfs.joinpath("numcliente_features"),
        "information_date_column": "mis_date",     # columna de fecha de información (partición mensual)
        "process_date_column": "process_date",     # columna de fecha de proceso/ejecución (segunda partición)
        "lag": 0,                                  # ventana sin desplazamiento respecto a vintage_date
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,  # meses de historia (12)
        "information_date_mode":"each",            # se usan todos los meses del intervalo
        "process_date_mode":"last"                 # por cada mes se toma la process_date más reciente
    },
    # Lo anterior sobre el pivote + columna `features` (vector ensamblado).
    "numcliente_features_vector": {"table_or_hdfs": current_hdfs.joinpath("numcliente_features_vector"),
        "information_date_column": "mis_date",     # columna de fecha de información (partición mensual)
        "process_date_column": "process_date",     # columna de fecha de proceso/ejecución (segunda partición)
        "lag": 0,                                  # ventana sin desplazamiento respecto a vintage_date
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,  # meses de historia (12)
        "information_date_mode":"each",            # se usan todos los meses del intervalo
        "process_date_mode":"last"                 # por cada mes se toma la process_date más reciente
    }
}
