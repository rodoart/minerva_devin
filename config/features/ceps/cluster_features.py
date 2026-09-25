##########################################################################################
# Libraries
##########################################################################################

# --------------------------------------------------------------------------------------
# Pyspark
# --------------------------------------------------------------------------------------

from pyspark.sql.functions import (
    col, count as spark_count, sum as spark_sum, mean as spark_mean,
    stddev as spark_std, min as spark_min, max as spark_max, percentile_approx)

# --------------------------------------------------------------------------------------
# CUSTOM
# --------------------------------------------------------------------------------------

from libs.data_engineering_toolbox.path import HivePath

import config.job as c_j
import config.graph_making.ceps.edges_and_nodes as ccen
import config.features.ceps.graph_features as cfgf
import config.features.ceps.target_propagation_features as cfcf

##########################################################################################
# CONFIGURATION
##########################################################################################

job_config = c_j.sbx  # config del job (raíz HDFS, fechas, cohortes)

# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# Columnas de agrupación de cada nodo:
#   - "component_id": componente conexa (determinista; parquet `components` ya existente)
#   - "scc":          componente fuertemente conexa (determinista; sub-partición dirigida)
GROUP_COLUMNS = ["component_id", "scc"]

# Algoritmo de sub-partición dentro de cada componente conexa.
#   "scc"               -> stronglyConnectedComponents (determinista)
#   "label_propagation" -> labelPropagation (NO determinista: los grupos pueden
#                          cambiar entre ejecuciones con los mismos datos)
SUBCLUSTER_METHOD = "scc"
SUBCLUSTER_MAX_ITER = 10

# Estadísticos calculados por grupo y columna agregada. Las funciones reciben el
# nombre de la columna y devuelven la expresión de agregación.
CLUSTER_STATS = {
    "count":  spark_count,
    "sum":    spark_sum,
    "mean":   lambda c: spark_mean(col(c)),
    "median": lambda c: percentile_approx(col(c), 0.5),
    "std":    lambda c: spark_std(col(c)),
    "min":    spark_min,
    "max":    spark_max,
}

# Columnas excluidas de la agregación (ids, columnas de grupo y metadatos de
# partición-param de los parquets de features leídos con mergeSchema).
AGGREGATE_EXCLUDE_COLUMNS = [
    "id", "numcliente", "component_id", "scc",
    "mis_date", "process_date", "information_date",
    "weight", "weight_type", "target_column",
    "max_iter", "alpha", "keep_seed_floor", "reset_prob",
    "seed_score",
]

# Features de grafo cuyos parquets se unen a los nodos para las stats intra-grupo.
# Se lee el padre de cada `output` con mergeSchema (cubre los subdirs por
# parámetros, p.ej. weight=count_txn) y se colapsa a una fila por `id`.
GRAPH_FEATURE_KEYS = [
    "pagerank", "degrees",
    "weighted_pagerank", "weighted_degrees", "weighted_edge_stats",
]
GRAPH_FEATURE_SOURCES = {
    key: cfgf.output[key]["table_or_hdfs"] for key in GRAPH_FEATURE_KEYS
}
GRAPH_FEATURE_SOURCES["contagion"] = (
    cfcf.output["target_propagation"]["table_or_hdfs"])

# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------

_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))      # raíz HDFS de las salidas del job
current_hdfs = _general_root_hdfs.joinpath("features/ceps/cluster_features")  # directorio de salida del step
current_tmp_hdfs = _general_root_hdfs.joinpath("features/ceps/tmp_cluster_features")  # tmp del step (checkpoint)


input = {
    # Componente conexa por nodo (parquet de la feature `components` ya calculada).
    "components": cfgf.output["components"],
    # Aristas del grafo (src/dst) para stronglyConnectedComponents.
    "edges": ccen.output["edges"],
    # Nodos + etiquetas target_lovelace_* (base del enriquecimiento por nodo).
    "nodes_join_target_lovelace": cfcf.output["nodes_join_target_lovelace"],
}


output = {
    # Una fila por id: stats intra-grupo (component_id y scc) de todas las
    # columnas numéricas del nodo enriquecido (targets + monto + antigüedad +
    # features de grafo + contagio).
    "cluster_stats": {"table_or_hdfs": current_hdfs.joinpath("cluster_stats"),
        "keep_or_delete": "keep"
    },
    # Directorio de checkpoint de GraphFrames para stronglyConnectedComponents.
    "checkpoint": {"table_or_hdfs": current_tmp_hdfs.joinpath("checkpoint"),
        "keep_or_delete": "delete"
    },
}
