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
#                   SOLO disponible si SUBCLUSTER_ENABLED=True; si está
#                   desactivado, cluster_stats la ignora con un warning.
GROUP_COLUMNS = ["component_id", "scc"]

# Si es False no se ejecuta ningún algoritmo de sub-partición: el step solo
# agrupa por "component_id" (parquet `components` ya existente) y sigue.
# Desactivarlo salta la parte más pesada del step (SCC de GraphFrames).
SUBCLUSTER_ENABLED = True

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
    "weight_type", "target_column",
    "max_iter", "alpha", "keep_seed_floor", "reset_prob",
    "seed_score",
]

# --- Robustez de los groupBy de stats (skew fuerte en component_id/scc) ---
# La distribución de grupos es muy asimétrica (una componente gigante): el
# groupBy se ejecuta en dos etapas con sal — parciales por (grupo, sal) con
# SALT_BUCKETS buckets, merge por grupo — y en chunks de columnas, cada uno
# persistido como parquet intermedio en `cluster_stats_parts` (reanudable).
SALT_BUCKETS = 64           # buckets de sal por grupo (des-skew en 2 etapas)
STATS_COLUMN_CHUNK = 8      # columnas agregadas por etapa/parquet intermedio
STATS_BROADCAST_MAX_GROUPS = 200000   # broadcast del join final hasta N grupos

# --- Aligeramiento del step (si sigue pesado) ---
# CLUSTER_LIGHT_MODE=True limita stats y columnas a las listas LIGHT_* Y solo
# une LIGHT_FEATURE_SOURCES a los nodos enriquecidos (salta los joins de
# fuentes pesadas de features de grafo).
CLUSTER_LIGHT_MODE = False
LIGHT_STATS = ["count", "mean", "min", "max", "sum"]
LIGHT_AGGREGATE_PREFIXES = [
    "target_", "contagion_", "oper_mto", "_strength",
]
LIGHT_FEATURE_SOURCES = ["contagion"]   # fuentes unidas en modo ligero
AGGREGATE_INCLUDE_PREFIXES = None       # lista -> solo columnas con esos prefijos/sufijos

# Prefijos de columnas que DEBEN entrar en los estadísticos de grupo: la
# etiqueta original (target_lovelace*), la propagada (contagion_*) y las
# columnas de peso/monto (oper_mto, weight*, *_strength). Si ninguna columna
# agregable coincide con un prefijo se registra un warning al construir
# `cluster_stats` — sirve como guarda de que los targets propagados y los
# pesos participan en las features de agrupación.
REQUIRED_STATS_PREFIXES = [
    "target_",      # etiqueta(s) semilla unidas a los nodos
    "contagion_",   # target propagado por el grafo
    "oper_mto",     # monto/peso base de los nodos
    "weight",       # pesos de arista ya agregados por nodo (p.ej. *_weight)
    "_strength",    # fuerzas ponderadas in/out/total de weighted_degrees
]

# Features de grafo cuyos parquets se unen a los nodos para las stats intra-grupo.
# Se lee el padre de cada `output` con mergeSchema (cubre los subdirs por
# parámetros, p.ej. weight=count_txn) y se colapsa a una fila por `id`.
GRAPH_FEATURE_KEYS = [
    "pagerank", "degrees",
    "degree_balance", "reciprocity", "self_loops",
    "weighted_pagerank", "weighted_degrees", "weighted_edge_stats",
    "weighted_degree_balance",
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
    # Intermedios materializados (reload-or-recompute). El SCC de GraphFrames
    # NO es reanudable (sus checkpoints orgánicos quedan huérfanos al morir el
    # proceso): si el job cae, sin parquet propio habría que recomputar el
    # subgrafo entero. Lo mismo aplica al join pesado `nodes_enriched`.
    "subcluster_df": {"table_or_hdfs": current_hdfs.joinpath("subcluster_df"),
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    },
    "nodes_enriched": {"table_or_hdfs": current_hdfs.joinpath("nodes_enriched"),
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    },
    # Partes de los stats por grupo (un parquet por chunk de columnas y
    # group_column): puntos de reanudación de los groupBy etapados.
    "cluster_stats_parts": {"table_or_hdfs": current_hdfs.joinpath("cluster_stats_parts"),
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    },
    # Directorio de checkpoint de GraphFrames para stronglyConnectedComponents.
    "checkpoint": {"table_or_hdfs": current_tmp_hdfs.joinpath("checkpoint"),
        "keep_or_delete": "delete"
    },
}
