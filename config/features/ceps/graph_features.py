##########################################################################################
# Libraries
##########################################################################################

# --------------------------------------------------------------------------------------
# Pyspark
# --------------------------------------------------------------------------------------
from libs.functions.features import STANDARD_WEIGHT_STATS

# --------------------------------------------------------------------------------------
# CUSTOM
# --------------------------------------------------------------------------------------

from libs.data_engineering_toolbox.path import HivePath

import config.job as c_j
import config.ceps.txn_replacement as cc_tr
import config.graph_making.ceps.special_treatment as c_gmc_st
import config.graph_making.ceps.group_by as c_gmc_gb
import config.graph_making.ceps.edges_and_nodes as c_gmc_ean


##########################################################################################
# CONFIGURATION
##########################################################################################
job_config = c_j.sbx                                   # config del job (raíz HDFS, fechas, cohortes)
ceps_txn_replacement_output = cc_tr.output             # outputs del step de sustitución de txns CEPS
special_treatment_output = c_gmc_st.output             # outputs del special treatment de graph_making
special_treatment_input = c_gmc_st.input               # inputs del special treatment de graph_making
group_by_output = c_gmc_gb.output                      # outputs del group_by (agregados por id/txn)
edges_and_nodes_output = c_gmc_ean.output              # outputs de aristas y nodos del grafo



# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# GRAPH

# features to be calculated for the edges, this vars can be used to estimate the weight.
# Catálogo importable: libs.functions.features.STANDARD_WEIGHT_STATS
# Mapa nombre -> función de agregación sobre la columna `weight` de las aristas
# (min, max, mean, std, count, countDistinct, sum, curt, skew, so).
WEIGHTED_EDGE_STARTS_FEATURES = STANDARD_WEIGHT_STATS

# Define the aggregations for the weighted edge features, DON'T MOVE THIS, IT IS USED IN THE GRAPH FEATURES STEP.
# Alias de las agregaciones generadas como f"{func}_weight" a partir del catálogo anterior.
WEIGHTED_EDGE_STARTS_FEATURES_AGGREGATIONS = [
    # func(variable).alias(f"{func_name}_{variable}") for variable in ["weight"] for func_name, func in WEIGHTED_EDGE_STARTS_FEATURES.items()
    "min_weight",   # mínimo peso de las aristas incidentes del nodo
    "max_weight",   # máximo peso de las aristas incidentes del nodo
    "mean_weight",  # media del peso de las aristas incidentes del nodo
    "so_weight"     # momento de orden 4 normalizado (sum(w^4)/sum(w^2)^2)
]

# Reasignación que sobrescribe la lista anterior dejándola vacía: sin una lista
# explícita de alias, `weighted_edge_stats` usa el catálogo STANDARD_WEIGHT_STATS.
WEIGHTED_EDGE_STARTS_FEATURES_AGGREGATIONS = [
    # func(variable).alias(f"{func_name}_{variable}") for variable in ["weight"] for func_name, func in WEIGHTED_EDGE_STARTS_FEATURES.items()
]


# Select final vars to the edges, this vars can be used to estimate the weight.

# Renombrados aplicados en `get_graph` sobre aristas/nodos para obtener las
# columnas que GraphFrames exige: src/dst en aristas.
GRAPH_RENAMES = {
    "id_src": "src",  # id del nodo origen de la arista
    "id_dst": "dst"   # id del nodo destino de la arista
}

# Prefijos que clasifican una feature como "ponderada": el step unweighted
# ejecuta las que NO empiezan por estos prefijos y el weighted las que sí.
GRAPH_ALL_FEATURE_PREFIXES = [
    "weighted"
]

# Features de grafo a ejecutar (nombre -> kwargs). Las "weighted_*" reciben
# `weight`: clave del mapa `weights` de las aristas (ver WEIGHT_COLUMNS).
# Los kwargs escalares además parametrizan el subdirectorio de salida
# (p.ej. weight=composed) y el resto pasan a la función de la feature.
GRAPH_CENTRALITY_FEATURES = [
    {"pagerank": None},                                  # PageRank no ponderado por nodo (max_iter=10, reset_prob=0.15 por defecto)
    {"degrees": None},                                   # grado de entrada/salida/total por nodo
    {"components": None},                                # id de la componente conexa de cada nodo
    # {"triangle_count": None},                          # nº de triángulos en los que participa cada nodo
    {"weighted_pagerank": {"weight": "count_txn"}},      # PageRank ponderado por el peso "composed" de las aristas
    {"weighted_degrees": {"weight": "count_txn"}},       # fuerza (strength) in/out/total con peso "composed"
    {"weighted_edge_stats": {"weight": "count_txn"}},    # estadísticas del peso de aristas incidentes (in+out)
    # {"weighted_triangle_count": {"weight": "composed"}}, # triángulos por nodo + fuerza ponderada
]



# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------

_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))   # raíz HDFS de las salidas del job
current_hdfs = _general_root_hdfs.joinpath("features/ceps/graph_features")       # salidas persistentes del step
current_tmp_hdfs = _general_root_hdfs.joinpath("tmp/features/ceps/graph_features")  # salidas temporales (checkpoint)


input = {
    "edges": edges_and_nodes_output["edges"],  # aristas del grafo (src/dst + mapa `weights`), particionadas
    "nodes": edges_and_nodes_output["nodes"]   # nodos del grafo (columna `id`), particionados
}


# Una clave por feature de GRAPH_CENTRALITY_FEATURES: parquet simple por nodo.
# "keep_or_delete": "keep" = salida persistente; "delete" = temporal (--cleanup).
output = {
    "pagerank": {"table_or_hdfs": current_hdfs.joinpath("pagerank"),#simple
        "keep_or_delete": "keep"
    },
    "degrees": {"table_or_hdfs": current_hdfs.joinpath("degrees"),#simple
        "keep_or_delete": "keep"
    },
    "components": {"table_or_hdfs": current_hdfs.joinpath("components"),#simple
        "keep_or_delete": "keep"
    },
    "triangle_count": {"table_or_hdfs": current_hdfs.joinpath("triangle_count"),#simple
        "keep_or_delete": "keep"
    },
    "k_core": {"table_or_hdfs": current_hdfs.joinpath("k_core"),#simple
        "keep_or_delete": "keep"
    },
    "weighted_pagerank": {"table_or_hdfs": current_hdfs.joinpath("weighted_pagerank"),#simple
        "keep_or_delete": "keep"
    },
    "weighted_degrees": {"table_or_hdfs": current_hdfs.joinpath("weighted_degrees"),#simple
        "keep_or_delete": "keep"
    },
    "weighted_edge_stats": {"table_or_hdfs": current_hdfs.joinpath("weighted_edge_stats"),#simple
        "keep_or_delete": "keep"
    },
    "weighted_triangle_count": {"table_or_hdfs": current_hdfs.joinpath("weighted_triangle_count"),#simple
        "keep_or_delete": "keep"
    },
    "checkpoint": {"table_or_hdfs": current_tmp_hdfs.joinpath("checkpoint"),#simple  # dir. de checkpoint de GraphFrames
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    }
}
