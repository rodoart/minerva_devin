##########################################################################################
# Libraries
##########################################################################################

from libs.data_engineering_toolbox.path import HivePath

import config.job as c_j
import config.graph_making.ceps.edges_and_nodes as ccen
import config.features.ceps.target_propagation_features as cfcf

##########################################################################################
# CONFIGURATION
##########################################################################################

job_config = c_j.sbx  # config del job (raíz HDFS, fechas, cohortes)

# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# Semillas DURAS: columnas binarias/continuas de `nodes_join_target_lovelace`.
# Cada una produce una familia de columnas risk_*_<columna>: exposición de cada
# nodo al conjunto semilla (fracción de vecinos semilla, semillas a 2 saltos,
# ratio de flujo, distancia a la semilla y Personalized PageRank desde ellas).
RISK_HARD_SEED_COLUMNS = ["target_lovelace"]

# Semillas POST-PROPAGACIÓN: pesos de contagio cuyas columnas `contagion_<w>`
# (leídas del parquet `target_propagation` con merge_schema) se usan como
# semilla soft — las mismas familias risk_*_contagion_<w> miden la exposición
# al riesgo YA propagado, para comparar con la exposición al target duro.
RISK_CONTAGION_WEIGHTS = ["composed", "count_txn"]

# Un nodo es semilla (para las familias basadas en listas de ids: 2-hop, flujo,
# distancia y PPR) si su score supera este umbral. Las semillas duras usan el
# suyo propio (target > 0); el de contagio es más alto para no declarar
# semilla a medio grafo (el contagio difunde score > 0 a todo lo alcanzable).
RISK_HARD_SEED_THRESHOLD = 0.0
RISK_CONTAGION_SEED_THRESHOLD = 0.5

# Direcciones de la distancia a la semilla más cercana (`risk_dist_<dir>_<semilla>`):
#   "to"   -> del nodo HACIA la semilla (¿a cuántos saltos puede mandar dinero al fraude?)
#   "from" -> DESDE la semilla hacia el nodo (¿a cuántos saltos llega el dinero del fraude?)
RISK_DISTANCE_DIRECTIONS = ("to", "from")

# Columna de peso de las aristas para el ratio de flujo (si existe; si no,
# cada arista pesa 1).
RISK_FLOW_WEIGHT_COLUMN = "count_txn"

# Pares candidatos (A, X): A es semilla de estas fuentes (sufijos de semilla;
# típicamente el target duro) y X está a <= PAIR_MAX_HOPS dirigidos de A.
# Sobre ellos se calculan las proximidades de link prediction
# (vecinos comunes, Jaccard, Adamic-Adar, preferential attachment,
# reciprocidad, caminos de longitud 2 y distancia mínima A->X).
PAIR_CANDIDATE_SEED_SUFFIXES = ["target_lovelace"]
PAIR_MAX_HOPS = 2

# Personalized PageRank paralelo desde las semillas.
PPR_MAX_ITER = 10
PPR_RESET_PROB = 0.15
PPR_MAX_SEEDS = 5000       # por encima se usa una muestra determinista (hash)

# Estadísticos con los que las features de par se agregan a nivel nodo X
# (pair_features_node): pair_<f>_<stat> por id.
PAIR_NODE_AGGREGATE_STATS = ("max", "mean")

# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------

from config.graph_making.ceps.variants import DEFAULT_VARIANT, variant_subpath  # namespacing por variante de grafo (rfc/cta)

_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))      # raíz HDFS de las salidas del job


def build(variant: str = DEFAULT_VARIANT):
    """(input, output) de risk_features CEPS para una variante de grafo.

    - "rfc": rutas históricas intactas (cero recomputo de lo persistido).
    - otras: rutas namespaced bajo `features/ceps/<variant>/risk_features`
      e insumos (`edges`, `nodes_join_target_lovelace`, `target_propagation`)
      de la misma variante.
    """
    _ean_input, ean_output = ccen.build(variant)
    _tp_input, tp_output = cfcf.build(variant)

    current_hdfs = _general_root_hdfs.joinpath(
        variant_subpath("features/ceps", variant, "risk_features"))      # directorio de salida del step
    current_tmp_hdfs = _general_root_hdfs.joinpath(
        variant_subpath("features/ceps", variant, "tmp_risk_features"))  # tmp del step (checkpoint de GraphFrames)

    input = {
        # Aristas del grafo (src/dst + count_txn como peso de flujo).
        "edges": ean_output["edges"],
        # Nodos + etiquetas target_lovelace_* (semillas duras).
        "nodes_join_target_lovelace": tp_output["nodes_join_target_lovelace"],
        # Scores de contagio (semillas post-propagación; merge_schema).
        "target_propagation": tp_output["target_propagation"],
    }

    output = {
        # Una fila por id: todas las familias risk_*_<semilla> unidas.
        "risk_node_features": {"table_or_hdfs": current_hdfs.joinpath("risk_node_features"),
            "keep_or_delete": "keep"
        },
        # Una fila por par (a, x): proximidades topológicas pair_*.
        "pair_features": {"table_or_hdfs": current_hdfs.joinpath("pair_features"),
            "keep_or_delete": "keep"
        },
        # Features de par agregadas a nivel nodo X: pair_<f>_<stat> por id.
        "pair_features_node": {"table_or_hdfs": current_hdfs.joinpath("pair_features_node"),
            "keep_or_delete": "keep"
        },
        # Directorio de checkpoint de GraphFrames (shortestPaths / PPR).
        "checkpoint": {"table_or_hdfs": current_tmp_hdfs.joinpath("checkpoint"),
            "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
        },
    }
    return input, output


# Back-compat: el módulo expone el (input, output) de la variante por defecto.
input, output = build()
