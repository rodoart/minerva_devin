##########################################################################################
# Libraries
##########################################################################################

# --------------------------------------------------------------------------------------
# Pyspark
# --------------------------------------------------------------------------------------


from pyspark.sql.functions import max as spark_max, greatest

# --------------------------------------------------------------------------------------
# CUSTOM
# --------------------------------------------------------------------------------------

import config.graph_making as c_gmc
import config.target_propagation.lovelace.special_treatment as ctp_l_st
import config.graph_making.ceps.group_by as ctp_gb_st
import config.graph_making.ceps.edges_and_nodes as ctp_en_st

from libs.data_engineering_toolbox.path import HivePath
from ...job import sbx as job_config

##########################################################################################
# CONFIGURATION
##########################################################################################


# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# Fuentes de target a propagar: {nombre_target: {"modes": {modo: config}}}.
# Cada modo agrega el target sobre `group_by_id` y lo une a los nodos como
# columna `target_lovelace_<suffix>`.
TARGETS = {
    "lovelace":{
        "modes": {
            "cta":{  # agregación del target por cuenta beneficiaria
                "input_key":"target_cta",               # clave de `input` con el target ya agregado por cta
                "suffix":"cta",                         # columna (array) de group_by_id por la que se agrega; sufijo de la columna resultante
                "aggregation_function": spark_max,      # función de agregación del target (máximo = fraude si alguna txn lo es)
                "missing_treatment": ["mean_with_nulls"]# imputación de nulos del agregado: media contando nulos como 0
            },
            "numcliente":{  # agregación del target por cliente
                "input_key":"target_numcliente",        # clave de `input` con el target ya agregado por numcliente
                "suffix":"numcliente",                  # columna (array) de group_by_id por la que se agrega; sufijo de la columna resultante
                "aggregation_function": spark_max,      # función de agregación del target (máximo)
                "missing_treatment": ["mean_with_nulls"]# imputación de nulos del agregado: media contando nulos como 0
            }
        }
    }
}


# Combina las columnas `target_lovelace_*` de todos los modos en `target_lovelace`
# (greatest = se queda con el máximo entre cta y numcliente).
TARGET_SELECTION_FUNCTION = greatest


# --------------------------------------------------------------------------------------
# PROPAGATION
# --------------------------------------------------------------------------------------

# Tipos de peso sobre los que se propaga el target (claves del mapa `weights` en edges).
# Equivale a: weighted_mean_oper_mto, weighted_sum_oper_mto, weighted_count_txn,
# mean_oper_mto, count_txn, composed.
WEIGHT_TYPES = list(ctp_en_st.WEIGHT_COLUMNS.keys())

# Hiperparámetros de la difusión de contagio (standard_propagate_target).
PROPAGATION_MAX_ITER = 3              # iteraciones de paso de mensajes por el grafo
PROPAGATION_ALPHA = 0.15              # amortiguación: peso del score entrante frente al propio (1-alpha)
PROPAGATION_KEEP_SEED_FLOOR = True    # el score de un nodo nunca cae por debajo de su semilla original

# Features de contagio generadas: una por weight_type. Cada kwargs va a
# `propagate_target` y, al ser escalares, parametriza el subdirectorio de salida
# (weight_type=.../target_column=.../max_iter=.../alpha=.../keep_seed_floor=...).
PROPAGATION_FEATURES = [
    {f"contagion_{weight_type}": {   # nombre de la feature y de su columna de salida
        "weight_type": weight_type,                       # clave del mapa `weights` usada como peso de arista
        "max_iter": PROPAGATION_MAX_ITER,                 # iteraciones de difusión
        "alpha": PROPAGATION_ALPHA,                       # factor de amortiguación de la difusión
        "keep_seed_floor": PROPAGATION_KEEP_SEED_FLOOR,   # floor en la semilla del target
    }}
    for weight_type in WEIGHT_TYPES
]



# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))            # raíz HDFS de las salidas del job
current_hdfs = _general_root_hdfs.joinpath("features/ceps/propagation_features")        # salidas persistentes del step
current_tmp_hdfs = _general_root_hdfs.joinpath("tmp/features/ceps/propagation_features") # salidas temporales (checkpoint)


# Directorio padre de special_treatment (target_propagation/lovelace): ahí se
# escribe nodes_join_target_lovelace, junto a target_cta/target_numcliente.
propagation_hdfs = ctp_l_st.current_hdfs.parent


input = ({
    "group_by_id" : ctp_gb_st.output["group_by_id"],  # correspondencia id -> arrays cta/numcliente de cada nodo
    "nodes" : ctp_en_st.output["nodes"],              # nodos del grafo
    "edges" : ctp_en_st.output["edges"]               # aristas con el mapa `weights` de tipos de peso
    }
    # TARGETS
    # add other targets as neccesary
    | ctp_l_st.output  # añade target_cta y target_numcliente (semillas del special treatment Lovelace)
)


output = {
    # Nodos enriquecidos con target_lovelace (tabla particionada por mis_date/process_date).
    "nodes_join_target_lovelace": {"table_or_hdfs": propagation_hdfs.joinpath("nodes_join_target_lovelace"),
        "information_date_column": "mis_date",        # columna de fecha de información (partición mensual)
        "process_date_column": "process_date",        # columna de fecha de proceso/ejecución (segunda partición)
        "lag": c_gmc.GRAPH_GLOBAL_LAG,                # desplazamiento de la ventana en meses respecto a vintage_date
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,  # nº de meses de historia de la ventana (12)
        "information_date_mode":"each",               # se usan todos los meses del intervalo
        "process_date_mode":"last"                    # por cada mes se toma la process_date más reciente
    },
    # Aristas con pesos normalizados por grado del receptor (w_src_to_dst/w_dst_to_src);
    # en disco se escribe por weight_type=... en un directorio hermano.
    "edges_norm": {"table_or_hdfs": current_hdfs.joinpath("edges_norm"),#simple
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    },
    # Directorio padre de los scores de contagio, parametrizado por subdirs
    # (weight_type=.../target_column=.../...) que el assembler lee con merge_schema.
    "target_propagation": {"table_or_hdfs": current_hdfs.joinpath("target_propagation"),#simple
        "keep_or_delete": "keep"
    },
    "checkpoint": {"table_or_hdfs": current_tmp_hdfs.joinpath("checkpoint"),#simple  # dir. de checkpoint de GraphFrames
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    }
}
