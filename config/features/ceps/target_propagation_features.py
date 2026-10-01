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
#
# Cada modo admite dos formas de declarar las columnas a propagar:
#   - simple (back-compat): "aggregation_function" + "missing_treatment"
#     aplicados a la columna "target".
#   - multi-columna: "columns" = {columna: {"aggregation_function": f,
#     "missing_treatment": [...]}} — agrega y propaga TODAS esas columnas de
#     la tabla de entrada (target, scores, etc.), produciendo
#     `target_lovelace_<suffix>_<columna>` por nodo.
TARGETS = {
    "lovelace":{
        "modes": {
            "cta":{  # agregación del target por cuenta beneficiaria
                "input_key":"target_cta",               # clave de `input` con el target ya agregado por cta
                "suffix":"cta",                         # columna (array) de group_by_id por la que se agrega; sufijo de la columna resultante
                "aggregation_function": spark_max,      # función de agregación por defecto de las columnas
                "missing_treatment": ["mean_with_nulls"]# imputación de nulos por defecto: media contando nulos como 0
                # ,"columns": {                         # forma multi-columna (opcional):
                #     "target": {"aggregation_function": spark_max, "missing_treatment": ["mean_with_nulls"]},
                #     "score":  {"aggregation_function": spark_mean, "missing_treatment": ["mean_with_nulls"]},
                # }
            },
            "numcliente":{  # agregación del target por cliente
                "input_key":"target_numcliente",        # clave de `input` con el target ya agregado por numcliente
                "suffix":"numcliente",                  # columna (array) de group_by_id por la que se agrega; sufijo de la columna resultante
                "aggregation_function": spark_max,      # función de agregación por defecto
                "missing_treatment": ["mean_with_nulls"]# imputación de nulos por defecto: media contando nulos como 0
            }
        }
    }
}


# Combina las columnas `target_lovelace_*` de todos los modos en
# `target_lovelace[_<columna>]` (greatest = se queda con el máximo entre cta y
# numcliente). Se aplica por separado a cada columna propagada.
TARGET_SELECTION_FUNCTION = greatest


def _mode_columns(mode: dict) -> dict:
    """Columnas a propagar de un modo: normaliza la forma simple a la de dict."""
    if "columns" in mode:
        return mode["columns"]
    return {"target": {
        "aggregation_function": mode["aggregation_function"],
        "missing_treatment": mode.get("missing_treatment"),
    }}


def propagation_node_columns(targets: dict = None) -> list:
    """Columnas `target_lovelace*` combinadas que quedan en los nodos.

    "target" -> "target_lovelace"; cualquier otra columna (scores, etc.) ->
    "target_lovelace_<columna>". Útil para `node_final_columns` del grafo.
    """
    if targets is None:
        targets = TARGETS
    columns = set()
    for target in targets.values():
        for mode in target["modes"].values():
            columns.update(_mode_columns(mode).keys())
    return sorted(
        "target_lovelace" if column == "target" else f"target_lovelace_{column}"
        for column in columns)


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

# Features de contagio generadas: una por (columna de target propagada x
# weight_type). La columna semilla `target_lovelace` produce `contagion_<weight>`;
# el resto (`target_lovelace_<col>`: scores, etc.) produce
# `contagion_<col>_<weight>`. Cada kwargs va a `propagate_target` y, al ser
# escalares, parametriza el subdirectorio de salida
# (weight_type=.../target_column=.../max_iter=.../alpha=.../keep_seed_floor=...).
PROPAGATION_FEATURES = [
    {(
        f"contagion_{weight_type}"
        if node_column == "target_lovelace"
        else f"contagion_{node_column.replace('target_lovelace_', '')}_{weight_type}"
    ): {
        "weight_type": weight_type,                       # clave del mapa `weights` usada como peso de arista
        "target_column": node_column,                     # columna semilla en los nodos a propagar
        "max_iter": PROPAGATION_MAX_ITER,                 # iteraciones de difusión
        "alpha": PROPAGATION_ALPHA,                       # factor de amortiguación de la difusión
        "keep_seed_floor": PROPAGATION_KEEP_SEED_FLOOR,   # floor en la semilla del target
    }}
    for node_column in propagation_node_columns()
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
