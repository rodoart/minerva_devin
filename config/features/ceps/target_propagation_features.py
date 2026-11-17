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


def propagation_node_columns(targets: dict = None, synthetic_rules: list = None) -> list:
    """Columnas `target_lovelace*` y `target_synthetic_*` que quedan en los nodos.

    "target" -> "target_lovelace"; cualquier otra columna (scores, etc.) ->
    "target_lovelace_<columna>"; las reglas sintéticas añaden
    `target_synthetic_<name>`. Con `targets` explícito (modo custom) las
    sintéticas solo entran si se piden vía `synthetic_rules`; con la config
    global (targets=None) se incluyen las de SYNTHETIC_TARGETS por defecto.
    Útil para `node_final_columns` del grafo.
    """
    if targets is None:
        targets = TARGETS
        if synthetic_rules is None:
            synthetic_rules = SYNTHETIC_TARGETS
    columns = set()
    for target in targets.values():
        for mode in target["modes"].values():
            columns.update(_mode_columns(mode).keys())
    lovelace_columns = sorted(
        "target_lovelace" if column == "target" else f"target_lovelace_{column}"
        for column in columns)
    return lovelace_columns + synthetic_node_columns(synthetic_rules)


# --------------------------------------------------------------------------------------
# PROPAGATION
# --------------------------------------------------------------------------------------

# Tipos de peso sobre los que se propaga el target (claves del mapa `weights` en edges).
# Equivale a: weighted_mean_oper_mto, weighted_sum_oper_mto, weighted_count_txn,
# mean_oper_mto, count_txn, composed.
WEIGHT_TYPES = list(ctp_en_st.WEIGHT_COLUMNS.keys())

# --------------------------------------------------------------------------------------
# SYNTHETIC TARGETS (Minerva es genérico: fraude, crédito, ...)
# --------------------------------------------------------------------------------------
# Cuando no hay etiqueta real (o se quiere un proxy), una regla sintética marca
# nodos a partir de una métrica de nodo ya calculada y produce la columna
# `target_synthetic_<name>` en los nodos, que se propaga por el grafo como
# cualquier otra semilla (genera `contagion_<name>_<weight>`).
#
# Regla: {"name", "source_key" (clave de output de graph_features, p.ej.
# "pagerank"), "column" (None = primera no-id), "rule"
# ("top_quantile"/"above_threshold"/"top_n"), "q"/"threshold"/"n",
# "value": "binary"|"score"}.
SYNTHETIC_TARGETS = [
    {"name": "pagerank_top", "source_key": "pagerank", "column": "pagerank",
     "rule": "top_quantile", "q": 0.99, "value": "binary"},
]

# Pesos con los que se propagan las semillas sintéticas (subset de
# WEIGHT_TYPES): el target real usa todos; las sintéticas son exploratorias y
# propagarlas por los 6 pesos multiplicaría el coste del step.
SYNTHETIC_WEIGHT_TYPES = ["composed"]


def synthetic_node_columns(rules: list = None) -> list:
    """Columnas `target_synthetic_<name>` de las reglas dadas ([] si None)."""
    return [f"target_synthetic_{rule['name']}" for rule in (rules or [])]

# Hiperparámetros de la difusión de contagio (standard_propagate_target).
PROPAGATION_MAX_ITER = 3              # iteraciones de paso de mensajes por el grafo
PROPAGATION_ALPHA = 0.15              # amortiguación: peso del score entrante frente al propio (1-alpha)
PROPAGATION_KEEP_SEED_FLOOR = True    # el score de un nodo nunca cae por debajo de su semilla original

# Buckets de sal para el groupBy de grado (suma de pesos por nodo): con skew
# de grado (supernodos) reparte el cálculo entre N reducers en dos etapas.
# 0/None desactiva la sal.
DEGREE_SALT_BUCKETS = 64

def _contagion_feature_name(node_column: str, weight_type: str) -> str:
    """Nombre de la feature de contagio: `contagion_[<semilla>_]<weight>`.

    - `target_lovelace`          -> `contagion_<weight>` (histórico)
    - `target_lovelace_<col>`    -> `contagion_<col>_<weight>` (histórico)
    - `target_synthetic_<name>`  -> `contagion_synthetic_<name>_<weight>`
    """
    if node_column == "target_lovelace":
        seed = ""
    elif node_column.startswith("target_lovelace_"):
        seed = node_column.replace("target_lovelace_", "") + "_"
    elif node_column.startswith("target_synthetic_"):
        seed = "synthetic_" + node_column.replace("target_synthetic_", "") + "_"
    else:
        seed = node_column + "_"
    return f"contagion_{seed}{weight_type}"


def _propagation_feature(node_column: str, weight_type: str) -> dict:
    """Entrada de PROPAGATION_FEATURES para una (semilla, weight_type)."""
    return {_contagion_feature_name(node_column, weight_type): {
        "weight_type": weight_type,                       # clave del mapa `weights` usada como peso de arista
        "target_column": node_column,                     # columna semilla en los nodos a propagar
        "max_iter": PROPAGATION_MAX_ITER,                 # iteraciones de difusión
        "alpha": PROPAGATION_ALPHA,                       # factor de amortiguación de la difusión
        "keep_seed_floor": PROPAGATION_KEEP_SEED_FLOOR,   # floor en la semilla del target
    }}


# Features de contagio generadas: una por (columna semilla x weight_type).
# Las semillas Lovelace (reales) se propagan con TODOS los pesos; las
# sintéticas solo con SYNTHETIC_WEIGHT_TYPES (son exploratorias). Cada kwargs
# va a `propagate_target` y, al ser escalares, parametriza el subdirectorio
# de salida (weight_type=.../target_column=.../max_iter=.../alpha=.../...).
PROPAGATION_FEATURES = [
    *[_propagation_feature(column, weight_type)
      for column in propagation_node_columns(synthetic_rules=[])
      for weight_type in WEIGHT_TYPES],
    *[_propagation_feature(column, weight_type)
      for column in synthetic_node_columns(SYNTHETIC_TARGETS)
      for weight_type in SYNTHETIC_WEIGHT_TYPES],
]



# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------

from config.graph_making.ceps.variants import DEFAULT_VARIANT, variant_subpath  # namespacing por variante de grafo (rfc/cta)
import config.graph_making.ceps.group_by as _ccgb
import config.graph_making.ceps.edges_and_nodes as _ccen


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))            # raíz HDFS de las salidas del job

# Directorio padre de special_treatment (target_propagation/lovelace): ahí se
# escribe nodes_join_target_lovelace (variante rfc), junto a
# target_cta/target_numcliente; el resto de variantes lo namespacen un nivel
# más abajo para no pisar el histórico.
propagation_hdfs = ctp_l_st.current_hdfs.parent


def build(variant: str = DEFAULT_VARIANT):
    """(input, output) de propagation_features CEPS para una variante de grafo.

    - "rfc": rutas históricas intactas (cero recomputo de lo persistido).
    - otras: rutas namespaced bajo `features/ceps/<variant>/propagation_features`
      e insumos (`group_by_id`, `nodes`, `edges`) de la misma variante. Las
      semillas `target_cta`/`target_numcliente` son EXTERNAS al grafo y se
      comparten entre variantes (no se namespacen).
    """
    _gb_input, gb_output = _ccgb.build(variant)
    _ean_input, ean_output = _ccen.build(variant)

    current_hdfs = _general_root_hdfs.joinpath(
        variant_subpath("features/ceps", variant, "propagation_features"))        # salidas persistentes del step
    current_tmp_hdfs = _general_root_hdfs.joinpath(
        variant_subpath("tmp/features/ceps", variant, "propagation_features"))    # salidas temporales (checkpoint)

    # nodes_join_target_lovelace: la variante por defecto conserva la ruta
    # histórica (target_propagation/lovelace/nodes_join_target_lovelace).
    nodes_join_parent = propagation_hdfs if variant == DEFAULT_VARIANT \
        else propagation_hdfs.joinpath(variant)

    input = ({
        "group_by_id" : gb_output["group_by_id"],   # correspondencia id -> arrays cta/numcliente de cada nodo
        "nodes" : ean_output["nodes"],              # nodos del grafo
        "edges" : ean_output["edges"]               # aristas con el mapa `weights` de tipos de peso
        }
        # TARGETS
        # add other targets as neccesary
        | ctp_l_st.output  # añade target_cta y target_numcliente (semillas del special treatment Lovelace)
    )

    output = {
        # Nodos enriquecidos con target_lovelace (tabla particionada por mis_date/process_date).
        "nodes_join_target_lovelace": {"table_or_hdfs": nodes_join_parent.joinpath("nodes_join_target_lovelace"),
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
    return input, output


# Back-compat: el módulo expone el (input, output) de la variante por defecto.
input, output = build()
