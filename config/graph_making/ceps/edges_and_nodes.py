##########################################################################################
# Libraries
##########################################################################################

# --------------------------------------------------------------------------------------
# Pyspark
# --------------------------------------------------------------------------------------
from pyspark.sql.functions import col

from libs.functions.weights import build_weight_columns

# --------------------------------------------------------------------------------------
# CUSTOM
# --------------------------------------------------------------------------------------

from libs.data_engineering_toolbox.path import HivePath

import config.job as c_j
import config.ceps.txn_replacement as cc_tr
import config.graph_making.ceps.special_treatment as c_gmc_st
import config.graph_making.ceps.group_by as c_gmc_gb
import config.graph_making as c_gmc

##########################################################################################
# CONFIGURATION
##########################################################################################
job_config = c_j.sbx                            # config del job sandbox (rutas raíz HDFS)
ceps_txn_replacement_output = cc_tr.output      # salidas del step txn_replacement (config)
special_treatment_output = c_gmc_st.output      # salidas del step special_treatment (config)
special_treatment_input = c_gmc_st.input        # insumos del step special_treatment (config)
group_by_output = c_gmc_gb.output               # salidas del step group_by (insumos de este step)


# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# EDGE STEP

# Select final vars to the edges, this vars can be used to estimate the weight.
# Se conservan del group_by_txn: son los atributos de la arista y el insumo de las
# funciones de peso (WEIGHT_COLUMNS).
EDGES_VARS = [
    "mean_oper_mto",                                            # importe medio de las txns de la arista
    "max_tfrom_days",                                           # antigüedad máxima (días) de las txns de la arista
    "sum_oper_mto",                                             # importe total operado en la arista
    "weighted_mean_oper_mto",                                   # importe medio ponderado por recencia
    "weighted_sum_oper_mto",                                    # importe total ponderado por recencia
    col("count_oper_mto").alias("count_txn"),                   # nº de transacciones de la arista
    col("weighted_count_oper_mto").alias("weighted_count_txn")  # recuento ponderado por recencia
]


# Especificación declarativa de los pesos de arista: cada entrada genera una
# clave del mapa `weights` (libs.functions.weights.build_weights_map) que viaja
# en las aristas y se usa después en features y propagación de fraude.
# Formato: nombre_peso -> ("función", arg1, arg2, ...); el catálogo de funciones
# está en libs.functions.weights.STANDARD_WEIGHT_FUNCTIONS:
#   "column"   -> peso directo de una columna.
#   "composed" -> media simple de varias columnas.
#   "ratio"    -> cociente numerador/denominador (0 si el denominador <= 0).
WEIGHT_SPECS = {
    "weighted_mean_oper_mto": ("column", "weighted_mean_oper_mto"),       # peso = importe medio ponderado por recencia
    "weighted_sum_oper_mto": ("column", "weighted_sum_oper_mto"),         # peso = importe total ponderado por recencia
    "weighted_count_txn": ("column", "weighted_count_txn"),               # peso = recuento ponderado por recencia
    "mean_oper_mto": ("column", "mean_oper_mto"),                         # peso = importe medio
    "count_txn": ("column", "count_txn"),                                 # peso = nº de transacciones
    "composed": ("composed", "weighted_mean_oper_mto", "mean_oper_mto"),  # peso compuesto: media de los dos importes medios
}
WEIGHT_COLUMNS = build_weight_columns(WEIGHT_SPECS)

# NODE STEP
# Variables de nodo a conservar del group_by_id; ["*"] = todas las columnas salvo
# el id del nodo y las de fechas/partición (process_date, mis_date, vintage, cohort).
NODE_VARS = [
    "*"
]
# Columna(s) identificadora del nodo en la salida (generada al agrupar por id en group_by_id).
NODES_ID_COLUMNS="id"


# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------


# Raíz HDFS general del job y directorio donde este step persiste sus salidas.
_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("graph/ceps/edges_and_nodes")


# Insumos del step: reutilizan las definiciones de salida del step group_by
# (misma ruta y mismas claves de particionado/lectura; ver comentarios en special_treatment.input).
input = {
    "group_by_txn" : group_by_output["group_by_txn"],   # transacciones agregadas por arista -> base de "edges"
    "group_by_id" : group_by_output["group_by_id"]      # atributos agregados por nodo -> base de "nodes"
}


# Salidas del step: mismas claves que los insumos; describen dónde se persiste cada
# DataFrame y cómo se relee después (recarga de caché vía `dynamic_partitioned_table_or_parquet`).
output = {
    # Aristas del grafo: ids + EDGES_VARS + mapa `weights` + columnas de partición.
    "edges": {"table_or_hdfs": current_hdfs.joinpath("edges"),                # parquet de salida
        "information_date_column": "mis_date",            # partición de fecha de información (YYYY-MM) escrita/releída
        "process_date_column": "process_date",            # partición de fecha de proceso escrita/releída
        "lag": c_gmc.GRAPH_GLOBAL_LAG,                    # lag global del grafo al releer (0 = sin desfase)
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,   # al releer se cubre la ventana completa del grafo
        "information_date_mode":"each",                   # todos los meses del intervalo
        "process_date_mode":"last"                        # por cada mes se usa la fecha de proceso más reciente
    },
    # Nodos del grafo: id + NODE_VARS + columnas de partición.
    "nodes": {"table_or_hdfs": current_hdfs.joinpath("nodes"),                # parquet de salida
        "information_date_column": "mis_date",            # partición de fecha de información (YYYY-MM) escrita/releída
        "process_date_column": "process_date",            # partición de fecha de proceso escrita/releída
        "lag": c_gmc.GRAPH_GLOBAL_LAG,                    # lag global del grafo al releer (0 = sin desfase)
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,   # al releer se cubre la ventana completa del grafo
        "information_date_mode":"each",                   # todos los meses del intervalo
        "process_date_mode":"last"                        # por cada mes se usa la fecha de proceso más reciente
    },
}
