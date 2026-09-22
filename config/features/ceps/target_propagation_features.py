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

TARGETS = {
    "lovelace":{
        "modes": {
            "cta":{
                "input_key":"target_cta",
                "suffix":"cta",
                "aggregation_function": spark_max,
                "missing_treatment": ["mean_with_nulls"]
            },
            "numcliente":{
                "input_key":"target_numcliente",
                "suffix":"numcliente",
                "aggregation_function": spark_max,
                "missing_treatment": ["mean_with_nulls"]
            }
        }
    }
}


TARGET_SELECTION_FUNCTION = greatest


# --------------------------------------------------------------------------------------
# PROPAGATION
# --------------------------------------------------------------------------------------

# Tipos de peso sobre los que se propaga el target (claves del mapa `weights` en edges).
WEIGHT_TYPES = list(ctp_en_st.WEIGHT_COLUMNS.keys())

# Hiperparámetros de la difusión de contagio (standard_propagate_target).
PROPAGATION_MAX_ITER = 3
PROPAGATION_ALPHA = 0.15
PROPAGATION_KEEP_SEED_FLOOR = True

# Features de contagio generadas: una por weight_type.
PROPAGATION_FEATURES = [
    {f"contagion_{weight_type}": {
        "weight_type": weight_type,
        "max_iter": PROPAGATION_MAX_ITER,
        "alpha": PROPAGATION_ALPHA,
        "keep_seed_floor": PROPAGATION_KEEP_SEED_FLOOR,
    }}
    for weight_type in WEIGHT_TYPES
]



# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("features/ceps/propagation_features")
current_tmp_hdfs = _general_root_hdfs.joinpath("tmp/features/ceps/propagation_features")


propagation_hdfs = ctp_l_st.current_hdfs.parent


input = ({
    "group_by_id" : ctp_gb_st.output["group_by_id"],
    "nodes" : ctp_en_st.output["nodes"],
    "edges" : ctp_en_st.output["edges"]
    }
    # TARGETS
    # add other targets as neccesary
    | ctp_l_st.output
)


output = {
    "nodes_join_target_lovelace": {"table_or_hdfs": propagation_hdfs.joinpath("nodes_join_target_lovelace"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": c_gmc.GRAPH_GLOBAL_LAG,
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    },
    "edges_norm": {"table_or_hdfs": current_hdfs.joinpath("edges_norm"),#simple
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    },
    "target_propagation": {"table_or_hdfs": current_hdfs.joinpath("target_propagation"),#simple
        "keep_or_delete": "keep"
    },
    "checkpoint": {"table_or_hdfs": current_tmp_hdfs.joinpath("checkpoint"),#simple
        "keep_or_delete": "delete"     # intermedio: se borra con --cleanup
    }
}
