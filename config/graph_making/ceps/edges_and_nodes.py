##########################################################################################
# Libraries
##########################################################################################

# --------------------------------------------------------------------------------------
# Pyspark
# --------------------------------------------------------------------------------------
from pyspark.sql import Window
from pyspark.sql.functions import (col, lpad, min as spark_min,
    max as spark_max, mean as spark_mean, stddev as spark_std,
    sum as spark_sum, count, countDistinct, last, first, coalesce, lit,
    collect_set
)

# --------------------------------------------------------------------------------------
# CUSTOM
# --------------------------------------------------------------------------------------

from data_engineering_toolbox.path import HivePath
import data_engineering_toolbox.pyspark.tools as pdt

import config.job as c_j
import config.ceps.txn_replacement as cc_tr
import config.graph_making.ceps.special_treatment as c_gmc_st
import config.graph_making.ceps.group_by as c_gmc_gb
import config.graph_making as c_gmc

from importlib import reload

for module in [c_j, cc_tr, c_gmc_st, c_gmc, pdt]:
    reload(module)

##########################################################################################
# CONFIGURATION
##########################################################################################
job_config = c_j.sbx
ceps_txn_replacement_output = cc_tr.output
special_treatment_output = c_gmc_st.output
special_treatment_input = c_gmc_st.input
group_by_output = c_gmc_gb.output


# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# EDGE STEP

# Select final vars to the edges, this vars can be used to estimate the weight.
EDGES_VARS = [
    "mean_oper_mto",
    "max_tfrom_days",
    "sum_oper_mto",
    "weighted_mean_oper_mto",
    "weighted_sum_oper_mto",
    col("count_oper_mto").alias("count_txn"),
    col("weighted_count_oper_mto").alias("weighted_count_txn")
]


# TODO: Make a library of standard weight functions, and make it configurable in the job config.
WEIGHT_COLUMNS = {
    "weighted_mean_oper_mto": col("weighted_mean_oper_mto"),
    "weighted_sum_oper_mto": col("weighted_sum_oper_mto"),
    "weighted_count_txn": col("weighted_count_txn"),
    "mean_oper_mto": col("mean_oper_mto"),
    "count_txn": col("count_txn"),
    "composed": (col("weighted_mean_oper_mto") + col("mean_oper_mto")) / 2
}

# NODE STEP
NODE_VARS = [
    "*"
]
NODES_ID_COLUMNS="id"


# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("graph/ceps/edges_and_nodes")


input = {
    "group_by_txn" : group_by_output["group_by_txn"],
    "group_by_id" : group_by_output["group_by_id"]
}


output = {
    "edges": {"table_or_hdfs": current_hdfs.joinpath("edges"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": c_gmc.GRAPH_GLOBAL_LAG,
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    },
    "nodes": {"table_or_hdfs": current_hdfs.joinpath("nodes"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": c_gmc.GRAPH_GLOBAL_LAG,
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    },
}
