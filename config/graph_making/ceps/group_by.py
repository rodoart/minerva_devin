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


# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# GROUP BY TXN STEP

# TODO, Library with standard group by features, and make it configurable in the job config.
GROUP_BY_TXN_GROUPING_VARS = ["id_src", "id_dst"]

global_window = Window.partitionBy()  # sin claves - toda la columna

GROUP_BY_AUXILIARY_COLS = {
    "_max_tfrom_days": spark_max(col("tfrom_days")).over(global_window),
    "_recency_weight": (col("_max_tfrom_days") - col("tfrom_days") + lit(1))
}

GROUP_BY_TXN_FEATURES = {
    "min": spark_min,
    "max": spark_max,
    "mean": lambda c: spark_mean(col(c)),
    "weighted_mean": lambda c: spark_sum(col(c)*col("_recency_weight"))/spark_sum(col("_recency_weight")),
    "weighted_sum": lambda c: spark_sum(col(c)*col("_recency_weight")),
    "std": lambda c: coalesce(spark_std(col(c)), lit(0.0)),
    "count": count,
    "weighted_count": lambda c: spark_sum(col("_recency_weight")),
    "countDistinct": countDistinct,
    "sum": spark_sum,
    "last": lambda c: last(col(c), ignorenulls=True),
    "first": lambda c: first(col(c), ignorenulls=True),
    "curt": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),
    "skew": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),
    "so": lambda c: spark_sum(col(c)**4)/spark_sum(col(c)**2)**(4/2)
}


GROUP_BY_TXN_VARIABLES = [
    "oper_mto",
    "id_ban_src",
    "tfrom_days"
]


# set of aggregations to be applied to the group by step. Each aggregation is a tuple of (function, variable).
GROUP_TXN_AGGREGATIONS = [
    # func(variable).alias(f"{func_name}_{variable}") for variable in GROUP_BY_TXN_VARIABLES for func_name, func in GROUP_BY_TXN_FEATURES.items()
    "min_oper_mto",
    "max_oper_mto",
    "mean_oper_mto",
    "so_txn_number",
]


# set of variables to group by in the group by step. These variables are used to define a transaction between two entities. In this case, the source

# GROUP BY IDs
# Currently, the input columns are extracted from the special treatment input configuration. The source and destination columns are identified by th
INPUT_COLUMNS = [column if isinstance(column, str) else pdt.get_column_alias(column) for column in special_treatment_input["s264_ceps_flattened_rank_rfc_by_cta_cases_replace"]["columns"]]
ID_SRC_COLUMNS = "id_src"
ID_DST_COLUMNS = "id_dst"

SRC_COLUMNS = [column for column in INPUT_COLUMNS if column.endswith("_src") and column not in GROUP_BY_TXN_GROUPING_VARS]
DST_COLUMNS = [column for column in INPUT_COLUMNS if column.endswith("_dst") and column not in GROUP_BY_TXN_GROUPING_VARS]
COMMON_COLUMNS = [column for column in INPUT_COLUMNS if not column.endswith("_src") and not column.endswith("_dst") and column not in GROUP_BY_TXN_GROUPING_VARS]


GROUP_ID_AGGREGATIONS = [
    collect_set("numcliente").alias("numcliente"),
    collect_set("nom").alias("nom"),
    collect_set("cta").alias("cta"),
    collect_set("id_ban").alias("id_ban"),
    last("information_date").alias("information_date"),
    spark_sum("oper_mto").alias("oper_mto")
]


# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("graph/ceps/group_by")


input = {
    "missing_treatment" : special_treatment_output["missing_treatment"]
}


output = {
    "group_by_txn": {"table_or_hdfs": current_hdfs.joinpath("group_by_txn"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    },
    "group_by_id": {"table_or_hdfs": current_hdfs.joinpath("group_by_id"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": c_gmc.GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    }
}
