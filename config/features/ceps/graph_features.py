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
import config.graph_making.ceps.edges_and_nodes as c_gmc_ean

import config.graph_making as c_gmc

from importlib import reload

for module in [c_j, cc_tr, c_gmc_st, c_gmc, pdt, c_gmc_ean]:
    reload(module)


##########################################################################################
# CONFIGURATION
##########################################################################################
job_config = c_j.sbx
ceps_txn_replacement_output = cc_tr.output
special_treatment_output = c_gmc_st.output
special_treatment_input = c_gmc_st.input
group_by_output = c_gmc_gb.output
edges_and_nodes_output = c_gmc_ean.output



# --------------------------------------------------------------------------------------
# PARAMETERS
# --------------------------------------------------------------------------------------

# GRAPH

# features to be calculated for the edges, this vars can be used to estimate the weight.
WEIGHTED_EDGE_STARTS_FEATURES = {
    "min": spark_min,
    "max": spark_max,
    "mean": lambda c: spark_mean(col(c)),
    "std": lambda c: coalesce(spark_std(col(c)), lit(0.0)),
    "count": count,
    "countDistinct": countDistinct,
    "sum": spark_sum,
    "curt": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),
    "skew": lambda c: spark_sum(col(c)**3)/spark_sum(col(c)**2)**(3/2),
    "so": lambda c: spark_sum(col(c)**4)/spark_sum(col(c)**2)**(4/2)
}

# Define the aggregations for the weighted edge features, DON'T MOVE THIS, IT IS USED IN THE GRAPH FEATURES STEP.
WEIGHTED_EDGE_STARTS_FEATURES_AGGREGATIONS = [
    # func(variable).alias(f"{func_name}_{variable}") for variable in ["weight"] for func_name, func in WEIGHTED_EDGE_STARTS_FEATURES.items()
    "min_weight",
    "max_weight",
    "mean_weight",
    "so_txn_number"
]

WEIGHTED_EDGE_STARTS_FEATURES_AGGREGATIONS = [
    # func(variable).alias(f"{func_name}_{variable}") for variable in ["weight"] for func_name, func in WEIGHTED_EDGE_STARTS_FEATURES.items()
]


# Select final vars to the edges, this vars can be used to estimate the weight.

GRAPH_RENAMES = {
    "id_src": "src",
    "id_dst": "dst"
}

GRAPH_ALL_FEATURE_PREFIXES = [
    "weighted"
]



# --------------------------------------------------------------------------------------
# RELATIVE PATHS
# --------------------------------------------------------------------------------------

_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("features/ceps/graph_features")
current_tmp_hdfs = _general_root_hdfs.joinpath("tmp/features/ceps/graph_features")


input = {
    "edges": edges_and_nodes_output["edges"],
    "nodes": edges_and_nodes_output["nodes"]
}


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
    "checkpoint": {"table_or_hdfs": current_tmp_hdfs.joinpath("checkpoint"),#simple
        "keep_or_delete": "keep"
    }
}
