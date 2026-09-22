from libs.data_engineering_toolbox.path import HivePath
from ...job import sbx as job_config
from pyspark.sql.functions import col


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("target_propagation/lovelace/special_treatment")

STANDARD_MISSING_TREATMENT_VARS = {
    "oper_mto": ["value", 0]
}



# TODO THIS INPUT
input = {
    "lovelace_target" : {
        "table_or_hdfs": HivePath("/data/gcpandlmxcysp/work/hive/gcpandlmxcysp_work/ej33121/fraudes_auxiliar/model_v11/app/sampling/iter_2_seed_57370/dev_subset"),
        "information_date_column": "fecha_e15",
        # "lag": # GRAPH_GLOBAL_LAG, TODO: Use real lag the uncommented line is a placeholder for the lag value, which should be determined based on the specific requirements
        # of the data processing task. The lag value is used to control the time window for data analysis, and it may vary depending on the nature of the data and the analysis being performed.
        # "history": # GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "lag": -3,
        "history": 2,
        "information_date_mode":"all",
        "minimum_required_history": False,
        "select":

        [
            col("num_cliente").alias("numcliente"),
            col("beneficiaryaccountnumber").alias("cta"),
            col("ft_nac_propba").alias("target"),
            col("fecha_e15").alias("information_date"),
            col("to").alias("mis_date")
        ]
    }
}

output = {
    "target_numcliente": {"table_or_hdfs": current_hdfs.joinpath("target_numcliente"),#simple
        "keep_or_delete": "keep"
    },
    "target_cta": {"table_or_hdfs": current_hdfs.joinpath("target_cta"),#simple
        "keep_or_delete": "keep"
    }
}
