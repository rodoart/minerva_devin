from data_engineering_toolbox.path import HivePath
from ..job import sbx as job_config
from pyspark.sql import Window
from pyspark.sql.functions import col

from .rfc_nom_ranking import (CEPS_MAXIMUM_HISTORY_IN_MONTHS, input as rfc_nom_ranking_input, output as rfc_nom_ranking_output
)


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("ceps/txn_replacement")
tmp_current_hdfs = _general_root_hdfs.joinpath("tmp/ceps/txn_replacement")


input = {
    "s264_ceps_flattened_rank_rfc_by_cta_cases_replace": rfc_nom_ranking_output["s264_ceps_flattened_rank_rfc_by_cta_cases_replace"],
    "s264_ceps_flattened_rank_rfc_by_nom_cases_replace": rfc_nom_ranking_output["s264_ceps_flattened_rank_rfc_by_nom_cases_replace"],
    "rfc_curp_analysis_s264_ceps": rfc_nom_ranking_output["rfc_curp_analysis_s264_ceps"]
}
print("input", input, sep="\n")

output = {
    "tmp_replaced_joined_hdfs": { #simple
        "table_or_hdfs": tmp_current_hdfs.joinpath("tmp_replaced_joined_hdfs"),
        "keep_or_delete": "keep"
    },
    "rfc_curp_analysis_s264_ceps_replaced": {"table_or_hdfs": current_hdfs.joinpath("rfc_curp_analysis_s264_ceps_replaced"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    }
}
