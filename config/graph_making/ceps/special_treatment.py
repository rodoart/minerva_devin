from libs.data_engineering_toolbox.path import HivePath
from ...job import sbx as job_config
from pyspark.sql.functions import col,lpad
from pyspark.sql.types import StringType
from ...ceps.txn_replacement import output as ceps_txn_replacement_output
from .. import GRAPH_TOTAL_HISTORY_IN_MONTHS, GRAPH_GLOBAL_LAG

_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("graph/ceps/special_treatment")

STANDARD_MISSING_TREATMENT_VARS = {
    "oper_mto": ["value", 0]
}



# TODO THIS INPUT
input = {
    "s264_ceps_flattened_rank_rfc_by_cta_cases_replace" : {
        "table_or_hdfs": ceps_txn_replacement_output["rfc_curp_analysis_s264_ceps_replaced"]["table_or_hdfs"],
        "information_date_column": ceps_txn_replacement_output["rfc_curp_analysis_s264_ceps_replaced"]["information_date_column"],
        "lag": GRAPH_GLOBAL_LAG,
        "history": GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":ceps_txn_replacement_output["rfc_curp_analysis_s264_ceps_replaced"]["information_date_mode"],
        "minimum_required_history": False,
        "select":

        [
            col("customer_id").alias("numcliente"),
            col("rfc_curp_ord").alias("id_src"),
            col("rfc_curp_ben").alias("id_dst"),
            col("cta_ord").alias("cta_src"),
            col("cta_ben").alias("cta_dst"),
            col("nom_ord").alias("nom_src"),
            col("nom_ben").alias("nom_dst"),
            col("id_ban_ord").alias("id_ban_src"),
            col("id_ban_ben").alias("id_ban_dst"),
            col("fec_informacion").cast(StringType()).alias("information_date"),
            lpad(col("hora_oper").cast(StringType()), 6, "0").alias("hora_oper"),
            "cve_tipo_orden",
            "oper_mto"
        ]
    }
}


output = {
    "missing_treatment": {"table_or_hdfs": current_hdfs.joinpath("missing_treatment"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": GRAPH_TOTAL_HISTORY_IN_MONTHS,
        "information_date_mode":"each",
        "process_date_mode":"last"
    }
}
