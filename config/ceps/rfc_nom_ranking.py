from data_engineering_toolbox.path import HivePath
from ..job import sbx as job_config
from pyspark.sql import Window
from pyspark.sql.functions import col


CEPS_MAXIMUM_HISTORY_IN_MONTHS = 3

CURP_PATTERN    = r"^[A-ZÑ&]{4}[0-9]{6}[HM][A-Z]{5}[A-Z0-9]{2}$"
RFC_FISICA_PATTERN = r"^[A-ZÑ&]{4}[0-9]{6}[A-Z0-9]{3}$"
RFC_FISICA_SIN_HOMOCLAVE_PATTERN = r"^[A-ZÑ&]{4}[0-9]{6}$"
RFC_MORAL_PATTERN = r"^[A-ZÑ&]{3}[0-9]{6}[A-Z0-9]{3}$"
TC_PATTERN    = r"^[0-9]{16}$"
CLABE_PATTERN    = r"^[0-9]{18}$"



RFC_NULL_SYNONYMS = ["", "ND", "RFC NO DISPONIBLE"]
NOM_NULL_SYNONYMS = ["", "ND"]


RFC_CURP_KIND_PRIORITY = {
"rfc_moral": 0,
"rfc_fisica": 1,
"curp": 2,
"rfc_fisica_sin_homoclave": 3,
"clabe": 4,
"tc":5,
"invalid": 6
}


# ranking for RFC and CURP by cta, prioritizing the following criteria:
RFC_BY_CTA_PRIORITY_WINDOW = (Window.partitionBy("cta")
.orderBy(
col("rfc_curp").isNull().asc(), # bad
col("nom").isNull().asc(), # bad
col("rfc_curp_ends_with_xxx").isNull().asc(), # bad
col("RFC_CURP_KIND_PRIORITY").isNull().asc(),
col("rfc_curp_ends_with_xxx").asc(), # bad
col("RFC_CURP_KIND_PRIORITY").asc(), # bad
col("cnt_rfc_by_cta").desc(),
col("tot_oper_sto").desc(),        # good
col("lst_fec_informacion_hora_oper").asc(), # good
col("rfc_curp").asc()
)
)

# Priority for RFC and CURP by nom, prioritizing the following criteria:
RFC_BY_NOM_PRIORITY_WINDOW = (Window.partitionBy("nom")
.orderBy(
col("rfc_curp").isNull().asc(), # bad
col("nom").isNull().asc(), # bad
col("rfc_curp_ends_with_xxx").asc(),  # bad
col("RFC_CURP_KIND_PRIORITY").asc(),  # bad
col("cnt_rfc_by_cta").desc(),
col("cnt_rfc_by_nom").desc(),         # good
col("tot_oper_sto").desc(),           # good
col("lst_fec_informacion_hora_oper").asc(), # good
col("rfc_curp").asc()
)
)


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("ceps/rfc_nom_raking")


input = {
"s264_ceps" : {
"table_or_hdfs": "gcpdlkmvpsd_prd_db.fz2s264_bxic0_t_d",
"information_date_column": "fec_informacion",
"lag": 0,
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,
"information_date_mode":"all",
"minimum_required_history": round(CEPS_MAXIMUM_HISTORY_IN_MONTHS/2)
}
}

output = {
"rfc_curp_analysis_s264_ceps": {"table_or_hdfs": current_hdfs.joinpath("rfc_curp_analysis_s264_ceps"),
"information_date_column": "mis_date",
"process_date_column": "process_date",
"lag": 0,
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,
"information_date_mode":"each",
"process_date_mode":"last"
},
"s264_ceps_flattened": {"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened"),
"information_date_column": "mis_date",
"process_date_column": "process_date",
"lag": 0,
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,
"information_date_mode":"each",
"process_date_mode":"last"
},
"s264_ceps_flattened_groupby": {"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_groupby"),
"information_date_column": "mis_date",
"process_date_column": "process_date",
"lag": 0,
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,
"information_date_mode":"each",
"process_date_mode":"last"
},
"s264_ceps_flattened_ranks": {"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_ranks"),
"information_date_column": "mis_date",
"process_date_column": "process_date",
"lag": 0,
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,
"information_date_mode":"each",
"process_date_mode":"last"
},
"s264_ceps_flattened_ranks": {"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_ranks"),
"information_date_column": "mis_date",
"process_date_column": "process_date",
"lag": 0,
"history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,
"information_date_mode":"each",
"process_date_mode":"last"
},
"s264_ceps_flattened_rank_rfc_by_cta_cases_replace": { #simple
"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_rank_rfc_by_cta_cases_replace"),
"keep_or_delete": "keep"
},
"s264_ceps_flattened_rank_rfc_by_nom_cases_replace": { #simple
"table_or_hdfs": current_hdfs.joinpath("s264_ceps_flattened_rank_rfc_by_nom_cases_replace"),
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
