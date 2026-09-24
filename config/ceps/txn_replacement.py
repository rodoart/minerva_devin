from libs.data_engineering_toolbox.path import HivePath
from ..job import sbx as job_config   # config del job (raíz HDFS de salidas, cohorte, lag, is_dynamic)


from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)


# Se reutilizan la historia máxima y los outputs del pipeline de ranking:
# este step lee como entrada exactamente lo que CepsRfcNomRankingStep escribió
# (mismas rutas y mismas claves de particionado).
from .rfc_nom_ranking import (CEPS_MAXIMUM_HISTORY_IN_MONTHS, output as rfc_nom_ranking_output
)


# Rutas base de este pipeline: salidas finales bajo "ceps/txn_replacement" y
# checkpoints/temporales bajo "tmp/ceps/txn_replacement".
_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))
current_hdfs = _general_root_hdfs.joinpath("ceps/txn_replacement")
tmp_current_hdfs = _general_root_hdfs.joinpath("tmp/ceps/txn_replacement")


# INPUTS: los dicts se toman del config del ranking, de modo que la
# ruta/partición leída coincide siempre con lo escrito aguas arriba.
input = {
    # casos de reemplazo por cuenta: dict simple -> parquet NO particionado;
    # el pipeline lo lee directo con read.parquet(table_or_hdfs)
    "s264_ceps_flattened_rank_rfc_by_cta_cases_replace": rfc_nom_ranking_output["s264_ceps_flattened_rank_rfc_by_cta_cases_replace"],
    # casos de reemplazo por nombre: idem
    "s264_ceps_flattened_rank_rfc_by_nom_cases_replace": rfc_nom_ranking_output["s264_ceps_flattened_rank_rfc_by_nom_cases_replace"],
    # análisis RFC/CURP depurado: parquet particionado (mis_date/process_date);
    # se lee vía standard_load_parquet_or_table respetando lag/history/mode
    "rfc_curp_analysis_s264_ceps": rfc_nom_ranking_output["rfc_curp_analysis_s264_ceps"]
}
logger.debug("input: %s", input)

output = {
    # checkpoints intermedios del join de reemplazo (un parquet por fuente y
    # lado, p.ej. fc_by_cta_ben.parquet): cortan el linaje de Spark cada 3
    # joins. Dict simple (2 claves) -> parquet no particionado; "keep" =
    # conservar ("delete" lo registraría en tmp_paths para borrado al final).
    "tmp_replaced_joined_hdfs": { #simple
        "table_or_hdfs": tmp_current_hdfs.joinpath("tmp_replaced_joined_hdfs"),
        "keep_or_delete": "keep"
    },
    # salida final: historial CEP con rfc_curp / rfc_curp_kind rellenados a
    # partir de los casos rankeados, particionada por mis_date + process_date
    "rfc_curp_analysis_s264_ceps_replaced": {"table_or_hdfs": current_hdfs.joinpath("rfc_curp_analysis_s264_ceps_replaced"),
        "information_date_column": "mis_date",      # columna-partición de fecha de información (yyyyMM)
        "process_date_column": "process_date",      # columna-partición de fecha de carga
        "lag": 0,                                   # sin desfase: el intervalo termina en el mes vintage
        "history": CEPS_MAXIMUM_HISTORY_IN_MONTHS,  # al releer, cubre los 3 meses de historia
        "information_date_mode":"each",             # todos los meses del intervalo
        "process_date_mode":"last"                  # por cada mes, solo la carga (process_date) más reciente
    }
}
