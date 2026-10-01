from libs.data_engineering_toolbox.path import HivePath
from ..job import sbx as job_config   # config del job (raíz HDFS de salidas, cohorte, lag, is_dynamic)


from libs.data_engineering_toolbox.context.logging import get_logger
logger = get_logger(__name__)


# Se reutilizan los outputs del pipeline de ranking: este step lee como entrada
# exactamente lo que CepsRfcNomRankingStep escribió (mismas rutas y mismas
# claves de particionado).
from .rfc_nom_ranking import output as rfc_nom_ranking_output


# Meses de historia TRANSACCIONAL que conserva la tabla `replaced`: el
# reemplazo de RFC/CURP se aplica solo a las transacciones de los últimos
# `TXN_REPLACED_HISTORY_IN_MONTHS` meses (por fec_informacion), aunque los
# catálogos de reemplazo se hayan construido con una ventana mucho más
# profunda (CEPS_RANKING_HISTORY_IN_MONTHS en config/ceps/rfc_nom_ranking.py).
# Ambas ventanas son independientes.
TXN_REPLACED_HISTORY_IN_MONTHS = 3


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
    # se lee vía standard_load_parquet_or_table respetando lag/history/mode.
    # history=1: cada partición (mis_date=vintage) ya contiene toda la ventana
    # del ranking (CEPS_RANKING_HISTORY_IN_MONTHS meses de txns), así que solo
    # hace falta la del último vintage; el recorte a la ventana de la tabla
    # replaced se hace después por fec_informacion (ver TXN_REPLACED_HISTORY_IN_MONTHS
    # y pipelines/ceps/txn_replacement.py::limit_txn_history_window).
    "rfc_curp_analysis_s264_ceps": {
        **rfc_nom_ranking_output["rfc_curp_analysis_s264_ceps"],
        "history": 1,
    }
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
    # partir de los casos rankeados, particionada por mis_date + process_date.
    # Cada partición (mis_date=vintage) contiene solo las transacciones de los
    # últimos TXN_REPLACED_HISTORY_IN_MONTHS meses; el grafo acumula la ventana
    # completa leyendo las particiones de los últimos vintages.
    "rfc_curp_analysis_s264_ceps_replaced": {"table_or_hdfs": current_hdfs.joinpath("rfc_curp_analysis_s264_ceps_replaced"),
        "information_date_column": "mis_date",      # columna-partición de fecha de información (yyyyMM)
        "process_date_column": "process_date",      # columna-partición de fecha de carga
        "lag": 0,                                   # sin desfase: el intervalo termina en el mes vintage
        "history": 1,                               # al releer, solo la partición del vintage actual
        "information_date_mode":"each",             # todos los meses del intervalo
        "process_date_mode":"last"                  # por cada mes, solo la carga (process_date) más reciente
    }
}
