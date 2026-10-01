"""Config input/output del pipeline de ejemplo: transacciones -> agregados.

Demuestra el esquema de los dicts `input`/`output` que consume el framework:

  "table_or_hdfs"            -> tabla Hive o ruta HDFS del parquet.
  "information_date_column"  -> partición con la fecha de información (mes).
  "process_date_column"      -> partición con la fecha de proceso (opcional).
  "lag"                      -> meses de desfase de la ventana vs. vintage_date.
  "history"                  -> nº de meses de historia a leer.
  "information_date_mode"    -> "each" todos los meses | "first"/"last".
  "process_date_mode"        -> "last"/"first"/"all"/"each" por mes.
  "minimum_required_history" -> meses mínimos exigidos al cargar (opcional).
  "select"                   -> proyección aplicada tras la carga (opcional).
  "keep_or_delete"           -> "delete" marca la salida como intermedia
                                borrable con `Step.delete_tmp_paths()`.
"""
from pyspark.sql.functions import col

from libs.data_engineering_toolbox.path import HivePath
from .job import job as job_config

# Ventana de historia del ejemplo (meses de transacciones a agregar).
HISTORY_IN_MONTHS = 12
GLOBAL_LAG = 0

# Raíz de salida del job.
_root = HivePath(str(job_config["general_root_hdfs"]))
_extract_hdfs = _root.joinpath("extract")
_groupby_hdfs = _root.joinpath("group_by")


# ----------------------------------------------------------------------------
# Step 1: extract (special treatment)
# ----------------------------------------------------------------------------
MISSING_TREATMENT = {
    "amount": ["value", 0],          # importe: nulos -> 0
    "channel": ["value", "unknown"], # canal: nulos -> "unknown"
}

extract_input = {
    # Tabla de transacciones crudas, particionada por information_date/process_date.
    "raw_txns": {
        "table_or_hdfs": _root.joinpath("raw_txns"),
        "information_date_column": "information_date",
        "process_date_column": "process_date",
        "lag": GLOBAL_LAG,
        "history": HISTORY_IN_MONTHS,
        "information_date_mode": "each",
        "process_date_mode": "last",
        "minimum_required_history": False,
        "select": [
            col("src").alias("id_src"),        # id del origen
            col("dst").alias("id_dst"),        # id del destino
            col("amount").alias("oper_mto"),   # importe
            col("event_date").alias("information_date"),  # fecha de información
            "channel",                          # canal de la operación
        ],
    }
}

extract_output = {
    # Transacciones limpias: una partición por mes (mis_date) y fecha de proceso.
    "txns_clean": {
        "table_or_hdfs": _extract_hdfs.joinpath("txns_clean"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": HISTORY_IN_MONTHS,
        "information_date_mode": "each",
        "process_date_mode": "last",
    }
}


# ----------------------------------------------------------------------------
# Step 2: group_by incremental mensual
# ----------------------------------------------------------------------------
GROUPING_VARS = ["id_src", "id_dst"]   # la "arista" del ejemplo
VALUE_COLUMNS = ["oper_mto"]           # variables de valor a agregar

# Agregaciones declarativas "{func}_{variable}"; el catálogo de funciones es
# `libs.functions.aggregations.standard_group_by_features`.
GROUP_TXN_AGGREGATIONS = [
    "min_oper_mto",
    "max_oper_mto",
    "mean_oper_mto",
    "sum_oper_mto",
    "count_oper_mto",
    "std_oper_mto",
    "max_tfrom_days",
    "weighted_mean_oper_mto",
    "weighted_sum_oper_mto",
]

# Funciones habilitadas para resolver los nombres anteriores.
GROUP_BY_ENABLED_FEATURES = [
    "min", "max", "mean", "sum", "count", "std",
    "weighted_mean", "weighted_sum",
]

group_by_input = {
    # Se reutiliza la config de salida del step extract como input.
    "txns_clean": extract_output["txns_clean"],
}

group_by_output = {
    # Parciales combinables por arista-MES: base incremental de txns_grouped.
    # `ensure_monthly_partitions` solo computa los meses ausentes de la ventana.
    "txns_monthly": {
        "table_or_hdfs": _groupby_hdfs.joinpath("txns_monthly"),
        "information_date_column": "month_partition",  # fin de mes de la txn
        "process_date_column": "process_date",
        "lag": 0,
        "history": HISTORY_IN_MONTHS,
        "information_date_mode": "each",
        "process_date_mode": "last",
    },
    # Agregado final por arista sobre toda la ventana.
    "txns_grouped": {
        "table_or_hdfs": _groupby_hdfs.joinpath("txns_grouped"),
        "information_date_column": "mis_date",
        "process_date_column": "process_date",
        "lag": 0,
        "history": HISTORY_IN_MONTHS,
        "information_date_mode": "each",
        "process_date_mode": "last",
    },
}
