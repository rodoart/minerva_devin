from libs.data_engineering_toolbox.path import HivePath
from ...job import sbx as job_config
from pyspark.sql.functions import col


_general_root_hdfs = HivePath(str(job_config["general_root_hdfs"]))  # raíz HDFS de las salidas del job
current_hdfs = _general_root_hdfs.joinpath("target_propagation/lovelace/special_treatment")  # directorio de salida de este step

# Tratamiento de nulos por columna: {columna: [tipo, *args]} interpretado por
# `apply_missing_treatment` (libs.functions.missing_treatment).
# Tipos: "value" -> rellena con el literal; "mean"/"mean_with_nulls" -> media
# (la segunda cuenta los nulos como 0 al calcularla).
STANDARD_MISSING_TREATMENT_VARS = {
    "oper_mto": ["value", 0]  # oper_mto: los nulos se imputan a 0
}



input = {
    # Histórico del target Lovelace: fuente de la semilla que luego se propaga por el grafo.
    "lovelace_target" : {
        "table_or_hdfs": HivePath("/data/gcpandlmxcysp/work/hive/gcpandlmxcysp_work/ej33121/fraudes_auxiliar/model_v11/app/sampling/iter_2_seed_57370/dev_subset"),  # parquet/tabla origen (muestra de dev)
        "information_date_column": "fecha_e15",  # columna de fecha de información (snapshot a día 15 del mes)
        "lag": -3,  # desplazamiento de la ventana en meses (positivo = hacia atrás; aquí -3 la adelanta ~3 meses del vintage)
        "history": 2,  # nº de meses de historia que cubre la ventana
        "information_date_mode":"all",  # qué meses del intervalo se usan: "all"/"each" = todos; "first"/"last" = solo uno
        "minimum_required_history": False,  # nº mínimo de meses exigidos al cargar; False (=0) no exige historia completa
        "select":  # proyección aplicada tras la carga: renombra al esquema interno

        [
            col("num_cliente").alias("numcliente"),           # identificador de cliente
            col("beneficiaryaccountnumber").alias("cta"),     # cuenta beneficiaria de la transferencia
            col("ft_nac_propba").alias("target"),             # etiqueta de fraude (semilla de la propagación)
            col("fecha_e15").alias("information_date"),       # fecha de información de la partición
            col("to").alias("mis_date")                       # fecha MIS usada como partición mensual aguas abajo
        ]
    }
}

output = {
    # Target agregado a nivel numcliente (max de `target`); parquet simple sin particionar.
    "target_numcliente": {"table_or_hdfs": current_hdfs.joinpath("target_numcliente"),#simple
        "keep_or_delete": "keep"  # salida persistente ("delete" marcaría un temporal borrable con --cleanup)
    },
    # Target agregado a nivel cuenta (max de `target`); parquet simple sin particionar.
    "target_cta": {"table_or_hdfs": current_hdfs.joinpath("target_cta"),#simple
        "keep_or_delete": "keep"
    }
}
