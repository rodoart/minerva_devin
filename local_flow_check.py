#!/usr/bin/env python3
"""local_flow_check.py — E2E del flujo Minerva con Spark local y datos mínimos.

Monta un entorno "hiper ligero":
  - SparkSession local[2] (sin YARN ni HDFS).
  - Shim de `libs.data_engineering_toolbox.path` (_ls/exists/is_dir/mkdir/
    touch/rmdir/mv) -> filesystem local, para que HivePath y el IO de
    particiones funcionen sin cluster.
  - Redirige todos los `table_or_hdfs` de config/input/output a /tmp.
  - Tablas mínimas: CEPS crudo particionado por `fec_informacion` y Lovelace
    particionado por `fecha_e15`.

Uso: python local_flow_check.py
"""
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

# --- env vars ANTES de importar config -------------------------------------------------
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")
os.environ.setdefault("MINERVA_WORKSPACE_DIR_LINUX", "/tmp")
os.environ.setdefault("PYSPARK_QUEUE", "datalabs")
os.environ.setdefault("PYSPARK_PORT", "4040")
os.environ.setdefault("MINERVA_NAME", "minerva_local_test")
os.environ.setdefault("MINERVA_VENV_TAR_GZ_LINUX", "unused")
os.environ.setdefault("MINERVA_VENV_TAR_GZ_HDFS", "unused")
os.environ.setdefault("GRAPHFRAMES_JAR", "unused")

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S")
logger = logging.getLogger("local_flow")

LOCAL_ROOT = Path("/tmp/minerva_local")
TABLES_ROOT = LOCAL_ROOT / "tables"

# --------------------------------------------------------------------------------------
# 1) SparkSession local
# --------------------------------------------------------------------------------------
from pyspark.sql import SparkSession

GRAPHFRAMES_JAR_LOCAL = Path(__file__).resolve().parent / ".local_jars" / "graphframes-0.8.4-spark3.5-s_2.12.jar"

spark = (SparkSession.builder
    .master("local[2]")
    .appName("minerva_local")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.jars", str(GRAPHFRAMES_JAR_LOCAL))
    .config("spark.sql.warehouse.dir", str(LOCAL_ROOT / "warehouse"))
    .config("spark.graphframes.connectedComponents.algorithm", "graphframes")
    .config("spark.graphframes.connectedComponents.checkpointInterval", 3)
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
spark.sparkContext.setCheckpointDir(str(LOCAL_ROOT / "spark_checkpoint"))
logger.info("Spark %s listo", spark.version)

# --------------------------------------------------------------------------------------
# 2) Shim de path -> filesystem local
# --------------------------------------------------------------------------------------
import libs.data_engineering_toolbox.path as pm

def _local_ls(path, *args):
    base = Path(path)
    if not base.exists():
        return []
    it = base.rglob("*") if "-R" in args else base.glob("*")
    entries = []
    for p in sorted(it):
        if not p.exists():
            continue
        st = p.stat()
        entries.append({
            "file_type": "d" if p.is_dir() else "-",
            "permissions": "drwxrwxrwx" if p.is_dir() else "-rw-r--r--",
            "copies": "1", "user": "u", "group": "g",
            "size": str(st.st_size),
            "date_and_time": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M"),
            "path": str(p),
        })
    return entries

pm._ls = _local_ls
pm.exists = lambda p: Path(p).exists()
pm.is_dir = lambda p: Path(p).is_dir()
pm.is_file = lambda p: Path(p).is_file()
pm.mkdir = lambda p, *a: (Path(p).mkdir(parents=True) if "-p" in a else Path(p).mkdir(exist_ok=True))
def _touch(p):
    Path(p).parent.mkdir(parents=True, exist_ok=True)
    Path(p).touch()
pm.touch = _touch
pm.rmdir = lambda p, *a: shutil.rmtree(p, ignore_errors=True)
pm.mv = lambda s, d: shutil.move(str(s), str(d))
logger.info("Shim de HivePath -> fs local aplicado")

# --------------------------------------------------------------------------------------
# 3) Redirección de paths de config -> LOCAL_ROOT/tables
#    Mismo path de origen -> mismo path local (los dicts comparten valores).
# --------------------------------------------------------------------------------------
import config.job as cj
import config.ceps.rfc_nom_ranking as ccrbr
import config.ceps.txn_replacement as cctr
import config.graph_making.ceps.special_treatment as ccspt
import config.graph_making.ceps.group_by as ccgb
import config.graph_making.ceps.edges_and_nodes as ccgen
import config.features.ceps.graph_features as cfgf
import config.target_propagation.lovelace.special_treatment as ctp_l_st
import config.features.ceps.target_propagation_features as cfcf
import config.features.ceps.vector_assembler as cvas

CONFIG_MODULES = [ccrbr, cctr, ccspt, ccgb, ccgen, cfgf, ctp_l_st, cfcf, cvas]
_path_mapping = {}

def redirect_configs() -> None:
    for mod in CONFIG_MODULES:
        ns = mod.__name__.split(".")[-1]
        for attr in ("input", "output"):
            cfg_dict = getattr(mod, attr, None)
            if not isinstance(cfg_dict, dict):
                continue
            for key, cfg in cfg_dict.items():
                if isinstance(cfg, dict) and "table_or_hdfs" in cfg:
                    old = str(cfg["table_or_hdfs"])
                    if old not in _path_mapping:
                        _path_mapping[old] = pm.HivePath(
                            str(TABLES_ROOT / f"{ns}__{key}"))
                    cfg["table_or_hdfs"] = _path_mapping[old]

redirect_configs()
logger.info("Redirigidos %d paths de config a %s", len(_path_mapping), TABLES_ROOT)

def local_path(old_path: str) -> str:
    return str(_path_mapping[old_path])

# --------------------------------------------------------------------------------------
# 4) Datos mínimos
# --------------------------------------------------------------------------------------
# Esquema raw CEPS: los nombres *_emi/*_rec se renombran a *_ord/*_ben en s264_ceps.
CEPS_COLUMNS = [
    "cve_tipo_orden", "customer_id", "opa_cve", "fec_oper", "hora_oper",
    "oper_mto", "id_ban_emi", "id_ban_rec", "nom_emisor", "nom_ben",
    "nom_ban_emi", "nom_ban_rec", "cta_ord", "cta_ben",
    "rfc_curp_ord", "rfc_curp_ben", "tipo_cta_ord", "tipo_cta_ben",
    "id_kind_ord", "id_kind_ben", "tfrom", "fec_informacion",
]

def ceps_row(fec, ordenante, beneficiario, mto, customer="1001"):
    return (
        "E", customer, "012", fec, "103000",
        mto, "40012", "40021", ordenante[1], beneficiario[1],
        "BBVA", "SANTANDER", ordenante[2], beneficiario[2],
        ordenante[0], beneficiario[0], "CLABE", "CLABE",
        "1", "1", "0", fec,
    )

A = ("RFCA010101AAA", "ALFA SA", "001111111111111111")
B = ("RFCB020202BBB", "BETA SA", "002222222222222222")
C = ("RFCC030303CCC", "GAMMA SA", "003333333333333333")
D = ("CURPD040404DDD04", "DELTA PERSONA", "004444444444444444")

# Particiones fec_informacion diarias yyyy-mm-dd (ventana Feb-Jul 2025).
ceps_rows = [
    ceps_row("2025-04-15", A, B, "1000.00"),
    ceps_row("2025-04-15", B, C, "2000.00"),
    ceps_row("2025-05-15", A, C, "3000.00"),
    ceps_row("2025-05-15", C, A, "500.00"),
    ceps_row("2025-06-15", D, B, "750.00"),
    ceps_row("2025-06-15", A, B, "1500.00"),
    ceps_row("2025-07-15", B, A, "800.00"),
    ceps_row("2025-07-15", C, B, "950.00"),
]

ceps_input_path = local_path("gcpdlkmvpsd_prd_db.fz2s264_bxic0_t_d")
ceps_df = spark.createDataFrame(ceps_rows, CEPS_COLUMNS)
ceps_df.write.mode("overwrite").partitionBy("fec_informacion").parquet(ceps_input_path)
logger.info("CEPS crudo: %d filas -> %s", ceps_df.count(), ceps_input_path)

lovelace_rows = [
    ("1001", "001111111111111111", 0.91, "2025-09-15", "202509", 10.0, 1, "2025-09-15"),
    ("1002", "002222222222222222", 0.35, "2025-09-15", "202509", 20.0, 2, "2025-09-15"),
    ("1001", "001111111111111111", 0.60, "2025-10-15", "202510", 30.0, 3, "2025-10-15"),
    ("1003", "003333333333333333", 0.10, "2025-10-15", "202510", 40.0, 4, "2025-10-15"),
]
lovelace_cols = ["num_cliente", "beneficiaryaccountnumber", "ft_nac_propba",
    "fecha_e15", "to", "dispute_amt", "num_oper", "fecha_s015"]
lovelace_input_path = local_path(
    "/data/gcpandlmxcysp/work/hive/gcpandlmxcysp_work/ej33121/fraudes_auxiliar/model_v11/app/sampling/iter_2_seed_57370/dev_subset")
lovelace_df = spark.createDataFrame(lovelace_rows, lovelace_cols)
lovelace_df.write.mode("overwrite").partitionBy("fecha_e15").parquet(lovelace_input_path)
logger.info("Lovelace: %d filas -> %s", lovelace_df.count(), lovelace_input_path)

# --------------------------------------------------------------------------------------
# 5) Ejecutar flujo completo
# --------------------------------------------------------------------------------------
# Debug: capturar date_format/partitions en el punto de fallo
import libs.data_engineering_toolbox.pyspark.tools.partition_lags as _pl
_orig_fmt = _pl.SparkTwoPartition._format_list_of_partitions
def _fmt_dbg(self, partitions, date_format):
    try:
        return _orig_fmt(self, partitions, date_format)
    except Exception:
        logger.error("STRPTIME FAIL cls=%s date_format=%r partitions=%s",
            type(self).__name__, date_format, partitions)
        raise
_pl.SparkTwoPartition._format_list_of_partitions = _fmt_dbg

# Local: los checkpoints de GraphFrames van a fs local (sin hdfs://)
import pipelines.features.graph_features as _pfg
def _local_define_checkpoint(self, checkpoint_hdfs):
    self.sqlContext.sparkContext.setCheckpointDir(str(checkpoint_hdfs))
_pfg.SubStep.define_checkpoint = _local_define_checkpoint

# Local: conjunto mínimo de features de grafo (una por categoría, un peso).
# Se excluyen las más pesadas: components/triangle_count/k_core/motifs.
import config.features.ceps.graph_features as _cfgf
_cfgf.GRAPH_CENTRALITY_FEATURES = [
    {"degrees": None},                            # unweighted, la más ligera
    {"weighted_degrees": {"weight": "composed"}}, # una weighted, un solo peso
]
logger.info("Features de grafo recortadas a: %s", _cfgf.GRAPH_CENTRALITY_FEATURES)

# FEATURE_SOURCES guarda referencias a los HivePath antiguos (pre-redirect)
# y enumera todas las features -> recortar a las producidas y re-apuntar paths.
import config.features.ceps.vector_assembler as _cvas
_keep_sources = {"degrees", "weighted_degrees", "contagion",
                 "nodes_join_target_lovelace"}
_cvas.FEATURE_SOURCES = {
    k: v for k, v in _cvas.FEATURE_SOURCES.items() if k in _keep_sources}
for _name, _src in _cvas.FEATURE_SOURCES.items():
    if "path" in _src:
        _src["path"] = _path_mapping.get(str(_src["path"]), _src["path"])
# weighted_degrees escribe bajo .../weight=composed -> apuntar al parquet exacto
_cvas.FEATURE_SOURCES["weighted_degrees"]["path"] = pm.HivePath(
    str(TABLES_ROOT / "graph_features__weighted_degrees" / "weight=composed"))

import main

logger.info("=== Construyendo pipeline ===")
last_step = main.build_pipeline(spark)
logger.info("=== Ejecutando flujo completo ===")
last_step.execute()
logger.info("=== Flujo completado ===")

# --------------------------------------------------------------------------------------
# 6) Inspección rápida de salidas
# --------------------------------------------------------------------------------------
for key in ("numcliente_features", "numcliente_features_vector"):
    out = cvas.output[key]["table_or_hdfs"]
    df = spark.read.parquet(str(out))
    logger.info("OUTPUT %s (%d filas): %s", key, df.count(), out)
    df.printSchema()
    df.show(10, truncate=30)

spark.stop()
