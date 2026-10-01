from pyspark.sql import SparkSession
from typing import Optional

from typing import Any, Dict, Union
from pathlib import Path
from copy import copy
import os
import sys
import time

from .logging import get_logger

logger = get_logger(__name__)


class SparkSessionBuilder:
    __RETRY_DELAY_SECONDS = 0.01
    __MAX_RETRIES = 5
    """Clase que representa SparkSessionBuilder.

    Construye una SparkSession YARN (o local con ``master="local[*]"``).
    Las rutas HDFS se leen de variables de entorno ``PYSPARK_*``:

        PYSPARK_QUEUE               cola YARN (sin el prefijo "root.")
        PYSPARK_PORT                puerto de la Spark UI del driver
        PYSPARK_STAGING_DIR_HDFS    staging dir de YARN
        PYSPARK_LOGS_DIR_HDFS       dir de event logs
        PYSPARK_WAREHOUSE_DIR_HDFS  warehouse de Hive
        PYSPARK_CHECKPOINTS_DIR_HDFS dir de checkpoints
        PYSPARK_VENV_TAR_GZ_HDFS    (opcional) tar.gz del venv para
                                    spark.yarn.dist.archives (se ancla "#env")
        PYSPARK_JARS_HDFS           (opcional) jars extra para spark.jars
                                    (lista separada por comas)

    El nombre de la aplicación puede pasarse como ``app_name`` o leerse de
    ``APP_NAME``.
    """

    def __init__(self, app_name: Optional[str] = None,
                 extra_conf: Dict[str, Any] = None,
                 master: str = "yarn",
                 venv_python_version: Optional[str] = None) -> None:
        """Inicializa una nueva instancia de SparkSessionBuilder."""
        self.app_name = app_name or os.environ.get("APP_NAME", "spark-app")
        self.extra_conf = extra_conf or {}
        self.master = master
        self.venv_python_version = (venv_python_version
            or f"{sys.version_info.major}.{sys.version_info.minor}")

    # Prefijo hdfs:// para que Spark NO use file:/
    @staticmethod
    def hdfs_uri(p: Union[str, Path]) -> str:
        if isinstance(p, Path):
            s = str(p)
        else:
            s = copy(p)
        #
        return s if s.startswith("hdfs://") else f"hdfs://{s}"
        #

    def linux_uri(self, p: Union[str, Path]) -> str:
        """Devuelve la ruta Linux para Spark (no HDFS)."""
        if isinstance(p, Path):
            s = str(p)
        else:
            s = copy(p)

        return s if s.startswith("/") else f"/{s}"
        #

    def build(self) -> SparkSession:
        """Método que construye."""
        if self.master.startswith("local"):
            builder = (SparkSession.builder
                .appName(self.app_name)
                .config("spark.master", self.master))
            for key, value in self.extra_conf.items():
                builder = builder.config(key, value)
            return self._get_or_create_with_retries(builder)
        #
        env_site_packages = f"./env/lib/python{self.venv_python_version}/site-packages"
        builder = (SparkSession.builder
            .appName(self.app_name)
            .config("spark.yarn.queue", f"root.{os.environ['PYSPARK_QUEUE']}")
            .config("spark.ui.port", str(os.environ['PYSPARK_PORT']))
            .config("spark.master", self.master)
            .config("spark.submit.deployMode", "client")
            .config("spark.driver.allowMultipleContexts", "True")
            # Custom
            .config("spark.dynamicAllocation.maxExecutors", "200")
            .config("spark.default.parallelism", "120")
            .config("spark.sql.shuffle.partitions", "120")
            .config("spark.executor.instances", "40")
            .config("spark.executor.cores", "5")
            .config("spark.executor.memory", "16g")
            .config("spark.executor.memoryOverhead", "2000m")
            .config("spark.driver.memory", "12g")
            .config("spark.network.timeout", "7200s")
            # --- Redirigir fuera del home de HDFS ---
            .config("spark.yarn.stagingDir", self.hdfs_uri(os.environ["PYSPARK_STAGING_DIR_HDFS"]))
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", self.hdfs_uri(os.environ["PYSPARK_LOGS_DIR_HDFS"]))
            .config("spark.history.fs.logDirectory", self.hdfs_uri(os.environ["PYSPARK_STAGING_DIR_HDFS"]))
            # Checkpoints y warehouse tambien fuera del home
            .config("spark.sql.warehouse.dir", self.hdfs_uri(os.environ["PYSPARK_WAREHOUSE_DIR_HDFS"]))
            #
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        )
        # Virtual environment empaquetado (opcional)
        venv_archive = os.environ.get("PYSPARK_VENV_TAR_GZ_HDFS")
        if venv_archive:
            builder = (builder
                .config("spark.yarn.dist.archives", self.hdfs_uri(venv_archive) + "#env")
                .config("spark.executorEnv.PYTHONPATH", env_site_packages)
                .config("spark.yarn.appMasterEnv.PYTHONPATH", env_site_packages))
        # Jars extra (opcional; lista separada por comas)
        jars = os.environ.get("PYSPARK_JARS_HDFS")
        if jars:
            builder = builder.config("spark.jars", ",".join(
                self.hdfs_uri(j) for j in jars.split(",") if j.strip()))
        #
        for key, value in self.extra_conf.items():
            builder = builder.config(key, value)
        #
        spark = self._get_or_create_with_retries(builder)
        #
        checkpoints_dir = os.environ.get("PYSPARK_CHECKPOINTS_DIR_HDFS")
        if checkpoints_dir:
            spark.sparkContext.setCheckpointDir(self.hdfs_uri(checkpoints_dir))
        #
        return spark

    def _get_or_create_with_retries(self, builder) -> SparkSession:
        # SESSION RETRIES
        last_exception = None
        #
        for attempt in range(1, self.__MAX_RETRIES + 1):
            try:
                logger.info(
                    "Creando SparkSession (intento %s/%s)",
                    attempt,
                    self.__MAX_RETRIES,
                )
                #
                spark = builder.getOrCreate()
                #
                logger.info("SparkSession creada correctamente")
                return spark
            #
            except Exception as e:
                last_exception = e
                #
                # error_text = str(e)
                #
                logger.exception(
                    "Falló la creación de SparkSession (intento %s/%s)",
                    attempt,
                    self.__MAX_RETRIES,
                )
                #
                if attempt < self.__MAX_RETRIES:
                    logger.info(
                        "Esperando %s segundos antes de reintentar",
                        self.__RETRY_DELAY_SECONDS,
                    )
                    time.sleep(self.__RETRY_DELAY_SECONDS)
                else:
                    logger.error(
                        "Se alcanzó el máximo de reintentos (%s)",
                        self.__MAX_RETRIES,
                    )
                    raise
        #
        raise last_exception
