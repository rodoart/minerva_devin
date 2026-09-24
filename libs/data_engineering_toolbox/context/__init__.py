from pyspark.sql import SparkSession
from typing import Optional

from typing import Any, Dict, Union
from pathlib import Path
from copy import copy
import os
import time

from .logging import get_logger

logger = get_logger(__name__)


class SparkSessionBuilder:
    __RETRY_DELAY_SECONDS = 0.01
    __MAX_RETRIES = 5
    """Clase que representa SparkSessionBuilder."""

    def __init__(self, app_name: str = "minerva", extra_conf: Dict[str, Any] = None) -> None:
        """Inicializa una nueva instancia de SparkSessionBuilder."""
        self.app_name = app_name
        self.extra_conf = extra_conf or {}

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
        env_site_packages = "./env/lib/python3.9/site-packages"
        builder = (SparkSession.builder
            .appName(self.app_name)
            .config("spark.yarn.queue", f"root.{os.environ['PYSPARK_QUEUE']}")
            .config("spark.ui.port", str(os.environ['PYSPARK_PORT']))
            .config("spark.master", "yarn")
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
            # --- Redirigir fuera de /user/rg49392 ---
            .config("spark.yarn.stagingDir", self.hdfs_uri(os.environ["PYSPARK_STAGING_DIR_HDFS"]))
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", self.hdfs_uri(os.environ["PYSPARK_LOGS_DIR_HDFS"]))
            .config("spark.history.fs.logDirectory", self.hdfs_uri(os.environ["PYSPARK_STAGING_DIR_HDFS"]))
            # Checkpoints y warehouse tambien fuera del home
            .config("spark.sql.warehouse.dir", self.hdfs_uri(os.environ["PYSPARK_WAREHOUSE_DIR_HDFS"]))
            # Virtual environment as tar.gz
            .config("spark.yarn.dist.archives", self.hdfs_uri(os.environ["MINERVA_VENV_TAR_GZ_HDFS"]) + "#env")
            .config("spark.jars", self.hdfs_uri(os.environ["MINERVA_GRAPHFRAMES_JAR_HDFS"]))
            .config("spark.executorEnv.PYTHONPATH", env_site_packages)
            .config("spark.yarn.appMasterEnv.PYTHONPATH", env_site_packages)
            #
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        )
        for key, value in self.extra_conf.items():
            builder = builder.config(key, value)
        #
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
                spark.sparkContext.setCheckpointDir(
                    self.hdfs_uri(os.environ["PYSPARK_CHECKPOINTS_DIR_HDFS"]))
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
