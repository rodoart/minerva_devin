from pyspark.sql import SparkSession
from typing import Optional


def notebook(
    name: str,
    pyspark_queue: str,
    pyspark_port: int,
    jars: Optional[str] = None,
    archive: Optional[str] = None,
) -> SparkSession:
    """ Generates an appropriate context to run process at the cluster.
    ----
    Returns:
        SparkSession: spark session to run functions in it.
    """
    builder = (SparkSession.builder
        .appName(name)
        .config("spark.yarn.queue", f"root.{pyspark_queue}")
        .config("spark.ui.port", str(pyspark_port))
        .config("spark.master", "yarn")
        .config("spark.submit.deployMode", "client")
        .config("spark.driver.allowMultipleContexts", "True")
        .config("spark.sql.streaming.ui.enabled", "False")
        .config("spark.eventLog.enabled", "False")
        .config("spark.extraListeners", "")
        .config("spark.dynamicAllocation.maxExecutors", "200")
        .config("spark.default.parallelism", "120")
        .config("spark.sql.shuffle.partitions", "120")
        .config("spark.executor.instances", "40")
        .config("spark.executor.cores", "5")
        .config("spark.executor.memory", "16g")
        .config("spark.executor.memoryOverhead", "2000m")
        .config("spark.driver.memory", "12g")
        .config("spark.network.timeout", "7200s")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("hive.exec.dynamic.partition", "true")
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .config("spark.graphframes.connectedComponents.algorithm", "graphframes")
        .config("spark.graphframes.connectedComponents.checkpointInterval", 3)
    )

    if jars is not None:
        builder = builder.config("spark.jars", str(jars))

    if archive is not None:
        # Distribuye el env como archive con alias 'env'
        archive_with_alias = f"{archive}#env"
        # Ruta donde Spark extrae el archive en el working dir del executor
        env_site_packages = "./env/lib/python3.9/site-packages"
        #
        builder = (builder
            .config("spark.yarn.dist.archives", archive_with_alias)
            #  No sobrescribir PYSPARK_PYTHON (mantenemos el del cluster)
            #  Solo añadir el site-packages del env al PYTHONPATH
            .config("spark.executorEnv.PYTHONPATH", env_site_packages)
            .config("spark.yarn.appMasterEnv.PYTHONPATH", env_site_packages)
        )

    return (builder
        .enableHiveSupport()
        .getOrCreate()
    )


def get_remote(name:str) -> SparkSession:
    spark = SparkSession.builder \
        .appName(name) \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
