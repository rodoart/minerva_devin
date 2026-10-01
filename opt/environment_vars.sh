#!/bin/sh

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# =============================================================================
# environment_vars.sh — plantilla de variables de entorno del framework.
#
# Ajusta los valores a tu cluster. Convención de nombres:
#   APP_*      parámetros/propios de la aplicación
#   PYSPARK_*  configuración de la sesión Spark (los lee SparkSessionBuilder)
#   VENV_*     virtualenv empaquetado con conda-pack
# =============================================================================

# Parameters
export APP_HIVE_DATABASE="your_hive_database"

# Paths
CURRENT_SCRIPT_DIR_LINUX=$(dirname "$(realpath "$0")")
export TESTING_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/../..")
export LOGS_DIR_LINUX="${TESTING_DIR_LINUX}/logs"
export APP_TMP_DIR_LINUX=$(realpath "$TESTING_DIR_LINUX/tmp/tests")

export APP_WORKSPACE_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/..")

# PYSPARK

# Parameters
export PYSPARK_PORT=4040
export PYSPARK_QUEUE="default"

# Paths (ajusta a las rutas de tu distribución Hadoop/Spark)
export JAVA_HOME="/usr/java/default"
export SPARK_HOME="/opt/cloudera/parcels/SPARK3/lib/spark3"
export PYLIB="${SPARK_HOME}/python/lib"
export HADOOP_CONF_DIR="/etc/hadoop/conf"
export HIVE_CONF_DIR="/etc/hive/conf"
export PYSPARK_PYTHON="python"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_PYTHON}"

export PYSPARK_TMP_DIR_HDFS="/user/${USER}/spark_app/tmp"
export PYSPARK_STAGING_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/spark_staging"
export PYSPARK_LOGS_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/spark_logs"
export PYSPARK_WAREHOUSE_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/warehouse"
export PYSPARK_CHECKPOINTS_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/checkpoints"

export APP_WORKSPACE_DIR_HDFS="/user/${USER}/spark_app"
export APP_TMP_TESTS_DIR_HDFS="${APP_WORKSPACE_DIR_HDFS}/tmp/tests"
export APP_CHECKPOINT_BASE_DIR_HDFS="${APP_WORKSPACE_DIR_HDFS}/tmp/checkpoints"
export APP_STAGING_BASE_DIR_HDFS="${APP_WORKSPACE_DIR_HDFS}/tmp/staging"
export APP_WAREHOUSE_DIR_HDFS="/user/${USER}/hive"

# VIRTUAL ENVIRONMENT

# Parameters
export APP_NAME="spark-app"
export VENV_NAME="spark-app"
export APP_TODAY="$(date '+%Y-%m-%d')"

# Paths
export MINIFORGE_DIR_LINUX="${HOME}/miniforge"
export CONDA_LINUX="${MINIFORGE_DIR_LINUX}/bin/conda"

export VENV_TAR_GZ_PARENT_DIR_LINUX="${APP_WORKSPACE_DIR_LINUX}/tmp"
export VENV_TAR_GZ_PARENT_DIR_HDFS="/tmp/spark/env"

export VENV_TAR_GZ_LINUX="${VENV_TAR_GZ_PARENT_DIR_LINUX}/${VENV_NAME}.tar.gz"
export PYSPARK_VENV_TAR_GZ_HDFS="${VENV_TAR_GZ_PARENT_DIR_HDFS}/${VENV_NAME}.tar.gz"

# Jars extra para la sesión (p.ej. graphframes). Lista separada por comas en HDFS.
export EXTRA_JAR_LINUX="${APP_WORKSPACE_DIR_LINUX}/jars/graphframes-0.8.1-spark3.0-s_2.12.jar"
export EXTRA_JAR_PARENT_DIR_HDFS="/tmp/spark/jars"
export PYSPARK_JARS_HDFS="${EXTRA_JAR_PARENT_DIR_HDFS}/graphframes-0.8.1-spark3.0-s_2.12.jar"

# Parameters

PATH_VARS=(
    CURRENT_SCRIPT_DIR_LINUX
    TESTING_DIR_LINUX
    LOGS_DIR_LINUX
    APP_TMP_DIR_LINUX
    APP_WORKSPACE_DIR_LINUX
    MINIFORGE_DIR_LINUX
    VENV_TAR_GZ_PARENT_DIR_LINUX
)

for var in "${PATH_VARS[@]}"; do
    echo "$var: ${!var}"
    mkdir -p "${!var}"
done

INFO_VARS=(
    APP_TODAY
    APP_NAME
    VENV_NAME
    VENV_TAR_GZ_PARENT_DIR_HDFS
    CONDA_LINUX
)

for var in "${INFO_VARS[@]}"; do
    echo "$var: ${!var}"
done

HIVE_DIR_VARS=(
    PYSPARK_TMP_DIR_HDFS
    PYSPARK_STAGING_DIR_HDFS
    PYSPARK_LOGS_DIR_HDFS
    PYSPARK_WAREHOUSE_DIR_HDFS
    PYSPARK_CHECKPOINTS_DIR_HDFS
    APP_WORKSPACE_DIR_HDFS
    APP_TMP_TESTS_DIR_HDFS
    APP_STAGING_BASE_DIR_HDFS
    APP_WAREHOUSE_DIR_HDFS
    EXTRA_JAR_PARENT_DIR_HDFS
)

for var in "${HIVE_DIR_VARS[@]}"; do
    echo "$var: ${!var}"
    # hdfs dfs -mkdir -p "${!var}"
done
