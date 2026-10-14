#!/bin/sh

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# =============================================================================
# environment_vars.cluster.sh — variables de entorno para modo "cluster".
#
# Seleccionable con `APP_MODE=cluster` (o `hdfs`) al hacer source de
# environment_vars.sh. Solo para ejecución en el clúster (YARN + HDFS);
# para desarrollo local ver environment_vars.unix.sh / .wsl.sh.
#
# Ajusta los valores a tu cluster. Convención de nombres:
#   APP_*      parámetros/propios de la aplicación
#   PYSPARK_*  configuración de la sesión Spark (los lee SparkSessionBuilder)
#   VENV_*     virtualenv empaquetado con conda-pack
# =============================================================================

# Parameters
export APP_HIVE_DATABASE="your_hive_database"

# Paths
CURRENT_SCRIPT_DIR_LINUX=$(dirname "$(realpath "${BASH_SOURCE[0]}")")
export TESTING_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/../..")
export LOGS_DIR_LINUX="${TESTING_DIR_LINUX}/logs"
export APP_TMP_DIR_LINUX="${TESTING_DIR_LINUX}/tmp/tests"
mkdir -p "${APP_TMP_DIR_LINUX}"
export APP_TMP_DIR_LINUX=$(realpath "${APP_TMP_DIR_LINUX}")

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

# =============================================================================
# RUN — entry point, supervisor y logging
# =============================================================================

# Script del pipeline que ejecutan opt/run.sh (sin supervisor) y
# opt/run-supervised.sh (a través de SUPERVISOR_COMMAND).
export APP_MAIN_SCRIPT="${APP_MAIN_SCRIPT:-${APP_WORKSPACE_DIR_LINUX}/examples/main.py}"

# Comando que vigila supervisor.py (default: el pipeline en sí).
export SUPERVISOR_COMMAND="${SUPERVISOR_COMMAND:-${PYSPARK_DRIVER_PYTHON} ${APP_MAIN_SCRIPT}}"

# Logging: nivel + archivo. Los .log quedan en LOGS_DIR_LINUX con nombre
# ${APP_NAME}_<timestamp>.log (resuelve la suite de logging del framework).
# APP_LOG_FILE=<ruta> fija un archivo concreto; APP_LOG_FILE=off lo desactiva.
export APP_LOG_LEVEL="${APP_LOG_LEVEL:-INFO}"

# Timeouts del supervisor (ver docstring de supervisor.py)
export SUPERVISOR_STEP_TIMEOUT="${SUPERVISOR_STEP_TIMEOUT:-7200}"
export SUPERVISOR_STALL_TIMEOUT="${SUPERVISOR_STALL_TIMEOUT:-600}"
export SUPERVISOR_WATCHDOG_INTERVAL="${SUPERVISOR_WATCHDOG_INTERVAL:-30}"
export SUPERVISOR_MAX_RESTARTS="${SUPERVISOR_MAX_RESTARTS:-10}"

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
