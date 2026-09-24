#!/bin/sh

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# Parameters
export MINERVA_HIVE_DATABASE="gcprcmsbx_work"

# Paths
CURRENT_SCRIPT_DIR_LINUX=$(dirname "$(realpath "$0")")
export TESTING_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/../..")
export LOGS_DIR_LINUX="${TESTING_DIR_LINUX}/logs"
export MINERVA_TMP_DIR_LINUX=$(realpath "$TESTING_DIR_LINUX/tmp/tests")

export MINERVA_WORKSPACE_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/..")

# PYSPARK

# Parameters
export PYSPARK_PORT=12880
export PYSPARK_QUEUE="datalabs2"

# Paths
export JAVA_HOME="/usr/java/default"
export SPARK_HOME="/opt/cloudera/parcels/SPARK3-3.3.2.3.3.7191000.12-1-1.p0.70675659/lib/spark3"
export PYLIB="/opt/cloudera/parcels/SPARK3-3.3.2.3.3.7191000.12-1-1.p0.70675659/lib/spark3/python/lib"
export HADOOP_CONF_DIR="/opt/cloudera/parcels/CDH-7.1.9-1.cdh7.1.9.p1069.74330271/lib/hadoop/etc/hadoop"
export HIVE_CONF_DIR="/opt/cloudera/parcels/CDH-7.1.9-1.cdh7.1.9.p1069.74330271/lib/hive/conf"
export PYSPARK_PYTHON="/opt/cloudera/parcels/clt/conda38-9.9.65/bin/python"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_PYTHON}"

export PYSPARK_TMP_DIR_HDFS="/data/gcprcmsbx/work/hive/gcprcmsbx_work/rg49392/tmp"
export PYSPARK_STAGING_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/spark_staging"
export PYSPARK_LOGS_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/spark_logs"
export PYSPARK_WAREHOUSE_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/warehouse"
export PYSPARK_CHECKPOINTS_DIR_HDFS="${PYSPARK_TMP_DIR_HDFS}/checkpoints"

export MINERVA_WORKSPACE_DIR_HDFS="/data/gcprcmsbx/work/hive/gcprcmsbx_work/rg49392/minerva"
export MINERVA_TMP_TESTS_DIR_HDFS="${MINERVA_WORKSPACE_DIR_HDFS}/tmp/tests"
export MINERVA_CHECKPOINT_BASE_DIR_HDFS="${MINERVA_WORKSPACE_DIR_HDFS}/tmp/checkpoints"
export MINERVA_STAGING_BASE_DIR_HDFS="${MINERVA_WORKSPACE_DIR_HDFS}/tmp/staging"
export MINERVA_WAREHOUSE_DIR_HDFS="/data/gcprcmsbx/work/hive"

# VIRTUAL ENVIRONMENT

# Parameters
export MINERVA_NAME="minerva"
export VENV_NAME="minerva"
export MINERVA_TODAY="2025-08-17"

# Paths
export MINIFORGE_DIR_LINUX="/data/1/gcgaacqmxpysp/rg49392/miniforge"
export CONDA_LINUX="${MINIFORGE_DIR_LINUX}/bin/conda"

export MINERVA_VENV_TAR_GZ_PARENT_DIR_LINUX="${MINERVA_WORKSPACE_DIR_LINUX}/tmp"
export MINERVA_VENV_TAR_GZ_PARENT_DIR_HDFS="/tmp/spark/env"

export MINERVA_VENV_TAR_GZ_LINUX="${MINERVA_VENV_TAR_GZ_PARENT_DIR_LINUX}/${VENV_NAME}.tar.gz"
export MINERVA_VENV_TAR_GZ_HDFS="${MINERVA_VENV_TAR_GZ_PARENT_DIR_HDFS}/${VENV_NAME}.tar.gz"

export MINERVA_GRAPHFRAMES_JAR_LINUX="${MINERVA_WORKSPACE_DIR_LINUX}/jars/graphframes-0.8.1-spark3.0-s_2.12.jar"
export MINERVA_GRAPHFRAMES_JAR_PARENT_DIR_HDFS="/tmp/spark/jars"
export MINERVA_GRAPHFRAMES_JAR_HDFS="${MINERVA_GRAPHFRAMES_JAR_PARENT_DIR_HDFS}/graphframes-0.8.1-spark3.0-s_2.12.jar"

# Parameters

PATH_VARS=(
    CURRENT_SCRIPT_DIR_LINUX
    TESTING_DIR_LINUX
    LOGS_DIR_LINUX
    MINERVA_TMP_DIR_LINUX
    MINERVA_WORKSPACE_DIR_LINUX
    MINIFORGE_DIR_LINUX
    MINERVA_VENV_TAR_GZ_PARENT_DIR_LINUX
)

for var in "${PATH_VARS[@]}"; do
    echo "$var: ${!var}"
    mkdir -p "${!var}"
done

INFO_VARS=(
    MINERVA_TODAY
    MINERVA_NAME
    VENV_NAME
    MINERVA_VENV_TAR_GZ_PARENT_DIR_HDFS
    CONDA_LINUX
    MINERVA_NAME
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
    MINERVA_WORKSPACE_DIR_HDFS
    MINERVA_TMP_TESTS_DIR_HDFS
    MINERVA_STAGING_BASE_DIR_HDFS
    MINERVA_WAREHOUSE_DIR_HDFS
    MINERVA_GRAPHFRAMES_JAR_PARENT_DIR_HDFS
)

for var in "${HIVE_DIR_VARS[@]}"; do
    echo "$var: ${!var}"
    # hdfs dfs -mkdir -p "${!var}"
done
