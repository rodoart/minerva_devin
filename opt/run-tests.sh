#!/bin/sh

##############################################
# RUN BASH TERMINAL
##############################################
if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi


##############################################
# LOOKING FOR ENVIRONMENT VARIABLES DO NOT MODIFY THIS SECTION
##############################################

# SEARCHING FO ENVIRONMENT VARIABLES

CURRENT_SCRIPT_DIR_LINUX=$(dirname "$(realpath "$0")")
TESTING_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/../..")
APP_WORKSPACE_DIR_LINUX=$(realpath "$CURRENT_SCRIPT_DIR_LINUX/..")
export APP_UNIX_ENVIRONMENT_VARS="${APP_WORKSPACE_DIR_LINUX}/opt/environment_vars.sh"

source "${APP_UNIX_ENVIRONMENT_VARS}"

##############################################
# VIRTUAL ENVIRONMENT
##############################################

# Modo local (unix/wsl): el python del venv ya es el intérprete — no hay
# conda ni empaquetado para HDFS. Modo cluster: conda activate + subir el
# venv empaquetado y los jars a HDFS si faltan.
if [ "${APP_MODE:-unix}" = "cluster" ] || [ "${APP_MODE:-}" = "hdfs" ]; then
    # Conda setup
    $CONDA_LINUX init bash
    eval "$($CONDA_LINUX shell.bash hook)"
    conda activate $VENV_NAME
    PYTHON_BIN="${PYSPARK_DRIVER_PYTHON:-python}"

    # -----------------------------------------------------------------------
    # Check if the virtual environment tar.gz file exists in HDFS
    # -----------------------------------------------------------------------
    if hdfs dfs -test -e "${PYSPARK_VENV_TAR_GZ_HDFS}"; then
        echo "Virtual environment already exists in HDFS: ${PYSPARK_VENV_TAR_GZ_HDFS}"
    else
        echo "Virtual environment does not exist in HDFS. Uploading: ${PYSPARK_VENV_TAR_GZ_HDFS}"
        rm -f "${VENV_TAR_GZ_LINUX}"
        conda-pack -n $VENV_NAME -o "${VENV_TAR_GZ_LINUX}"
        hdfs dfs -mkdir -p "${VENV_TAR_GZ_PARENT_DIR_HDFS}"
        hdfs dfs -put -f "${VENV_TAR_GZ_LINUX}" "${VENV_TAR_GZ_PARENT_DIR_HDFS}"
        hdfs dfs -chmod -R o+rx "${VENV_TAR_GZ_PARENT_DIR_HDFS}"
    fi


    if hdfs dfs -test -e "${PYSPARK_JARS_HDFS}"; then
        echo "Jar Files already exist in HDFS: ${PYSPARK_JARS_HDFS}"
    else
        echo "Jar Files do not exist in HDFS. Uploading: ${PYSPARK_JARS_HDFS}"
        hdfs dfs -mkdir -p "${EXTRA_JAR_PARENT_DIR_HDFS}"
        hdfs dfs -put -f "${EXTRA_JAR_LINUX}" "${EXTRA_JAR_PARENT_DIR_HDFS}"
        hdfs dfs -chmod -R o+rx "${EXTRA_JAR_PARENT_DIR_HDFS}"
    fi
else
    # Local: venv estándar (python -m venv); si no existe, avisar.
    PYTHON_BIN="${APP_VENV_PYTHON:-${PYSPARK_DRIVER_PYTHON:-python3}}"
    if [ ! -x "${PYTHON_BIN}" ]; then
        echo "No existe el intérprete '${PYTHON_BIN}'." >&2
        echo "Crea el venv con: bash opt/setup-local-env.sh   (o python3 -m venv .venv)" >&2
        exit 1
    fi
    echo "Usando intérprete local: ${PYTHON_BIN}"
fi

##############################################
# TEST PATHS
##############################################


# Check current date
CURRENT_DATE=$(date '+%Y-%m-%d_%H-%M-%S')


# LOG NAMES
LOG_ERR_LINUX="${LOGS_DIR_LINUX}/app_tests-err_${CURRENT_DATE}.log"
LOG_STD_LINUX="${LOGS_DIR_LINUX}/app_tests-std_${CURRENT_DATE}.log"



##############################################
# PYTEST
##############################################
cd $APP_WORKSPACE_DIR_LINUX
nohup "${PYTHON_BIN}" -m pytest -s --basetemp="${APP_TMP_DIR_LINUX}" tests/ -v -q > "${LOG_STD_LINUX}" 2> "${LOG_ERR_LINUX}" &
