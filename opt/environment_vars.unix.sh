#!/bin/sh

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# =============================================================================
# environment_vars.unix.sh — variables de entorno para modo "unix".
#
# Es el modo POR DEFECTO: desarrollo/ejecución local en cualquier sistema
# POSIX (Linux, macOS) — sin cluster, sin HDFS. Spark corre en local[*] y
# los paths usan el backend local del framework (path.local / LocalPath).
#
# WSL: este fichero sirve de base a environment_vars.wsl.sh (que solo
# añade los overrides específicos de WSL).
#
# Convención de nombres:
#   APP_*      parámetros/propios de la aplicación
#   PYSPARK_*  configuración de la sesión Spark (los lee SparkSessionBuilder)
#   VENV_*     virtualenv local (python -m venv, no conda-pack)
# =============================================================================

# Parameters
export APP_HIVE_DATABASE="your_hive_database"
export APP_MODE="${APP_MODE:-unix}"
export APP_OS_MODE="${APP_OS_MODE:-unix}"
export APP_FS_MODE="${APP_FS_MODE:-unix}"
export APP_TEST_MODE="${APP_TEST_MODE:-unix}"

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
export PYSPARK_PORT="${PYSPARK_PORT:-4040}"
export PYSPARK_QUEUE="default"

# Paths — sesión local: JAVA_HOME del sistema o el JRE local del workspace.
# En WSL/Linux sin Java del sistema, ver opt/setup-local-env.sh (descarga un
# JRE en ${APP_WORKSPACE_DIR_LINUX}/.local/jre).
if [ -d "${APP_WORKSPACE_DIR_LINUX}/.local/jre" ]; then
    export JAVA_HOME="${APP_WORKSPACE_DIR_LINUX}/.local/jre"
    export PATH="${JAVA_HOME}/bin:${PATH}"
fi
# PYLIB dummy: el __init__ del toolbox lo usa para localizar los zips de
# pyspark del cluster; en local el paquete viene del venv (pip).
export PYLIB="${PYLIB:-/dev/null}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-${APP_VENV_PYTHON:-python3}}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_PYTHON}"

# VIRTUAL ENVIRONMENT (local — venv estándar)

# Parameters
export APP_NAME="spark-app"
export VENV_NAME="spark-app"
export APP_TODAY="$(date '+%Y-%m-%d')"

# Paths — venv creado con `python3 -m venv` (ver opt/setup-local-env.sh).
export VENV_DIR_LINUX="${VENV_DIR_LINUX:-${APP_WORKSPACE_DIR_LINUX}/.venv}"
export APP_VENV_PYTHON="${APP_VENV_PYTHON:-${VENV_DIR_LINUX}/bin/python}"

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
    VENV_DIR_LINUX
)

for var in "${PATH_VARS[@]}"; do
    echo "$var: ${!var}"
    mkdir -p "${!var}" 2>/dev/null || true
done

INFO_VARS=(
    APP_MODE
    APP_OS_MODE
    APP_FS_MODE
    APP_TEST_MODE
    APP_TODAY
    APP_NAME
    VENV_NAME
    APP_VENV_PYTHON
)

for var in "${INFO_VARS[@]}"; do
    echo "$var: ${!var}"
done
