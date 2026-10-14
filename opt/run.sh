#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run.sh
#
# Lanza el pipeline SIN supervisor: carga las variables de entorno del
# cluster (opt/environment_vars.sh) y ejecuta APP_MAIN_SCRIPT directamente.
#
# Si el proceso muere o se queda zombie no se reinicia — para ejecución
# vigilada usa opt/run-supervised.sh.
#
# Env vars relevantes (definidas en environment_vars.sh):
#   APP_MAIN_SCRIPT    script del pipeline (default examples/main.py)
#   APP_LOG_LEVEL      nivel de logging (default INFO)
#   APP_LOG_FILE       .log concreto / "off" para solo consola
#   LOGS_DIR_LINUX     dir de los .log (default <workspace>/logs)
#
# Uso:
#   ./opt/run.sh                # pipeline completo
#   ./opt/run.sh --local        # flags extra -> APP_MAIN_SCRIPT
#   nohup ./opt/run.sh > /dev/null 2>&1 &
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "${SCRIPT_DIR}")"

source "${SCRIPT_DIR}/environment_vars.sh"

cd "${WORKSPACE}"
exec ${PYSPARK_DRIVER_PYTHON:-python} "${APP_MAIN_SCRIPT}" "$@"
