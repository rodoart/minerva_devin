#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run-supervised.sh
#
# Wrapper para lanzar la sesión del pipeline bajo el supervisor autónomo
# (supervisor.py): carga las variables de entorno del cluster y delega.
#
# El supervisor reinicia el comando supervisado si muere o si un Step supera
# SUPERVISOR_STEP_TIMEOUT sin progreso en la Spark UI (zombie). La reanudación
# en el Step actual la da el propio dinamismo del framework (los parquets ya
# escritos se recargan en vez de recomputarse).
#
# Env vars del supervisor (ver docstring de supervisor.py):
#   SUPERVISOR_STEP_TIMEOUT, SUPERVISOR_STALL_TIMEOUT,
#   SUPERVISOR_WATCHDOG_INTERVAL, SUPERVISOR_MAX_RESTARTS, SUPERVISOR_COMMAND
#
# Uso:
#   ./opt/run-supervised.sh
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "${SCRIPT_DIR}")"

source "${SCRIPT_DIR}/environment_vars.sh"

cd "${WORKSPACE}"
exec python "${WORKSPACE}/supervisor.py" "$@"
