#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run-supervised.sh
#
# Wrapper para lanzar la sesión Minerva bajo el supervisor autónomo
# (supervisor.py): carga las variables de entorno del cluster y delega.
#
# El supervisor reinicia `python main.py` si muere o si un Step supera
# MINERVA_STEP_TIMEOUT sin progreso en la Spark UI (zombie). La reanudación
# en el Step actual la da el propio dinamismo (los parquets ya escritos se
# recargan en vez de recomputarse).
#
# Env vars del supervisor (ver docstring de supervisor.py):
#   MINERVA_STEP_TIMEOUT, MINERVA_STALL_TIMEOUT, MINERVA_WATCHDOG_INTERVAL,
#   MINERVA_MAX_RESTARTS, MINERVA_COMMAND
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
