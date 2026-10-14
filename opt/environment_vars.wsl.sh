#!/bin/sh

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# =============================================================================
# environment_vars.wsl.sh — modo "wsl": Windows Subsystem for Linux.
#
# Hereda TODA la configuración de environment_vars.unix.sh (WSL es un UNIX
# completo — nada de lo genérico se repite) y solo añade los overrides
# específicos del entorno WSL:
#
#   * el workspace suele vivir en el filesystem de Windows (/mnt/<drive>):
#     el IO de Spark sobre /mnt es lento, así que logs/tmp van al FS nativo
#     de WSL (~/...) salvo que se configure lo contrario;
#   * JAVA_HOME/venv en rutas nativas del FS de WSL.
#
# Uso:
#   source opt/environment_vars.sh            # autodetecta WSL -> este file
#   APP_MODE=wsl source opt/environment_vars.sh
# =============================================================================

CURRENT_SCRIPT_DIR_LINUX=$(dirname "$(realpath "${BASH_SOURCE[0]}")")
source "${CURRENT_SCRIPT_DIR_LINUX}/environment_vars.unix.sh"

export APP_MODE="wsl"
export APP_OS_MODE="wsl"
export APP_FS_MODE="wsl"
export APP_TEST_MODE="wsl"

# Logs/tmp/tests en el FS nativo de WSL: el workspace puede estar montado
# desde Windows (/mnt/c, /mnt/d) y el IO de Spark/pytest sobre /mnt es
# mucho más lento. Configurables para apuntarlos de vuelta al workspace.
export LOGS_DIR_LINUX="${LOGS_DIR_WSL:-${HOME}/laika_framework/logs}"
export APP_TMP_DIR_LINUX="${APP_TMP_DIR_WSL:-${HOME}/laika_framework/tmp/tests}"

# JRE y venv también nativos (los crea opt/setup-local-env.sh en el HOME).
export VENV_DIR_LINUX="${VENV_DIR_WSL:-${HOME}/venvs/laika_framework}"
export APP_VENV_PYTHON="${VENV_DIR_LINUX}/bin/python"
if [ -d "${HOME}/opt/jre" ]; then
    export JAVA_HOME="${HOME}/opt/jre"
    export PATH="${JAVA_HOME}/bin:${PATH}"
fi
export PYSPARK_PYTHON="${APP_VENV_PYTHON}"
export PYSPARK_DRIVER_PYTHON="${APP_VENV_PYTHON}"

mkdir -p "${LOGS_DIR_LINUX}" "${APP_TMP_DIR_LINUX}" "${VENV_DIR_LINUX}"

echo "APP_MODE: ${APP_MODE}"
echo "LOGS_DIR_LINUX: ${LOGS_DIR_LINUX}"
echo "APP_TMP_DIR_LINUX: ${APP_TMP_DIR_LINUX}"
echo "VENV_DIR_LINUX: ${VENV_DIR_LINUX}"
echo "JAVA_HOME: ${JAVA_HOME:-<sin JRE local — corre opt/setup-local-env.sh>}"
