#!/bin/sh

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# =============================================================================
# environment_vars.sh — SELECTOR de entorno por plataforma.
#
# No define variables: delega en el fichero específico del modo:
#
#   APP_MODE=cluster|hdfs  -> environment_vars.cluster.sh (YARN + HDFS)
#   APP_MODE=wsl           -> environment_vars.wsl.sh     (unix + overrides)
#   APP_MODE=unix          -> environment_vars.unix.sh    (local POSIX)
#   APP_MODE=windows       -> (no aplica — Windows no ejecuta bash; el modo
#                             windows no necesita env file, los defaults de
#                             código/conftest bastan)
#
# Sin APP_MODE: autodetección — si el kernel reporta "microsoft"/"wsl" se
# usa WSL; si no, **unix (default)**. El cluster NUNCA es el default: hay
# que pedirlo explícitamente con APP_MODE=cluster|hdfs.
#
# Uso:
#   source opt/environment_vars.sh            # wsl o unix (autodetect)
#   APP_MODE=cluster source opt/environment_vars.sh
# =============================================================================

_CURRENT_SCRIPT_DIR=$(dirname "$(realpath "${BASH_SOURCE[0]}")")

_detect_mode() {
    if [ -n "${APP_MODE}" ]; then
        echo "${APP_MODE}"
        return
    fi
    # WSL: el release del kernel contiene "microsoft" o "WSL".
    if uname -r 2>/dev/null | grep -qi "microsoft\|wsl"; then
        echo "wsl"
    else
        echo "unix"
    fi
}

case "$(_detect_mode | tr '[:upper:]' '[:lower:]')" in
    cluster|hdfs)
        source "${_CURRENT_SCRIPT_DIR}/environment_vars.cluster.sh"
        ;;
    wsl)
        source "${_CURRENT_SCRIPT_DIR}/environment_vars.wsl.sh"
        ;;
    unix|local)
        source "${_CURRENT_SCRIPT_DIR}/environment_vars.unix.sh"
        ;;
    *)
        echo "environment_vars.sh: APP_MODE='${APP_MODE}' desconocido; usando unix" >&2
        source "${_CURRENT_SCRIPT_DIR}/environment_vars.unix.sh"
        ;;
esac
