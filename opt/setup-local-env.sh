#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# setup-local-env.sh — crea el entorno de desarrollo local (unix / wsl).
#
# Qué hace (todo sin sudo, en el HOME del usuario):
#   1. Resuelve un Python 3.10 (el pipeline usa pyspark 3.3.2 — los Python
#      3.12+ de las distros nuevas NO le sirven porque pyspark importa
#      `distutils`, eliminado en 3.12):
#        - si `uv` está en PATH -> `uv python install 3.10` (standalone)
#        - si no, lo instala en ~/.local/bin con el script oficial de uv
#   2. Crea el venv (`uv venv`, o `python3 -m venv` si PYTHON_BIN ya es 3.10):
#        wsl  -> ${VENV_DIR_WSL:-~/venvs/laika_framework}
#        unix -> ${VENV_DIR_LINUX:-<workspace>/.venv}
#   3. Instala con pip/uv las dependencias de tests/pipeline
#      (pyspark 3.3.2, pytest, pandas<2, pyarrow<11, numpy<2, ...).
#   4. Si no hay `java` en PATH, descarga un JRE Temurin 17 a ~/opt/jre
#      (Spark lo necesita; no requiere sudo).
#   5. Verifica la instalación con un smoke test.
#
# Uso:
#   bash opt/setup-local-env.sh           # dentro de WSL o Linux
#   APP_MODE=unix bash opt/setup-local-env.sh
#
# Tras el setup, las pruebas corren con:
#   source opt/environment_vars.sh && bash opt/run-tests.sh
# o directamente:
#   <venv>/bin/python -m pytest tests/ -v
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE="$(dirname "${SCRIPT_DIR}")"

# --- Detección de modo (mismo criterio que environment_vars.sh) --------------
MODE="${APP_MODE:-auto}"
if [ "${MODE}" = "auto" ]; then
    if uname -r 2>/dev/null | grep -qi "microsoft\|wsl"; then
        MODE="wsl"
    else
        MODE="unix"
    fi
fi
echo "==> Modo detectado: ${MODE}"

if [ "${MODE}" = "wsl" ]; then
    VENV_DIR="${VENV_DIR_WSL:-${HOME}/venvs/laika_framework}"
    JRE_DIR="${HOME}/opt/jre"
else
    VENV_DIR="${VENV_DIR_LINUX:-${WORKSPACE}/.venv}"
    JRE_DIR="${WORKSPACE}/.local/jre"
fi

PYTHON_VERSION="${PYTHON_VERSION:-3.10}"

# --- 1. Python 3.10 (via uv si hace falta) ------------------------------------
PYTHON_BIN="${PYTHON_BIN:-}"
if [ -n "${PYTHON_BIN}" ]; then
    echo "==> Usando PYTHON_BIN=${PYTHON_BIN} ($(${PYTHON_BIN} --version 2>&1))"
elif command -v uv >/dev/null 2>&1; then
    echo "==> uv encontrado; asegurando CPython ${PYTHON_VERSION}"
    uv python install "${PYTHON_VERSION}"
elif "$(command -v python3 || true)" -c "import sys; raise SystemExit(sys.version_info[:2] != (3,10))" 2>/dev/null; then
    PYTHON_BIN="$(command -v python3)"
    echo "==> python3 del sistema ya es 3.10 (${PYTHON_BIN})"
else
    echo "==> Instalando uv (Python del sistema no sirve para pyspark 3.3.2)"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="${HOME}/.local/bin:${PATH}"
    uv python install "${PYTHON_VERSION}"
fi

# --- 2. venv ------------------------------------------------------------------
if [ ! -f "${VENV_DIR}/bin/python" ]; then
    echo "==> Creando venv en ${VENV_DIR}"
    if command -v uv >/dev/null 2>&1 && [ -z "${PYTHON_BIN}" ]; then
        uv venv "${VENV_DIR}" --python "${PYTHON_VERSION}"
    else
        "${PYTHON_BIN:-python3}" -m venv "${VENV_DIR}" 2>/dev/null \
            || { "${PYTHON_BIN:-python3}" -m pip install --user virtualenv
                 "${PYTHON_BIN:-python3}" -m virtualenv "${VENV_DIR}"; }
    fi
else
    echo "==> venv ya existe en ${VENV_DIR}"
fi
VENV_PY="${VENV_DIR}/bin/python"

# --- 3. Dependencias ----------------------------------------------------------
# Mínimas para tests + pipeline local. graphframes/inflection son opcionales
# (los tests que las necesitan se saltan si no están).
# numpy<2: pandas<2 está compilado contra la ABI de numpy 1.x.
echo "==> Instalando dependencias"
PIP_PKGS=(
    "pyspark==3.3.2"
    "pytest==8.3.*"
    "pandas>=1.5.3,<2.0"
    "numpy<2"
    "pyarrow>=10.0.1,<11.0"
    "python-dateutil"
    "regex"
    "cytoolz"
    "unidecode"
    "inflection"
)
if [ "${INSTALL_OPTIONAL:-1}" = "1" ]; then
    PIP_PKGS+=("graphframes")
fi
if command -v uv >/dev/null 2>&1; then
    uv pip install --python "${VENV_PY}" "${PIP_PKGS[@]}"
else
    "${VENV_PY}" -m pip install --upgrade pip
    "${VENV_PY}" -m pip install "${PIP_PKGS[@]}"
fi

# --- 4. Java (Spark lo necesita) ----------------------------------------------
if command -v java >/dev/null 2>&1; then
    echo "==> Java del sistema encontrado: $(java -version 2>&1 | head -1)"
elif [ -x "${JRE_DIR}/bin/java" ]; then
    echo "==> JRE local ya instalado en ${JRE_DIR}"
else
    echo "==> Sin Java en PATH; descargando JRE Temurin 17 a ${JRE_DIR}"
    echo "   (alternativa con sudo: apt install openjdk-17-jre-headless)"
    mkdir -p "$(dirname "${JRE_DIR}")"
    TMP_JRE="$(mktemp -d)"
    JRE_URL="https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jre/hotspot/normal/eclipse"
    if curl -fsSL "${JRE_URL}" -o "${TMP_JRE}/jre.tar.gz"; then
        tar -xzf "${TMP_JRE}/jre.tar.gz" -C "${TMP_JRE}"
        mv "${TMP_JRE}"/jdk-*/ "${JRE_DIR}" 2>/dev/null \
            || mv "$(ls -d "${TMP_JRE}"/*/ | head -1)" "${JRE_DIR}"
    else
        echo "   WARNING: no se pudo descargar el JRE. Instala Java a mano:" >&2
        echo "     sudo apt install default-jre    (con sudo)" >&2
    fi
    rm -rf "${TMP_JRE}"
fi

# --- 5. Verificación ----------------------------------------------------------
echo "==> Verificando instalación"
export JAVA_HOME="${JAVA_HOME:-${JRE_DIR}}"
"${VENV_PY}" - <<'PYEOF'
import sys
print("python:", sys.version.split()[0])
import pyspark
print("pyspark:", pyspark.__version__)
import pytest
print("pytest:", pytest.__version__)
PYEOF

echo ""
echo "Listo. Para correr los tests:"
echo "  source opt/environment_vars.sh   # autodetecta ${MODE}"
echo "  bash opt/run-tests.sh"
echo "o directamente:"
echo "  ${VENV_PY} -m pytest tests/ -v"
