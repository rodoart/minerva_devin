#!/usr/bin/env bash
# =============================================================================
# cleanup.sh — borra los paths temporales/staging declarados en
# environment_vars.cluster.sh una vez que el datapull terminó con éxito.
#
# Lo dispara el supervisor cuando el pipeline sale con rc=0
# (SUPERVISOR_CLEANUP_SCRIPT). Lee las vars del entorno heredado — NO
# hace source de environment_vars.cluster.sh (eso haría mkdir -p de
# los dirs que justo se quieren borrar).
#
# Qué borra:
#   HDFS : PYSPARK_TMP_DIR_HDFS (cubre spark_staging/spark_logs/
#          warehouse/checkpoints de la sesión), APP_TMP_TESTS_DIR_HDFS,
#          APP_CHECKPOINT_BASE_DIR_HDFS, APP_STAGING_BASE_DIR_HDFS,
#          PYSPARK_VENV_TAR_GZ_HDFS, EXTRA_JAR_PARENT_DIR_HDFS
#          (venv tarball y jars se re-suben solos en la siguiente corrida)
#   local: APP_TMP_DIR_LINUX, VENV_TAR_GZ_LINUX
#
# Qué NO borra (a propósito):
#   - APP_WAREHOUSE_DIR_HDFS (/user/<u>/hive): warehouse Hive con tablas
#   - LOGS_DIR_LINUX: logs de la corrida (útiles post-mortem)
#   - los padres compartidos (APP_WORKSPACE_DIR_HDFS, TESTING_DIR_LINUX)
#
# AL FINAL SE AUTO-BORRA (rm -f "$0") — si se vuelve a necesitar,
# recuperarlo del repo.
# =============================================================================
set -u

removed=0

_hdfs_rm() {
    local p="${1:-}"
    [ -z "$p" ] && return 0
    if hdfs dfs -test -e "$p" 2>/dev/null; then
        echo "cleanup: hdfs dfs -rm -r -f -skipTrash $p"
        hdfs dfs -rm -r -f -skipTrash "$p" && removed=$((removed + 1))
    else
        echo "cleanup: (no existe) $p"
    fi
}

_local_rm() {
    local p="${1:-}"
    [ -z "$p" ] && return 0
    if [ -e "$p" ]; then
        echo "cleanup: rm -rf $p"
        rm -rf "$p" && removed=$((removed + 1))
    else
        echo "cleanup: (no existe) $p"
    fi
}

# --- HDFS: dirs tmp/staging/checkpoint de la app ---
for p in \
    "${PYSPARK_TMP_DIR_HDFS:-}" \
    "${APP_TMP_TESTS_DIR_HDFS:-}" \
    "${APP_CHECKPOINT_BASE_DIR_HDFS:-}" \
    "${APP_STAGING_BASE_DIR_HDFS:-}" \
    "${PYSPARK_VENV_TAR_GZ_HDFS:-}" \
    "${EXTRA_JAR_PARENT_DIR_HDFS:-}" \
; do
    _hdfs_rm "$p"
done

# --- Local: dirs/archivos tmp ---
for p in \
    "${APP_TMP_DIR_LINUX:-}" \
    "${VENV_TAR_GZ_LINUX:-}" \
; do
    _local_rm "$p"
done

echo "cleanup: ${removed} paths eliminados; auto-borrando script"
rm -f -- "$0"
