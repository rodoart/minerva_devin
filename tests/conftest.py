"""Fixtures compartidas de la suite de tests del framework.

Modo de ejecución (variables `APP_TEST_MODE`/`APP_MODE`):
- `"unix"` (**default**), `"windows"`, `"wsl"`, `"local"` (alias legacy):
  SparkSession local[2], rutas temporales en `tmp_path` del filesystem local
  y `PYLIB` dummy. Corre sin cluster.
- `"cluster"`/`"hdfs"`: `SparkSessionBuilder()` (sesión YARN real) y
  `tmp_hdfs` sobre HDFS (`APP_TMP_TESTS_DIR_HDFS`). Requiere
  `opt/environment_vars.sh` (o `environment_vars.cluster.sh`).

Fixtures:
- `spark`: SparkSession (local[2] en modos locales, YARN en cluster).
- `graphframes`: verifica que GraphFrame se puede instanciar.
- `tmp_hdfs`: directorio temporal — `tmp_path` en local, HivePath en cluster.
- `checkpoint_dir`: directorio de checkpoint bajo `tmp_hdfs`.
- `df_factory`: fábrica de DataFrames a partir de listas de dicts/Rows.
"""
import os
import sys

import pytest

TEST_MODE = (os.environ.get("APP_TEST_MODE")
             or os.environ.get("APP_MODE") or "unix").lower()
_IS_CLUSTER = TEST_MODE in ("cluster", "hdfs")

if not _IS_CLUSTER:
    # libs.data_engineering_toolbox.__init__ inserta en sys.path los zips de
    # pyspark del cluster leyendo PYLIB; en local basta un path dummy.
    os.environ.setdefault("PYLIB", "/dev/null")
    # Workers de Spark con el mismo Python que el driver (evita
    # PYTHON_VERSION_MISMATCH cuando `python3` del PATH es otra versión).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def pytest_configure(config):
    config.addinivalue_line("markers", "spark: tests that require a SparkSession")
    config.addinivalue_line("markers", "graphframes: tests that require GraphFrames")


@pytest.fixture(scope="session")
def spark():
    if not _IS_CLUSTER:
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        session = (SparkSession.builder
            .master("local[2]")
            .appName("framework-tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost")
            .getOrCreate())
    else:
        from libs.data_engineering_toolbox.context import SparkSessionBuilder
        session = SparkSessionBuilder().build()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def graphframes(spark):
    """SparkSession con GraphFrames verificado (salta si el jar no está)."""
    from graphframes import GraphFrame
    from pyspark.sql import Row
    if not _IS_CLUSTER:
        pytest.importorskip("graphframes")
        try:
            v = spark.createDataFrame([Row(id="a")])
            e = spark.createDataFrame([Row(src="a", dst="a")])
            GraphFrame(v, e)
        except Exception as exc:
            pytest.skip(f"GraphFrames no disponible en esta sesión: {exc}")
        return spark
    v = spark.createDataFrame([Row(id="a")])
    e = spark.createDataFrame([Row(src="a", dst="a")])
    GraphFrame(v, e)
    return spark


@pytest.fixture()
def tmp_hdfs(tmp_path):
    """Directorio temporal para tests que escriben parquets.

    - modos locales (unix/windows/wsl): `tmp_path` de pytest.
    - modo "cluster"/"hdfs": HivePath de `APP_TMP_TESTS_DIR_HDFS`.
    """
    if not _IS_CLUSTER:
        return tmp_path
    from libs.data_engineering_toolbox.path import HivePath
    return HivePath(os.environ.get("APP_TMP_TESTS_DIR_HDFS"))


@pytest.fixture()
def checkpoint_dir(spark, tmp_hdfs):
    """Directorio de checkpoint para tests de GraphFrames."""
    spark.sparkContext.setCheckpointDir(str(tmp_hdfs / "checkpoint"))
    return tmp_hdfs / "checkpoint"


@pytest.fixture()
def df_factory(spark):
    """Fábrica de DataFrames de Spark a partir de listas de dicts/Rows."""
    def _make(rows, schema=None):
        return spark.createDataFrame(rows, schema=schema)
    return _make


@pytest.fixture()
def local_hdfs(monkeypatch):
    """Fuerza el backend local de `libs.data_engineering_toolbox.path`.

    Exporta ``APP_FS_MODE`` con el modo de la plataforma actual
    (`windows`/`wsl`/`unix`): las funciones del módulo (`exists`, `_ls`,
    `is_dir`, `mkdir`, ...) despachan entonces a ``path.local`` (filesystem
    real) y `HivePath`/`UniversalPath` funcionan sobre `tmp_path` sin
    parcheos. En modo "cluster"/"hdfs" no hace nada.
    """
    if _IS_CLUSTER:
        return None
    import libs.data_engineering_toolbox.path as pm
    monkeypatch.setenv("APP_FS_MODE", pm.os_mode())
    return pm
