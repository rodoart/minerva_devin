"""Fixtures compartidas de la suite de tests de Minerva.

Modo de ejecución (variable `MINERVA_TEST_MODE`):
- `"local"` (default): SparkSession local[2], rutas temporales en `tmp_path`
  del filesystem local y `PYLIB` dummy. Corre sin cluster.
- `"cluster"`: `SparkSessionBuilder()` (sesión YARN real) y `tmp_hdfs` sobre
  HDFS (`MINERVA_TMP_TESTS_DIR_HDFS`). Requiere `opt/environment_vars.sh`.

Fixtures:
- `spark`: SparkSession (local[2] en modo local, YARN en modo cluster).
- `graphframes`: verifica que GraphFrame se puede instanciar.
- `tmp_hdfs`: directorio temporal — `tmp_path` en local, HivePath en cluster.
- `checkpoint_dir`: directorio de checkpoint bajo `tmp_hdfs`.
- `df_factory`: fábrica de DataFrames a partir de listas de dicts/Rows.
"""
import os
import sys

import pytest

TEST_MODE = os.environ.get("MINERVA_TEST_MODE", "local")

if TEST_MODE == "local":
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
    if TEST_MODE == "local":
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        session = (SparkSession.builder
            .master("local[2]")
            .appName("minerva-tests")
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
    if TEST_MODE == "local":
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

    - modo "local": `tmp_path` de pytest (filesystem local).
    - modo "cluster": HivePath de `MINERVA_TMP_TESTS_DIR_HDFS`.
    """
    if TEST_MODE == "local":
        return tmp_path
    from libs.data_engineering_toolbox.path import HivePath
    return HivePath(os.environ.get("MINERVA_TMP_TESTS_DIR_HDFS"))


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
