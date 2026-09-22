"""Fixtures compartidas de la suite de tests de Minerva.

- `spark`: SparkSession local[2] (se salta automáticamente si pyspark no está
  instalado).
- `graphframes`: verifica que GraphFrame se puede instanciar (jar cargado); si
  no, salta los tests que lo requieran.
- `tmp_step`: Step de framework mínimo sin dependencia de Spark.
"""
import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    session = (SparkSession.builder
        .master("local[2]")
        .appName("minerva-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def graphframes(spark):
    """SparkSession con GraphFrames verificado (salta si el jar no está)."""
    pytest.importorskip("graphframes")
    from graphframes import GraphFrame
    from pyspark.sql import Row
    try:
        v = spark.createDataFrame([Row(id="a")])
        e = spark.createDataFrame([Row(src="a", dst="a")])
        GraphFrame(v, e)
    except Exception as exc:
        pytest.skip(f"GraphFrames no disponible en esta sesión: {exc}")
    return spark


@pytest.fixture()
def checkpoint_dir(spark, tmp_path):
    """Directorio de checkpoint para tests de GraphFrames."""
    spark.sparkContext.setCheckpointDir(str(tmp_path / "checkpoint"))
    return tmp_path / "checkpoint"


@pytest.fixture()
def df_factory(spark):
    """Fábrica de DataFrames de Spark a partir de listas de dicts/Rows."""
    def _make(rows, schema=None):
        return spark.createDataFrame(rows, schema=schema)
    return _make
