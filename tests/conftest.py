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
    # PySpark de PyPI exige una SparkSession activa para construir
    # Window/Column en tiempo de import (los módulos de config definen ventanas
    # a nivel de módulo). Se arranca la sesión al importar el conftest para que
    # la recolección de tests no falle; la fixture `spark` la reutiliza.
    try:
        from pyspark.sql import SparkSession as _SS
        _builder = (_SS.builder
            .master("local[2]")
            .appName("minerva-tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost"))
        _jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
            "..", ".local_jars")
        if os.path.isdir(_jars_dir):
            _jars = [os.path.join(_jars_dir, f)
                for f in sorted(os.listdir(_jars_dir)) if f.endswith(".jar")]
            if _jars:
                _builder = _builder.config("spark.jars", ",".join(_jars))
        _builder.getOrCreate()
    except ImportError:
        pass  # sin pyspark: los tests que lo requieren se saltan solos


def pytest_configure(config):
    config.addinivalue_line("markers", "spark: tests that require a SparkSession")
    config.addinivalue_line("markers", "graphframes: tests that require GraphFrames")


@pytest.fixture(scope="session")
def spark():
    if TEST_MODE == "local":
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        builder = (SparkSession.builder
            .master("local[2]")
            .appName("minerva-tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "localhost"))
        # Jars locales (graphframes + deps) si existen: habilitan los tests
        # marcados `graphframes` en modo local en vez de saltarlos.
        jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)),
            "..", ".local_jars")
        if os.path.isdir(jars_dir):
            jars = [os.path.join(jars_dir, f)
                for f in sorted(os.listdir(jars_dir)) if f.endswith(".jar")]
            if jars:
                builder = builder.config("spark.jars", ",".join(jars))
        session = builder.getOrCreate()
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


@pytest.fixture()
def local_hdfs(monkeypatch):
    """Redirige `libs.data_engineering_toolbox.path` al filesystem local.

    Parchea las primitivas HDFS (`_ls`, `exists`, `is_dir`, ...) para que los
    decoradores de particionado del framework funcionen sobre `tmp_path` en
    modo "local". En modo "cluster" no parchea nada.
    """
    if TEST_MODE != "local":
        return None
    import shutil
    from datetime import datetime
    from pathlib import Path
    import libs.data_engineering_toolbox.path as pm

    def _local_ls(path, *args):
        base = Path(path)
        if not base.exists():
            return []
        it = base.rglob("*") if "-R" in args else base.glob("*")
        entries = []
        for p in sorted(it):
            if not p.exists():
                continue
            st = p.stat()
            entries.append({
                "file_type": "d" if p.is_dir() else "-",
                "permissions": "drwxrwxrwx" if p.is_dir() else "-rw-r--r--",
                "copies": "1", "user": "u", "group": "g",
                "size": str(st.st_size),
                "date_and_time": datetime.fromtimestamp(st.st_mtime),
                "path": p.as_posix(),
            })
        return entries

    monkeypatch.setattr(pm, "_ls", _local_ls)
    monkeypatch.setattr(pm, "exists", lambda p: Path(p).exists())
    monkeypatch.setattr(pm, "is_dir", lambda p: Path(p).is_dir())
    monkeypatch.setattr(pm, "is_file", lambda p: Path(p).is_file())
    monkeypatch.setattr(
        pm, "mkdir", lambda p, *a: Path(p).mkdir(parents=True, exist_ok=True))

    def _touch(p):
        Path(p).parent.mkdir(parents=True, exist_ok=True)
        Path(p).touch()
    monkeypatch.setattr(pm, "touch", _touch)
    monkeypatch.setattr(pm, "rmdir",
        lambda p, *a: shutil.rmtree(p, ignore_errors=True))
    monkeypatch.setattr(pm, "mv", lambda s, d: shutil.move(str(s), str(d)))
    return pm
