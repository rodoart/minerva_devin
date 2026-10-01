"""Tests de las opciones del vector assembler CEPS.

Cubre:
- Niveles de agregación (`AGGREGATION_LEVELS`, `NODE_ID_ARRAY_COLUMNS`,
  `PIVOT_BY_LEVEL`) -> `_compute_level_features` + `level_pivot`.
- Multi-agregación por feature (`FEATURE_AGGREGATION` lista) ->
  columnas `{funcion}_{feature}`.
- `VARIABLE_SUFFIX` -> todas las variables llevan el sufijo (`_ceps`).
- Fuentes opcionales: `"enabled": False` las desactiva y `"optional": True`
  convierte errores de carga en warning sin tumbar el step.
- Esquema final: `{nivel}_features_vector` = SOLO llave + columna vector.
"""
import os
from datetime import date

import pytest

pytest.importorskip("pyspark")

# config.job exige MINERVA_TODAY en import; los tests usan un vintage fijo.
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

import config.features.ceps.vector_assembler as cvas
from pipelines.features.ceps.vector_assembler import LovelaceCepsAssemblerSubStep

pytestmark = pytest.mark.spark


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

def make_step(spark, **kwargs):
    """Instancia mínima del substep sin `__init__` (como test_merge_schema_input)."""
    step = object.__new__(LovelaceCepsAssemblerSubStep)
    step.sqlContext = spark
    step.input_hive = kwargs.get("input_hive", {})
    step.output_hive = kwargs.get("output_hive", {})
    step.input_parameters = kwargs.get("input_parameters", {
        "vintage_date": date(2025, 8, 31)})
    step._decorated_cache = {}
    return step


@pytest.fixture()
def nodes_df(spark):
    """Nodos con arrays de ids multi-nivel, peso y una feature."""
    return spark.createDataFrame(
        [
            # id, numclientes, ctas, oper_mto, information_date, feat
            ("n1", ["c1"],      ["cta1"],        10.0, "2025-08-01", 1.0),
            ("n2", ["c1", "c2"],["cta1", "cta2"], 30.0, "2025-08-01", 3.0),
            ("n3", ["c3"],      ["cta9"],        5.0,  "2025-08-01", 2.0),
        ],
        ["id", "numcliente", "cta", "oper_mto", "information_date", "feat"])


def set_cached(step, name, value):
    """Precarga un `@ppf.cached_property` en el `__dict__` del step."""
    step.__dict__[f"_{name}_cache"] = value


# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

class TestConfig:
    def test_levels_and_outputs(self):
        for level in cvas.AGGREGATION_LEVELS:
            assert level in cvas.NODE_ID_ARRAY_COLUMNS
            assert level in cvas.PIVOT_BY_LEVEL
            assert f"{level}_features" in cvas.output
            assert f"{level}_features_vector" in cvas.output

    def test_suffix_and_null_sentinel(self):
        assert cvas.VARIABLE_SUFFIX == "_ceps"
        assert cvas.FILL_NULLS_VALUE == -99999.0

    def test_feature_sources_flags_default(self):
        for name, source in cvas.FEATURE_SOURCES.items():
            assert source.get("enabled", True)
            assert source.get("optional", True)


# ----------------------------------------------------------------------------
# Helpers de nivel
# ----------------------------------------------------------------------------

class TestNodeArrayColumns:
    def test_returns_configured_arrays(self, monkeypatch):
        monkeypatch.setattr(cvas, "AGGREGATION_LEVELS", ["numcliente", "cta"])
        assert LovelaceCepsAssemblerSubStep.node_array_columns() == [
            "numcliente", "cta"]

    def test_ignores_unmapped_levels(self, monkeypatch):
        monkeypatch.setattr(cvas, "AGGREGATION_LEVELS", ["numcliente", "nom"])
        monkeypatch.setattr(
            cvas, "NODE_ID_ARRAY_COLUMNS", {"numcliente": "numcliente"})
        assert LovelaceCepsAssemblerSubStep.node_array_columns() == [
            "numcliente"]


class TestSuffixed:
    def test_renames_all_but_key(self, spark, monkeypatch):
        monkeypatch.setattr(cvas, "VARIABLE_SUFFIX", "_ceps")
        df = spark.createDataFrame(
            [("c1", 1.0, 2)], ["numcliente", "feat", "n"])
        result = LovelaceCepsAssemblerSubStep._suffixed(df, "numcliente")
        assert set(result.columns) == {"numcliente", "feat_ceps", "n_ceps"}

    def test_empty_suffix_is_noop(self, spark, monkeypatch):
        monkeypatch.setattr(cvas, "VARIABLE_SUFFIX", "")
        df = spark.createDataFrame([("c1", 1.0)], ["numcliente", "feat"])
        result = LovelaceCepsAssemblerSubStep._suffixed(df, "numcliente")
        assert set(result.columns) == {"numcliente", "feat"}


class TestVectorColumns:
    def test_excludes_key_and_target_prefix(self, spark, monkeypatch):
        monkeypatch.setattr(cvas, "EXCLUDE_PREFIXES", ["target_"])
        step = make_step(spark)
        features_df = spark.createDataFrame(
            [("c1", 1.0, 0.9, 3)],
            ["numcliente", "feat_ceps", "target_lovelace_ceps",
             "node_count_ceps"])
        cols = step.vector_columns(features_df, "numcliente")
        assert cols == ["feat_ceps", "node_count_ceps"]


# ----------------------------------------------------------------------------
# level_pivot
# ----------------------------------------------------------------------------

class TestLevelPivot:
    def test_reads_and_renames_pivot_column(self, spark, tmp_path, monkeypatch):
        pivot_path = str(tmp_path / "pivot")
        spark.createDataFrame(
            [("c1",), ("c1",), ("c2",)], ["num_cliente"]).write.parquet(pivot_path)
        step = make_step(spark, input_hive={
            "pivot": {"table_or_hdfs": pivot_path, "pivot_column": "num_cliente"},
        })
        monkeypatch.setattr(cvas, "PIVOT_BY_LEVEL", {
            "numcliente": {"input_key": "pivot", "column": "num_cliente"}})
        df = step.level_pivot("numcliente")
        assert df.columns == ["numcliente"]
        assert df.count() == 2  # distinct


# ----------------------------------------------------------------------------
# _compute_level_features (agregación multi-nivel, multi-función, sufijo)
# ----------------------------------------------------------------------------

class TestComputeLevelFeatures:
    @pytest.fixture(autouse=True)
    def _fast_aggregation(self, monkeypatch):
        monkeypatch.setattr(cvas, "FEATURE_AGGREGATION", ["mean", "median", "std"])
        monkeypatch.setattr(cvas, "AGGREGATION_WEIGHT", None)
        monkeypatch.setattr(cvas, "VARIABLE_SUFFIX", "_ceps")

    def test_cta_level(self, spark, nodes_df):
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        result = {r["cta"]: r for r in
            step._compute_level_features("cta").collect()}
        # cta1 aparece en n1 (feat=1) y n2 (feat=3)
        assert result["cta1"]["mean_feat_ceps"] == pytest.approx(2.0)
        assert result["cta1"]["median_feat_ceps"] == pytest.approx(2.0)
        assert result["cta1"]["node_count_ceps"] == 2
        assert result["cta2"]["mean_feat_ceps"] == pytest.approx(3.0)
        assert result["cta9"]["mean_feat_ceps"] == pytest.approx(2.0)

    def test_numcliente_level(self, spark, nodes_df):
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        result = {r["numcliente"]: r for r in
            step._compute_level_features("numcliente").collect()}
        assert result["c1"]["mean_feat_ceps"] == pytest.approx(2.0)
        assert result["c1"]["node_count_ceps"] == 2
        assert result["c3"]["std_feat_ceps"] is None  # std de un solo valor

    def test_multi_aggregation_columns(self, spark, nodes_df):
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        df = step._compute_level_features("numcliente")
        assert {"mean_feat_ceps", "median_feat_ceps", "std_feat_ceps",
                "node_count_ceps", "numcliente"} == set(df.columns)

    def test_single_function_keeps_base_name_plus_suffix(self, spark, nodes_df,
            monkeypatch):
        monkeypatch.setattr(cvas, "FEATURE_AGGREGATION", "mean")
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        df = step._compute_level_features("numcliente")
        assert {"feat_ceps", "node_count_ceps", "numcliente"} == set(df.columns)


# ----------------------------------------------------------------------------
# Fuentes opcionales / desactivadas
# ----------------------------------------------------------------------------

class TestOptionalFeatureSources:
    def _write_feature_source(self, spark, tmp_path, name):
        path = str(tmp_path / name)
        spark.createDataFrame(
            [("n1", 7.0), ("n2", 8.0)], ["id", f"feat_{name}"]).write.parquet(path)
        return path

    def test_missing_optional_source_warns_and_continues(
            self, spark, tmp_path, monkeypatch, caplog, nodes_df):
        good = self._write_feature_source(spark, tmp_path, "good")
        monkeypatch.setattr(cvas, "FEATURE_SOURCES", {
            "good": {"mode": "simple", "path": good},
            "missing": {"mode": "simple", "path": str(tmp_path / "no_existe")},
        })
        step = make_step(spark)
        set_cached(step, "nodes", nodes_df.select("id"))
        tables = step.feature_tables
        assert set(tables) == {"good"}
        assert "feat_good" in tables["good"].columns
        assert any("missing" in r.message and "no disponible" in r.message
                   for r in caplog.records)

    def test_mandatory_source_raises(self, spark, tmp_path, monkeypatch, nodes_df):
        monkeypatch.setattr(cvas, "FEATURE_SOURCES", {
            "missing": {"mode": "simple", "path": str(tmp_path / "no_existe"),
                        "optional": False},
        })
        step = make_step(spark)
        set_cached(step, "nodes", nodes_df.select("id"))
        with pytest.raises(Exception):
            step.feature_tables

    def test_disabled_source_is_skipped(self, spark, tmp_path, monkeypatch,
            nodes_df):
        good = self._write_feature_source(spark, tmp_path, "good")
        monkeypatch.setattr(cvas, "FEATURE_SOURCES", {
            "good": {"mode": "simple", "path": good},
            "off": {"mode": "simple", "path": str(tmp_path / "no_existe"),
                    "enabled": False},
        })
        step = make_step(spark)
        set_cached(step, "nodes", nodes_df.select("id"))
        assert set(step.feature_tables) == {"good"}

    def test_feature_columns_dedup(self, spark, tmp_path, monkeypatch, nodes_df):
        """Si dos fuentes traen la misma columna, gana la primera."""
        p1 = self._write_feature_source(spark, tmp_path, "a")
        p2 = str(tmp_path / "b")
        spark.createDataFrame(
            [("n1", 9.0), ("n9", 1.0)], ["id", "feat_a"]).write.parquet(p2)
        monkeypatch.setattr(cvas, "FEATURE_SOURCES", {
            "a": {"mode": "simple", "path": p1},
            "b": {"mode": "simple", "path": p2},
        })
        step = make_step(spark)
        set_cached(step, "nodes", nodes_df.select("id"))
        tables = step.feature_tables
        assert "feat_a" in tables["a"].columns
        # "b" aporta feat_a también -> deduplicada, no aparece en b
        assert tables["b"].columns == ["id"]
