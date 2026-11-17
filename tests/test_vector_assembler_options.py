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
        monkeypatch.setattr(cvas, "AGGREGATION_SPECS", [
            {"name": "", "weight": None,
             "aggregation": ["mean", "median", "std"]}])
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
        monkeypatch.setattr(cvas, "AGGREGATION_SPECS", [
            {"name": "", "weight": None, "aggregation": "mean"}])
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        df = step._compute_level_features("numcliente")
        assert {"feat_ceps", "node_count_ceps", "numcliente"} == set(df.columns)


# ----------------------------------------------------------------------------
# AGGREGATION_SPECS (lista de transformaciones de agregación final)
# ----------------------------------------------------------------------------

class TestAggregationSpecs:
    @pytest.fixture(autouse=True)
    def _base(self, monkeypatch):
        monkeypatch.setattr(cvas, "VARIABLE_SUFFIX", "_ceps")

    def test_named_spec_prefixes_columns(self, spark, nodes_df, monkeypatch):
        monkeypatch.setattr(cvas, "AGGREGATION_SPECS", [
            {"name": "", "weight": None, "aggregation": "mean"},
            {"name": "mx", "weight": None, "aggregation": ["max"]},
        ])
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        df = step._compute_level_features("numcliente")
        assert {"feat_ceps", "mx_max_feat_ceps", "node_count_ceps",
                "numcliente"} == set(df.columns)
        result = {r["numcliente"]: r for r in df.collect()}
        assert result["c1"]["mx_max_feat_ceps"] == pytest.approx(3.0)

    def test_specs_with_same_weight_share_groupby(self, spark, nodes_df,
            monkeypatch):
        """Dos specs sin peso (mismo grupo) coexisten con nombres propios."""
        monkeypatch.setattr(cvas, "AGGREGATION_SPECS", [
            {"name": "", "weight": None, "aggregation": ["mean", "max"]},
            {"name": "std_only", "weight": None, "aggregation": ["std"]},
        ])
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        df = step._compute_level_features("numcliente")
        assert {"mean_feat_ceps", "max_feat_ceps", "std_only_std_feat_ceps",
                "node_count_ceps", "numcliente"} == set(df.columns)

    def test_include_prefixes_restricts_features(self, spark, monkeypatch):
        nodes = spark.createDataFrame(
            [("n1", ["c1"], 1.0, 10.0), ("n2", ["c1"], 3.0, 20.0)],
            ["id", "numcliente", "risk_x", "other_y"])
        monkeypatch.setattr(cvas, "AGGREGATION_SPECS", [
            {"name": "", "weight": None, "aggregation": "mean"},
            {"name": "risk", "weight": None, "aggregation": ["max"],
             "include_prefixes": ["risk_"]},
        ])
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes)
        set_cached(step, "feature_columns", ["risk_x", "other_y"])
        df = step._compute_level_features("numcliente")
        # el spec "risk" solo agrega risk_*; el default agrega ambas
        assert "risk_max_risk_x_ceps" in df.columns
        assert "risk_max_other_y_ceps" not in df.columns
        assert "risk_x_ceps" in df.columns and "other_y_ceps" in df.columns

    def test_node_count_emitted_once(self, spark, nodes_df, monkeypatch):
        monkeypatch.setattr(cvas, "AGGREGATION_SPECS", [
            {"name": "", "weight": None, "aggregation": "mean"},
            {"name": "mx", "weight": None, "aggregation": ["max"]},
        ])
        step = make_step(spark)
        set_cached(step, "nodes_with_features", nodes_df)
        set_cached(step, "feature_columns", ["feat"])
        df = step._compute_level_features("numcliente")
        assert df.columns.count("node_count_ceps") == 1

    def test_empty_specs_raise(self, spark, nodes_df, monkeypatch):
        from libs.functions.assembly import run_aggregation_specs
        with pytest.raises(ValueError):
            run_aggregation_specs(
                nodes_df, group_column="id", feature_columns=["oper_mto"],
                specs=[])


# ----------------------------------------------------------------------------
# NODE_TRANSFORMS (transformaciones a nivel nodo antes del agregado)
# ----------------------------------------------------------------------------

class TestNodeTransforms:
    def _step_with_feature_source(self, spark, tmp_path, monkeypatch):
        """Step con `nodes` base y una fuente de features `feat` por id."""
        nodes = spark.createDataFrame(
            [("n1", ["c1"], 10.0, "2025-08-01"),
             ("n2", ["c1"], 30.0, "2025-08-01")],
            ["id", "numcliente", "oper_mto", "information_date"])
        feat_path = str(tmp_path / "feat_src")
        spark.createDataFrame(
            [("n1", 1.0), ("n2", 3.0)], ["id", "feat"]).write.parquet(feat_path)
        monkeypatch.setattr(cvas, "FEATURE_SOURCES", {
            "f": {"mode": "simple", "path": feat_path}})
        monkeypatch.setattr(cvas, "NODE_WEIGHT_COLUMNS", ["oper_mto"])
        monkeypatch.setattr(
            cvas, "NODE_ID_ARRAY_COLUMNS", {"numcliente": "numcliente"})
        monkeypatch.setattr(cvas, "AGGREGATION_LEVELS", ["numcliente"])
        step = make_step(spark)
        set_cached(step, "nodes", nodes)
        return step

    def test_sql_transform_adds_feature_column(self, spark, tmp_path,
            monkeypatch):
        monkeypatch.setattr(cvas, "NODE_TRANSFORMS", [
            {"name": "double_feat", "expression": "feat * 2"}])
        step = self._step_with_feature_source(spark, tmp_path, monkeypatch)
        result = step.nodes_with_features
        assert "double_feat" in result.columns
        row = {r["id"]: r for r in result.collect()}["n2"]
        assert row["double_feat"] == pytest.approx(6.0)
        # la columna transformada entra sola a las features agregables
        assert "double_feat" in step.feature_columns

    def test_empty_transforms_is_noop(self, spark, tmp_path, monkeypatch):
        monkeypatch.setattr(cvas, "NODE_TRANSFORMS", [])
        step = self._step_with_feature_source(spark, tmp_path, monkeypatch)
        assert "double_feat" not in step.nodes_with_features.columns
        assert "tfrom_days" in step.nodes_with_features.columns


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
