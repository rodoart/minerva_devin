"""Tests para las features de clustering.

- `lff.cluster_group_stats`: stats intra-grupo por nodo (solo `spark`).
- `StandardClusterFeaturesSubStep.subcluster`: sub-partición dirigida con
  GraphFrames (marcado `graphframes`, se salta si el jar no está).
"""
import os

import pytest

# config.job exige MINERVA_TODAY en import; los tests usan un vintage fijo.
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

pytest.importorskip("pyspark")

from pyspark.sql.functions import (col, count as spark_count,
    sum as spark_sum, mean as spark_mean, stddev as spark_std,
    min as spark_min, max as spark_max, percentile_approx)

import libs.functions.features as lff

pytestmark = pytest.mark.spark


STATS = {
    "count":  spark_count,
    "sum":    spark_sum,
    "mean":   lambda c: spark_mean(col(c)),
    "median": lambda c: percentile_approx(col(c), 0.5),
    "std":    lambda c: spark_std(col(c)),
    "min":    spark_min,
    "max":    spark_max,
}


@pytest.fixture()
def grouped_df(spark):
    """Dos grupos: {a,b,c} en component_id=1 y {d,e} en component_id=2."""
    return spark.createDataFrame(
        [("a", 1, 0.0, 10.0), ("b", 1, 1.0, 20.0), ("c", 1, 1.0, 30.0),
         ("d", 2, 0.0, 40.0), ("e", 2, 1.0, 60.0)],
        ["id", "component_id", "target_lovelace", "oper_mto"])


# ----------------------------------------------------------------------------
# cluster_group_stats
# ----------------------------------------------------------------------------

class TestClusterGroupStats:
    def test_columns(self, grouped_df):
        df = lff.cluster_group_stats(
            grouped_df, "component_id", ["target_lovelace"], stats=STATS)
        expected = {"id", "component_id", "cluster_component_id_size"} | {
            f"cluster_component_id_{s}_target_lovelace" for s in STATS}
        assert set(df.columns) == expected

    def test_group_size(self, grouped_df):
        df = lff.cluster_group_stats(
            grouped_df, "component_id", ["oper_mto"], stats=STATS)
        rows = {r["id"]: r for r in df.collect()}
        assert rows["a"]["cluster_component_id_size"] == 3
        assert rows["e"]["cluster_component_id_size"] == 2

    def test_stats_math(self, grouped_df):
        df = lff.cluster_group_stats(
            grouped_df, "component_id",
            ["target_lovelace", "oper_mto"], stats=STATS)
        rows = {r["id"]: r for r in df.collect()}
        g1 = rows["a"]
        assert g1["cluster_component_id_count_target_lovelace"] == 3
        assert g1["cluster_component_id_sum_target_lovelace"] == pytest.approx(2.0)
        assert g1["cluster_component_id_mean_target_lovelace"] == pytest.approx(2 / 3)
        assert g1["cluster_component_id_median_target_lovelace"] == pytest.approx(1.0)
        assert g1["cluster_component_id_min_target_lovelace"] == 0.0
        assert g1["cluster_component_id_max_target_lovelace"] == 1.0
        assert g1["cluster_component_id_sum_oper_mto"] == pytest.approx(60.0)
        assert g1["cluster_component_id_mean_oper_mto"] == pytest.approx(20.0)
        assert g1["cluster_component_id_median_oper_mto"] == pytest.approx(20.0)
        g2 = rows["d"]
        assert g2["cluster_component_id_sum_oper_mto"] == pytest.approx(100.0)
        assert g2["cluster_component_id_mean_oper_mto"] == pytest.approx(50.0)
        # stddev muestral de {40, 60} = sqrt(200)
        assert g2["cluster_component_id_std_oper_mto"] == pytest.approx(14.142135623730951)

    def test_members_share_group_stats(self, grouped_df):
        df = lff.cluster_group_stats(
            grouped_df, "component_id", ["oper_mto"], stats=STATS)
        rows = {r["id"]: r for r in df.collect()}
        assert (rows["a"]["cluster_component_id_sum_oper_mto"]
                == rows["b"]["cluster_component_id_sum_oper_mto"]
                == rows["c"]["cluster_component_id_sum_oper_mto"])
        assert (rows["a"]["cluster_component_id_sum_oper_mto"]
                != rows["d"]["cluster_component_id_sum_oper_mto"])

    def test_default_stats(self, grouped_df):
        """Sin `stats` usa STANDARD_WEIGHT_STATS."""
        df = lff.cluster_group_stats(
            grouped_df, "component_id", ["oper_mto"])
        for func_name in lff.STANDARD_WEIGHT_STATS:
            assert f"cluster_component_id_{func_name}_oper_mto" in df.columns


# ----------------------------------------------------------------------------
# subcluster (GraphFrames)
# ----------------------------------------------------------------------------

@pytest.mark.graphframes
class TestSubcluster:
    @pytest.fixture()
    def substep(self):
        from pipelines.features.ceps.cluster_features import (
            StandardClusterFeaturesSubStep)
        return StandardClusterFeaturesSubStep

    @pytest.fixture()
    def nodes_edges(self, graphframes):
        """{a,b} ciclo (mismo scc), c colgando de b, d aislado con self-loop."""
        nodes = graphframes.createDataFrame(
            [("a",), ("b",), ("c",), ("d",)], ["id"])
        edges = graphframes.createDataFrame(
            [("a", "b"), ("b", "a"), ("b", "c"), ("d", "d")], ["src", "dst"])
        return nodes, edges

    def test_scc_groups(self, substep, nodes_edges, checkpoint_dir):
        nodes, edges = nodes_edges
        df = substep.subcluster(nodes, edges, method="scc", max_iter=5)
        assert set(df.columns) == {"id", "scc"}
        rows = {r["id"]: r["scc"] for r in df.collect()}
        assert rows["a"] == rows["b"]          # ciclo a<->b: mismo scc
        assert rows["a"] != rows["c"]          # c solo recibe: scc distinto
        assert len({rows["a"], rows["c"], rows["d"]}) == 3

    def test_label_propagation_warns(self, substep, nodes_edges,
            checkpoint_dir, caplog):
        nodes, edges = nodes_edges
        df = substep.subcluster(
            nodes, edges, method="label_propagation", max_iter=5)
        assert set(df.columns) == {"id", "scc"}
        assert any("NO es determinista" in r.message for r in caplog.records)

    def test_unknown_method_raises(self, substep, nodes_edges):
        nodes, edges = nodes_edges
        with pytest.raises(ValueError, match="subcluster"):
            substep.subcluster(nodes, edges, method="kmeans")


# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

class TestConfig:
    def test_group_columns_excluded_from_aggregation(self):
        import config.features.ceps.cluster_features as cclf
        for group_column in cclf.GROUP_COLUMNS:
            assert group_column in cclf.AGGREGATE_EXCLUDE_COLUMNS
        assert "id" in cclf.AGGREGATE_EXCLUDE_COLUMNS

    def test_stats_return_columns(self, grouped_df):
        import config.features.ceps.cluster_features as cclf
        for func in cclf.CLUSTER_STATS.values():
            assert func("oper_mto") is not None

    def test_feature_sources_cover_graph_features(self):
        import config.features.ceps.cluster_features as cclf
        assert "contagion" in cclf.GRAPH_FEATURE_SOURCES
        assert "components" in cclf.input
        assert "edges" in cclf.input
        assert "nodes_join_target_lovelace" in cclf.input
        assert "cluster_stats" in cclf.output

    def test_intermediate_parquets_declared(self):
        """Los intermedios pesados (SCC + join enriquecido) se materializan."""
        import config.features.ceps.cluster_features as cclf
        for key in ("subcluster_df", "nodes_enriched", "checkpoint"):
            assert key in cclf.output
            assert "table_or_hdfs" in cclf.output[key]

    def test_vector_assembler_source_registered(self):
        import config.features.ceps.vector_assembler as cvas
        assert cvas.FEATURE_SOURCES["cluster_stats"]["mode"] == "simple"
        for group_column in ("component_id", "scc"):
            assert group_column in cvas.EXCLUDE_COLUMNS


# ----------------------------------------------------------------------------
# REQUIRED_STATS_PREFIXES: targets propagadas y pesos deben entrar en los stats
# ----------------------------------------------------------------------------

def _cluster_substep(nodes_enriched):
    """Substep mínimo con `nodes_enriched` precargado en caché."""
    from pipelines.features.ceps.cluster_features import CepsClusterFeaturesSubStep
    step = object.__new__(CepsClusterFeaturesSubStep)
    step.__dict__["_nodes_enriched_cache"] = nodes_enriched
    return step


class TestRequiredStatsPrefixes:
    """Guarda de `cluster_stats`: si ningún prefijo obligatorio (target_*,
    contagion_*, oper_mto, weight*, *_strength) produce columnas agregables,
    se registra un warning en lugar de construir stats sin señal de target."""

    @pytest.fixture(autouse=True)
    def _minimal_config(self, monkeypatch):
        import config.features.ceps.cluster_features as cclf
        monkeypatch.setattr(cclf, "GROUP_COLUMNS", ["component_id"])
        monkeypatch.setattr(cclf, "CLUSTER_STATS", {"mean": spark_mean})
        monkeypatch.setattr(cclf, "AGGREGATE_EXCLUDE_COLUMNS",
            ["id", "component_id", "scc"])
        monkeypatch.setattr(cclf, "REQUIRED_STATS_PREFIXES",
            ["target_", "contagion_", "oper_mto", "weight", "_strength"])

    def test_warns_when_no_required_column_present(self, spark, caplog):
        df = spark.createDataFrame(
            [("a", 1, 10.0)], ["id", "component_id", "feat"])
        step = _cluster_substep(df)
        step.cluster_stats
        warnings = [r.message for r in caplog.records
            if "prefijo obligatorio" in r.message]
        assert len(warnings) == 5   # un warning por prefijo sin match

    def test_no_warning_with_target_contagion_and_weights(self, spark, caplog):
        df = spark.createDataFrame(
            [("a", 1, 1.0, 0.4, 100.0, 2.0, 5.0),
             ("b", 1, 0.0, 0.1, 200.0, 3.0, 6.0)],
            ["id", "component_id", "target_lovelace", "contagion_composed",
             "oper_mto", "weight_x", "total_strength"])
        step = _cluster_substep(df)
        result = step.cluster_stats
        assert not any("prefijo obligatorio" in r.message
            for r in caplog.records)
        # la target y la propagada se agregan por grupo
        rows = {r["id"]: r for r in result.collect()}
        assert rows["a"]["cluster_component_id_mean_target_lovelace"] == 0.5
        assert rows["a"]["cluster_component_id_mean_contagion_composed"] == \
            pytest.approx(0.25)
        assert rows["a"]["cluster_component_id_mean_total_strength"] == \
            pytest.approx(5.5)

    def test_endswith_prefixes_count_too(self, spark, caplog):
        """`_strength` se busca también como sufijo (in_strength, ...)."""
        df = spark.createDataFrame(
            [("a", 1, 1.0, 0.4, 100.0, 5.0)],
            ["id", "component_id", "target_lovelace", "contagion_x",
             "oper_mto", "in_strength"])
        step = _cluster_substep(df)
        step.cluster_stats
        remaining = [r.message for r in caplog.records
            if "prefijo obligatorio" in r.message]
        # solo falta el prefijo literal "weight"
        assert len(remaining) == 1
        assert "'weight'" in remaining[0]


# ----------------------------------------------------------------------------
# Persistencia de intermedios (reload-or-recompute)
# ----------------------------------------------------------------------------

def _bare_substep(spark, tmp_dir):
    """Substep mínimo con output_hive apuntando a rutas temporales locales."""
    from pipelines.features.ceps.cluster_features import (
        CepsClusterFeaturesSubStep)
    step = object.__new__(CepsClusterFeaturesSubStep)
    step.sqlContext = spark
    step.is_dynamic = True
    step.tmp_paths = []
    step._decorated_cache = {}
    step.output_hive = {
        key: {"table_or_hdfs": str(tmp_dir / key), "keep_or_delete": "delete"}
        for key in ("subcluster_df", "nodes_enriched", "cluster_stats")
    }
    return step


class TestIntermediatePersistence:
    """Los parquets intermedios hacen reanudable el step: en una segunda
    instancia (p.ej. tras morir el proceso) se recargan sin recomputar."""

    def test_subcluster_df_reloaded_without_recomputing(
            self, spark, tmp_path, monkeypatch):
        """El SCC de GraphFrames solo corre una vez; el segundo acceso lee el
        parquet (los checkpoints orgánicos del algoritmo no son reanudables)."""
        import config.features.ceps.cluster_features as cclf
        monkeypatch.setattr(cclf, "SUBCLUSTER_METHOD", "scc")
        monkeypatch.setattr(cclf, "SUBCLUSTER_MAX_ITER", 3)

        calls = []
        def fake_subcluster(nodes_df, edges_df, **kwargs):
            calls.append(1)
            return spark.createDataFrame([("a", 7)], ["id", "scc"])

        step = _bare_substep(spark, tmp_path)
        step.__dict__["_nodes_join_target_cache"] = (
            spark.createDataFrame([("a",)], ["id"]))
        step.__dict__["_edges_cache"] = (
            spark.createDataFrame([("a", "a")], ["src", "dst"]))
        monkeypatch.setattr(step, "define_checkpoint",
            lambda checkpoint_hdfs: None)
        monkeypatch.setattr(step, "subcluster", fake_subcluster)

        first = step.subcluster_df
        assert calls == [1]
        assert first.collect()[0]["scc"] == 7

        # Segunda instancia (simula reinicio del proceso): recarga, no recomputa.
        step2 = _bare_substep(spark, tmp_path)
        monkeypatch.setattr(step2, "subcluster", fake_subcluster)
        second = step2.subcluster_df
        assert calls == [1]          # no se volvió a llamar al algoritmo
        assert second.collect()[0]["scc"] == 7

    def test_cluster_stats_output_reloaded(self, spark, tmp_path):
        """La salida final también se recarga del parquet ya escrito."""
        stats = spark.createDataFrame(
            [("a", 1.0)], ["id", "cluster_component_id_mean_target_lovelace"])
        step = _bare_substep(spark, tmp_path)
        step.__dict__["_cluster_stats_cache"] = stats
        assert step.cluster_stats_output.count() == 1

        step2 = _bare_substep(spark, tmp_path)
        reloaded = step2.cluster_stats_output
        assert reloaded.columns == stats.columns
        assert reloaded.collect()[0]["id"] == "a"


# ----------------------------------------------------------------------------
# Subcluster desactivable
# ----------------------------------------------------------------------------

class TestSubclusterToggle:
    """Con SUBCLUSTER_ENABLED=False el step solo agrupa por component_id:
    no corre el algoritmo de grafo ni une la columna `scc`."""

    def _step(self, spark, tmp_path, monkeypatch, enabled):
        import config.features.ceps.cluster_features as cclf
        monkeypatch.setattr(cclf, "SUBCLUSTER_ENABLED", enabled)
        monkeypatch.setattr(cclf, "GRAPH_FEATURE_SOURCES", {})
        step = _bare_substep(spark, tmp_path)
        step.__dict__["_nodes_join_target_cache"] = (
            spark.createDataFrame([("a", 0.5)], ["id", "target_lovelace"]))
        step.__dict__["_components_df_cache"] = (
            spark.createDataFrame([("a", 1)], ["id", "component_id"]))
        return step

    def test_nodes_enriched_without_scc_when_disabled(
            self, spark, tmp_path, monkeypatch):
        step = self._step(spark, tmp_path, monkeypatch, enabled=False)
        df = step.nodes_enriched
        assert "component_id" in df.columns
        assert "scc" not in df.columns

    def test_nodes_enriched_joins_scc_when_enabled(
            self, spark, tmp_path, monkeypatch):
        step = self._step(spark, tmp_path, monkeypatch, enabled=True)
        step.__dict__["_subcluster_df_cache"] = (
            spark.createDataFrame([("a", 9)], ["id", "scc"]))
        df = step.nodes_enriched
        assert df.collect()[0]["scc"] == 9

    def test_cluster_stats_ignores_missing_group_columns(
            self, spark, tmp_path, monkeypatch):
        """GROUP_COLUMNS puede seguir listando "scc": si la columna no existe
        se omite con warning en lugar de fallar."""
        import config.features.ceps.cluster_features as cclf
        monkeypatch.setattr(cclf, "SUBCLUSTER_ENABLED", False)
        monkeypatch.setattr(cclf, "GROUP_COLUMNS", ["component_id", "scc"])
        monkeypatch.setattr(cclf, "REQUIRED_STATS_PREFIXES", [])
        monkeypatch.setattr(cclf, "AGGREGATE_EXCLUDE_COLUMNS",
            ["id", "component_id", "scc"])
        step = _bare_substep(spark, tmp_path)
        step.__dict__["_nodes_enriched_cache"] = spark.createDataFrame(
            [("a", 1, 0.5)], ["id", "component_id", "target_lovelace"])
        stats = step.cluster_stats
        assert not any("scc" in c for c in stats.columns)
        assert any(c.startswith("cluster_component_id_")
            for c in stats.columns)

    def _runnable_step(self, spark, tmp_path, monkeypatch, enabled):
        import config.features.ceps.cluster_features as cclf
        monkeypatch.setattr(cclf, "SUBCLUSTER_ENABLED", enabled)
        step = _bare_substep(spark, tmp_path)
        step.step_name = "test_substep"
        step.output_parameters = {"test_substep": {}}
        step.__dict__["_cluster_stats_output_cache"] = (
            spark.createDataFrame([("a", 1.0)], ["id", "x"]))
        return step

    def test_warns_when_subcluster_enabled(
            self, spark, tmp_path, monkeypatch, caplog):
        """Al llegar al step con la opción activa se avisa de que es pesada."""
        import logging
        step = self._runnable_step(spark, tmp_path, monkeypatch, enabled=True)
        with caplog.at_level(logging.WARNING):
            step.step_action()
        assert any("SUBCLUSTER_ENABLED" in r.message
            for r in caplog.records)

    def test_no_warning_when_subcluster_disabled(
            self, spark, tmp_path, monkeypatch, caplog):
        import logging
        step = self._runnable_step(spark, tmp_path, monkeypatch, enabled=False)
        with caplog.at_level(logging.WARNING):
            step.step_action()
        assert not any("SUBCLUSTER_ENABLED" in r.message
            for r in caplog.records)


# ----------------------------------------------------------------------------
# Config de propagación multi-columna
# ----------------------------------------------------------------------------

class TestPropagationConfig:
    def test_mode_columns_simple_form(self):
        import config.features.ceps.target_propagation_features as cfcf
        mode = {"aggregation_function": spark_max,
                "missing_treatment": ["mean_with_nulls"]}
        assert cfcf._mode_columns(mode) == {"target": {
            "aggregation_function": spark_max,
            "missing_treatment": ["mean_with_nulls"]}}

    def test_mode_columns_dict_form(self):
        import config.features.ceps.target_propagation_features as cfcf
        columns = {
            "target": {"aggregation_function": spark_max},
            "score": {"aggregation_function": spark_mean},
        }
        assert cfcf._mode_columns({"columns": columns}) == columns

    def test_propagation_node_columns_multi(self):
        import config.features.ceps.target_propagation_features as cfcf
        targets = {"lovelace": {"modes": {"cta": {"columns": {
            "target": {"aggregation_function": spark_max},
            "score": {"aggregation_function": spark_mean},
        }}}}}
        assert cfcf.propagation_node_columns(targets) == [
            "target_lovelace", "target_lovelace_score"]

    def test_propagation_features_naming(self):
        """contagion_<weight> para 'target' y contagion_<col>_<weight> para
        el resto de columnas propagadas."""
        import config.features.ceps.target_propagation_features as cfcf
        targets = {"lovelace": {"modes": {"cta": {"columns": {
            "target": {"aggregation_function": spark_max},
            "score": {"aggregation_function": spark_mean},
        }}}}}
        columns = cfcf.propagation_node_columns(targets)
        for node_column in columns:
            expected = ("contagion_" if node_column == "target_lovelace"
                else f"contagion_{node_column.replace('target_lovelace_', '')}_")
            # replica la regla de PROPAGATION_FEATURES
            name = ("contagion_w"
                if node_column == "target_lovelace"
                else f"contagion_score_w")
            assert name.startswith(expected)

    def test_propagation_features_targets_valid_columns(self):
        import config.features.ceps.target_propagation_features as cfcf
        valid = set(cfcf.propagation_node_columns())
        for feature in cfcf.PROPAGATION_FEATURES:
            kwargs = list(feature.values())[0]
            assert kwargs["target_column"] in valid
            assert list(feature)[0].startswith("contagion_")
