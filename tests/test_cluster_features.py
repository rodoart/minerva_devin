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

    def test_vector_assembler_source_registered(self):
        import config.features.ceps.vector_assembler as cvas
        assert cvas.FEATURE_SOURCES["cluster_stats"]["mode"] == "simple"
        for group_column in ("component_id", "scc"):
            assert group_column in cvas.EXCLUDE_COLUMNS
