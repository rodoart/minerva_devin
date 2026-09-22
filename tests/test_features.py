"""Tests para libs.functions.features.

Los tests que requieren GraphFrame (pagerank, degrees, propagate_target, ...)
están marcados con `graphframes` y se saltan si el jar no está disponible.
Los que operan a nivel DataFrame solo requieren `spark`.
"""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql.functions import (sum as spark_sum,
    mean as spark_mean)

import libs.functions.features as lff

pytestmark = pytest.mark.spark


# ----------------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------------

@pytest.fixture()
def tiny_graph(graphframes):
    """Grafo mínimo: a->b, b->c, a->c con peso 2.0; d aislado."""
    from graphframes import GraphFrame
    spark = graphframes
    vertices = spark.createDataFrame(
        [("a", 0.9), ("b", 0.0), ("c", 0.0), ("d", 0.0)],
        ["id", "target_lovelace"])
    edges = spark.createDataFrame(
        [("a", "b", 2.0), ("b", "c", 4.0), ("a", "c", 6.0)],
        ["src", "dst", "weight"])
    return GraphFrame(vertices, edges)


@pytest.fixture()
def edges_norm_fixture(tiny_graph):
    degree = lff.get_degree(tiny_graph)
    return lff.weight_normalization(tiny_graph, degree)


# ----------------------------------------------------------------------------
# Registro
# ----------------------------------------------------------------------------

class TestRegistry:
    def test_expected_feature_names(self):
        assert set(lff.GRAPH_FEATURE_FUNCTIONS.keys()) == {
            "pagerank", "degrees", "components", "triangle_count",
            "weighted_pagerank", "weighted_degrees", "weighted_edge_stats",
            "weighted_components", "weighted_triangle_count",
            "target_propagation",
        }

    def test_all_values_callable(self):
        for fn in lff.GRAPH_FEATURE_FUNCTIONS.values():
            assert callable(fn)

    def test_standard_weight_stats_keys(self):
        assert set(lff.STANDARD_WEIGHT_STATS.keys()) == {
            "min", "max", "mean", "std", "count", "countDistinct",
            "sum", "curt", "skew", "so"}


# ----------------------------------------------------------------------------
# Unweighted features (GraphFrames)
# ----------------------------------------------------------------------------

@pytest.mark.graphframes
class TestUnweightedFeatures:
    def test_degrees(self, tiny_graph):
        result = {r["id"]: r for r in lff.degrees(tiny_graph).collect()}
        assert result["a"]["out_degree"] == 2
        assert result["a"]["in_degree"] == 0
        assert result["b"]["in_degree"] == 1
        assert result["b"]["out_degree"] == 1
        assert result["c"]["in_degree"] == 2
        assert result["c"]["total_degree"] == 2

    def test_degrees_column_names(self, tiny_graph):
        assert set(lff.degrees(tiny_graph).columns) == {
            "id", "in_degree", "out_degree", "total_degree"}

    def test_components(self, tiny_graph, checkpoint_dir):
        result = lff.components(tiny_graph)
        assert "component_id" in result.columns
        # a,b,c en la misma componente; d aislada
        rows = {r["id"]: r["component_id"] for r in result.collect()}
        assert rows["a"] == rows["b"] == rows["c"]

    def test_triangle_count(self, tiny_graph):
        result = {r["id"]: r["triangle_count"]
            for r in lff.triangle_count(tiny_graph).collect()}
        assert result["a"] == 1   # triángulo a-b-c
        assert result["b"] == 1
        assert result["c"] == 1

    def test_pagerank_columns(self, tiny_graph):
        result = lff.pagerank(tiny_graph, max_iter=2)
        assert "pagerank" in result.columns
        assert result.count() == 4


# ----------------------------------------------------------------------------
# Weighted features (GraphFrames)
# ----------------------------------------------------------------------------

@pytest.mark.graphframes
class TestWeightedFeatures:
    def test_weighted_degrees(self, tiny_graph):
        result = {r["id"]: r for r in lff.weighted_degrees(tiny_graph).collect()}
        assert result["a"]["out_strength"] == 8.0   # 2 + 6
        assert result["a"]["in_strength"] == 0.0
        assert result["b"]["in_strength"] == 2.0
        assert result["b"]["out_strength"] == 4.0
        assert result["c"]["in_strength"] == 10.0   # 4 + 6
        assert result["c"]["total_strength"] == 10.0

    def test_weighted_degrees_columns(self, tiny_graph):
        assert set(lff.weighted_degrees(tiny_graph).columns) == {
            "id", "in_strength", "out_strength", "total_strength"}

    def test_weighted_edge_stats_columns(self, tiny_graph):
        result = lff.weighted_edge_stats(tiny_graph)
        expected = {f"{name}_weight" for name in lff.STANDARD_WEIGHT_STATS}
        assert expected.issubset(set(result.columns))
        assert "id" in result.columns

    def test_weighted_edge_stats_values(self, tiny_graph):
        result = {r["id"]: r for r in lff.weighted_edge_stats(tiny_graph).collect()}
        # nodo a: aristas incidentes 2+6 -> sum=8, count=2
        assert result["a"]["sum_weight"] == 8.0
        assert result["a"]["count_weight"] == 2
        # nodo c: 4+6 -> mean=5
        assert result["c"]["mean_weight"] == 5.0
        assert result["c"]["max_weight"] == 6.0
        assert result["c"]["min_weight"] == 4.0

    def test_weighted_edge_stats_custom_stats(self, tiny_graph):
        result = lff.weighted_edge_stats(
            tiny_graph, stats={"sum": spark_sum})
        assert "sum_weight" in result.columns
        assert "mean_weight" not in result.columns

    def test_weighted_triangle_count(self, tiny_graph):
        result = {r["id"]: r for r in
            lff.weighted_triangle_count(tiny_graph).collect()}
        assert result["a"]["triangle_count"] == 1
        assert result["a"]["total_strength"] == 8.0

    def test_weighted_components(self, tiny_graph, checkpoint_dir):
        result = lff.weighted_components(tiny_graph)
        assert "component_weight" in result.columns
        rows = {r["id"]: r for r in result.collect()}
        # componente {a,b,c}: peso total aristas = 2+4+6 = 12
        assert rows["a"]["component_weight"] == 12.0


# ----------------------------------------------------------------------------
# Propagación (df-level, sin GraphFrame requerido para helpers)
# ----------------------------------------------------------------------------

@pytest.mark.graphframes
class TestPropagationHelpers:
    def test_get_degree(self, tiny_graph):
        result = {r["node"]: r["deg_sum"]
            for r in lff.get_degree(tiny_graph).collect()}
        assert result["a"] == 8.0    # 2+6 salientes
        assert result["b"] == 6.0    # 2 entrante + 4 saliente
        assert result["c"] == 10.0   # 4+6 entrantes
        assert "d" not in result     # sin aristas -> no aparece

    def test_weight_normalization_columns(self, edges_norm_fixture):
        assert set(edges_norm_fixture.columns) == {
            "src", "dst", "w_src_to_dst", "w_dst_to_src"}

    def test_weight_normalization_math(self, edges_norm_fixture):
        rows = {(r["src"], r["dst"]): r for r in edges_norm_fixture.collect()}
        # a->b: w_src_to_dst = 2/deg_b = 2/6 ; w_dst_to_src = 2/deg_a = 2/8
        assert rows[("a", "b")]["w_src_to_dst"] == pytest.approx(2/6)
        assert rows[("a", "b")]["w_dst_to_src"] == pytest.approx(2/8)
        # a->c: 6/deg_c = 6/10 ; 6/deg_a = 6/8
        assert rows[("a", "c")]["w_src_to_dst"] == pytest.approx(6/10)
        # b->c: 4/deg_c = 4/10 ; 4/deg_b = 4/6
        assert rows[("b", "c")]["w_src_to_dst"] == pytest.approx(4/10)

    def test_propagate_target_basic(self, tiny_graph, edges_norm_fixture):
        result = {r["id"]: r["contagion_score"] for r in
            lff.propagate_target(
                tiny_graph, edges_norm_fixture,
                target_column="target_lovelace",
                max_iter=2, alpha=0.5).collect()}
        # 'a' tiene semilla 0.9 -> sus vecinos reciben parte del score
        assert result["a"] == pytest.approx(0.9)
        assert result["b"] > 0.0
        assert result["c"] > 0.0
        # 'd' aislado: sin aristas -> score 0
        assert result["d"] == 0.0

    def test_propagate_target_seed_floor(self, tiny_graph, edges_norm_fixture):
        result = {r["id"]: r["contagion_score"] for r in
            lff.propagate_target(
                tiny_graph, edges_norm_fixture,
                target_column="target_lovelace",
                keep_seed_floor=True, max_iter=3, alpha=0.9).collect()}
        # la semilla nunca cae por debajo de su valor original
        assert result["a"] >= 0.9

    def test_propagate_target_custom_output_name(self, tiny_graph,
            edges_norm_fixture):
        result = lff.propagate_target(
            tiny_graph, edges_norm_fixture,
            target_column="target_lovelace",
            final_output_column_name="mi_contagion", max_iter=1)
        assert "mi_contagion" in result.columns
        assert "seed_score" not in result.columns


# ----------------------------------------------------------------------------
# Agregación de target a id de nodo (DataFrame-level)
# ----------------------------------------------------------------------------

class TestTargetGroupById:
    def test_basic_aggregation(self, spark):
        group_by_id = spark.createDataFrame(
            [("n1", ["c1", "c2"]), ("n2", ["c3"])],
            ["id", "numcliente"])
        target = spark.createDataFrame(
            [("c1", 0.8), ("c2", 0.4), ("c3", 0.1)],
            ["numcliente", "target"])
        result = {r["id"]: r["target_agg_numcliente"] for r in
            lff.target_group_by_id(group_by_id, target, "numcliente").collect()}
        # max por defecto: n1 -> max(0.8, 0.4) = 0.8 ; n2 -> 0.1
        assert result["n1"] == pytest.approx(0.8)
        assert result["n2"] == pytest.approx(0.1)

    def test_custom_function(self, spark):
        group_by_id = spark.createDataFrame(
            [("n1", ["c1", "c2"])], ["id", "numcliente"])
        target = spark.createDataFrame(
            [("c1", 0.8), ("c2", 0.4)], ["numcliente", "target"])
        result = lff.target_group_by_id(
            group_by_id, target, "numcliente", function=spark_mean).collect()
        assert result[0]["target_agg_numcliente"] == pytest.approx(0.6)

    def test_missing_target_gives_null(self, spark):
        group_by_id = spark.createDataFrame(
            [("n1", ["c9"])], ["id", "numcliente"])
        target = spark.createDataFrame(
            [("c1", 0.8)], ["numcliente", "target"])
        result = lff.target_group_by_id(group_by_id, target, "numcliente")
        assert result.first()["target_agg_numcliente"] is None

    def test_output_column_name(self, spark):
        group_by_id = spark.createDataFrame(
            [("n1", ["c1"])], ["id", "numcliente"])
        target = spark.createDataFrame([("c1", 0.8)], ["numcliente", "target"])
        result = lff.target_group_by_id(group_by_id, target, "numcliente")
        assert result.columns == ["id", "target_agg_numcliente"]


class TestJoinTarget:
    def test_renames_to_suffix(self, spark):
        nodes = spark.createDataFrame([("n1",)], ["id"])
        agg = spark.createDataFrame([("n1", 0.7)], ["id", "target_agg_numcliente"])
        result = lff.join_target(nodes, agg, "numcliente",
            target_column="target_agg_numcliente")
        assert "target_numcliente" in result.columns
        assert result.first()["target_numcliente"] == 0.7

    def test_left_join_keeps_nodes(self, spark):
        nodes = spark.createDataFrame([("n1",), ("n2",)], ["id"])
        agg = spark.createDataFrame([("n1", 0.7)], ["id", "target_agg_numcliente"])
        result = lff.join_target(nodes, agg, "numcliente",
            target_column="target_agg_numcliente")
        assert result.count() == 2
        assert result.filter("id = 'n2'").first()["target_numcliente"] is None
