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
            "degree_balance", "reciprocity", "self_loops",
            "weighted_pagerank", "weighted_degrees", "weighted_edge_stats",
            "weighted_degree_balance",
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

    def test_degree_balance(self, tiny_graph):
        result = {r["id"]: r for r in lff.degree_balance(tiny_graph).collect()}
        # a: out=2, in=0 -> net=2, ratio null (no recibe)
        assert result["a"]["net_degree"] == 2
        assert result["a"]["in_out_degree_ratio"] is None
        # b: out=1, in=1 -> net=0, ratio=1
        assert result["b"]["net_degree"] == 0
        assert result["b"]["in_out_degree_ratio"] == pytest.approx(1.0)
        # c: out=0, in=2 -> net=-2, ratio=0
        assert result["c"]["net_degree"] == -2
        assert result["c"]["in_out_degree_ratio"] == 0.0

    def test_reciprocity(self, graphframes):
        """a<->b recíproco; a->c y c->a existen? no: a->c solo ida."""
        from graphframes import GraphFrame
        vertices = graphframes.createDataFrame(
            [("a",), ("b",), ("c",)], ["id"])
        edges = graphframes.createDataFrame(
            [("a", "b"), ("b", "a"), ("a", "c")], ["src", "dst"])
        g = GraphFrame(vertices, edges)
        result = {r["id"]: r for r in lff.reciprocity(g).collect()}
        # a: out={b,c}, in={b}; recíproco out=1 (b), in=1 (b)
        assert result["a"]["reciprocal_out"] == 1
        assert result["a"]["reciprocal_in"] == 1
        assert result["a"]["reciprocity_out"] == pytest.approx(0.5)
        assert result["a"]["reciprocity_in"] == pytest.approx(1.0)
        assert result["a"]["reciprocity"] == pytest.approx(2 / 3)
        # b: out={a}, in={a}; ambos recíprocos -> ratios 1.0
        assert result["b"]["reciprocity_out"] == pytest.approx(1.0)
        assert result["b"]["reciprocity_in"] == pytest.approx(1.0)
        # c: solo recibe -> reciprocity_in = 0 (a->c no recíproca)
        assert result["c"]["reciprocity_in"] == 0.0

    def test_reciprocity_ignores_self_loops(self, graphframes):
        """Una arista src==dst no cuenta como reciprocidad."""
        from graphframes import GraphFrame
        vertices = graphframes.createDataFrame([("a",)], ["id"])
        edges = graphframes.createDataFrame([("a", "a")], ["src", "dst"])
        g = GraphFrame(vertices, edges)
        rows = lff.reciprocity(g).collect()
        assert rows == []  # el self-loop se excluye de los pares dirigidos

    def test_self_loops(self, graphframes):
        from graphframes import GraphFrame
        vertices = graphframes.createDataFrame([("a",), ("b",)], ["id"])
        edges = graphframes.createDataFrame(
            [("a", "a"), ("a", "a"), ("a", "b")], ["src", "dst"])
        g = GraphFrame(vertices, edges)
        result = {r["id"]: r["self_loop_count"]
            for r in lff.self_loops(g).collect()}
        assert result["a"] == 2
        assert result["b"] == 0  # sin self-loop -> 0, no null


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

    def test_weighted_degree_balance(self, tiny_graph):
        result = {r["id"]: r for r in
            lff.weighted_degree_balance(tiny_graph).collect()}
        # a: out=8, in=0 -> net=8, ratio null
        assert result["a"]["net_strength"] == 8.0
        assert result["a"]["in_out_strength_ratio"] is None
        # b: out=4, in=2 -> net=2, ratio=2
        assert result["b"]["net_strength"] == 2.0
        assert result["b"]["in_out_strength_ratio"] == pytest.approx(2.0)
        # c: out=0, in=10 -> net=-10, ratio=0
        assert result["c"]["net_strength"] == -10.0
        assert result["c"]["in_out_strength_ratio"] == 0.0


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

    def test_propagate_target_checkpoints_each_iteration(
            self, tiny_graph, edges_norm_fixture, tmp_path):
        """Con checkpoint dir fijado, cada iteración trunca el linaje con un
        checkpoint eager durable (dir con archivos) — no solo cache()."""
        import os
        spark_session = getattr(tiny_graph.vertices, "sparkSession", None)
        if spark_session is None:
            spark_session = tiny_graph.vertices.sql_ctx.sparkSession
        checkpoint_dir = str(tmp_path / "ckpt")
        spark_session.sparkContext.setCheckpointDir(checkpoint_dir)
        result = {r["id"]: r["contagion_score"] for r in
            lff.propagate_target(
                tiny_graph, edges_norm_fixture,
                target_column="target_lovelace",
                max_iter=2, alpha=0.5).collect()}
        assert result["a"] == pytest.approx(0.9)
        # el checkpoint eager materializó archivos en el dir por iteración
        assert os.path.isdir(checkpoint_dir)
        assert any(os.scandir(checkpoint_dir))


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

    def test_multi_column_dict(self, spark):
        """Dict {columna: función}: propaga target y scores en una sola pasada."""
        group_by_id = spark.createDataFrame(
            [("n1", ["c1", "c2"]), ("n2", ["c3"])], ["id", "numcliente"])
        target = spark.createDataFrame(
            [("c1", 1.0, 0.9), ("c2", 0.0, 0.1), ("c3", 0.0, 0.5)],
            ["numcliente", "target", "score"])
        result = {r["id"]: r for r in lff.target_group_by_id(
            group_by_id, target, "numcliente",
            function={"target": spark_max, "score": spark_mean}).collect()}
        # n1: max(target)=1, mean(score)=(0.9+0.1)/2=0.5
        assert result["n1"]["target_agg_numcliente"] == pytest.approx(1.0)
        assert result["n1"]["target_agg_numcliente_score"] == pytest.approx(0.5)
        assert result["n2"]["target_agg_numcliente"] == 0.0
        assert result["n2"]["target_agg_numcliente_score"] == pytest.approx(0.5)

    def test_multi_column_names(self, spark):
        """'target' mantiene `target_agg_<col>`; el resto lleva su nombre."""
        group_by_id = spark.createDataFrame(
            [("n1", ["c1"])], ["id", "numcliente"])
        target = spark.createDataFrame(
            [(1, 0.9, 0.3)], ["numcliente", "target", "score"])
        result = lff.target_group_by_id(
            group_by_id, target, "numcliente",
            function={"target": spark_max, "score": spark_mean})
        assert set(result.columns) == {
            "id", "target_agg_numcliente", "target_agg_numcliente_score"}

    def test_multi_column_missing_target_gives_null(self, spark):
        group_by_id = spark.createDataFrame(
            [("n1", ["c9"])], ["id", "numcliente"])
        target = spark.createDataFrame(
            [("c1", 1.0, 0.9)], ["numcliente", "target", "score"])
        result = lff.target_group_by_id(
            group_by_id, target, "numcliente",
            function={"target": spark_max, "score": spark_mean})
        row = result.first()
        assert row["target_agg_numcliente"] is None
        assert row["target_agg_numcliente_score"] is None


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


class TestSaltedDegree:
    """get_degree con sal en dos etapas (grados sesgados por supernodos)."""

    def test_salted_degree_matches_plain(self, tiny_graph):
        plain = {r["node"]: r["deg_sum"] for r in
                 lff.get_degree(tiny_graph).collect()}
        salted = {r["node"]: r["deg_sum"] for r in
                  lff.get_degree(tiny_graph, salt_buckets=4).collect()}
        assert salted == plain

    def test_salted_degree_zero_buckets_matches_plain(self, tiny_graph):
        plain = {r["node"]: r["deg_sum"] for r in
                 lff.get_degree(tiny_graph).collect()}
        salted = {r["node"]: r["deg_sum"] for r in
                  lff.get_degree(tiny_graph, salt_buckets=0).collect()}
        assert salted == plain


@pytest.mark.graphframes
class TestSaltedEdgeAggregations:
    """Las variantes con sal en dos etapas producen los mismos resultados
    que el groupBy directo (y soportan el skew de supernodos)."""

    @staticmethod
    def _rows(df):
        return {r["id"]: {k: v for k, v in r.asDict().items() if k != "id"}
            for r in df.collect()}

    @staticmethod
    def _assert_same_rows(salted_df, plain_df):
        salted = TestSaltedEdgeAggregations._rows(salted_df)
        plain = TestSaltedEdgeAggregations._rows(plain_df)
        assert salted.keys() == plain.keys()
        for node, expected in plain.items():
            for key, value in expected.items():
                actual = salted[node][key]
                if isinstance(value, float):
                    assert actual == pytest.approx(value, rel=1e-6)
                else:
                    assert actual == value

    def test_degrees_salted(self, tiny_graph):
        self._assert_same_rows(
            lff.degrees(tiny_graph, salt_buckets=4),
            lff.degrees(tiny_graph, salt_buckets=0))

    def test_degree_balance_salted(self, tiny_graph):
        self._assert_same_rows(
            lff.degree_balance(tiny_graph, salt_buckets=4),
            lff.degree_balance(tiny_graph, salt_buckets=0))

    def test_reciprocity_salted(self, tiny_graph):
        self._assert_same_rows(
            lff.reciprocity(tiny_graph, salt_buckets=4),
            lff.reciprocity(tiny_graph, salt_buckets=0))

    def test_self_loops_salted(self, tiny_graph):
        self._assert_same_rows(
            lff.self_loops(tiny_graph, salt_buckets=4),
            lff.self_loops(tiny_graph, salt_buckets=0))

    def test_weighted_degrees_salted(self, tiny_graph):
        self._assert_same_rows(
            lff.weighted_degrees(tiny_graph, salt_buckets=4),
            lff.weighted_degrees(tiny_graph, salt_buckets=0))

    def test_weighted_degree_balance_salted(self, tiny_graph):
        self._assert_same_rows(
            lff.weighted_degree_balance(tiny_graph, salt_buckets=4),
            lff.weighted_degree_balance(tiny_graph, salt_buckets=0))

    def test_weighted_edge_stats_salted(self, tiny_graph):
        """Mezcla de combinables (incl. ratios de momentos curt/skew/so) y
        no combinables (countDistinct) con la misma salida."""
        self._assert_same_rows(
            lff.weighted_edge_stats(tiny_graph, salt_buckets=4),
            lff.weighted_edge_stats(tiny_graph, salt_buckets=0))

    def test_weighted_edge_stats_raw_aggregations_plain(self, tiny_graph):
        """Con `aggregations` crudas (sin nombres) siempre es vía directa."""
        from pyspark.sql.functions import sum as spark_sum
        result = lff.weighted_edge_stats(
            tiny_graph,
            aggregations=[spark_sum("weight").alias("total_w")],
            salt_buckets=8)
        rows = self._rows(result)
        assert rows["a"]["total_w"] == pytest.approx(14.0)

    def test_weighted_components_salted(self, tiny_graph, checkpoint_dir):
        self._assert_same_rows(
            lff.weighted_components(tiny_graph, salt_buckets=4),
            lff.weighted_components(tiny_graph, salt_buckets=0))
