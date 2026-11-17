"""Tests del step de risk features CEPS (nodo + par).

Cubre la config namespaced por variante y la sub-etapa con DataFrames
inyectados en caché (mismo patrón que test_cluster_features).
"""
import os
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

import pytest

pytestmark = pytest.mark.spark


###############################################################################
# CONFIG
###############################################################################

class TestRiskConfigBuild:
    def test_rfc_keeps_historical_layout(self):
        import config.features.ceps.risk_features as crf
        rf_input, rf_output = crf.build("rfc")
        assert "risk_node_features" in rf_output
        assert "pair_features" in rf_output
        assert "pair_features_node" in rf_output
        # la variante por defecto NO inserta segmento de variante
        assert "ceps/risk_features/risk_node_features" in str(
            rf_output["risk_node_features"]["table_or_hdfs"])

    def test_cta_is_namespaced(self):
        import config.features.ceps.risk_features as crf
        rf_input, rf_output = crf.build("cta")
        assert "ceps/cta/risk_features/risk_node_features" in str(
            rf_output["risk_node_features"]["table_or_hdfs"])
        # los insumos (edges, nodes_join_target) vienen de la variante cta
        assert "cta" in str(rf_input["edges"]["table_or_hdfs"])

    def test_checkpoint_is_delete(self):
        import config.features.ceps.risk_features as crf
        _, rf_output = crf.build("rfc")
        assert rf_output["checkpoint"]["keep_or_delete"] == "delete"
        assert rf_output["risk_node_features"]["keep_or_delete"] == "keep"


###############################################################################
# SUBSTEP
###############################################################################

EDGES = [
    ("a", "b", 3), ("a", "c", 1), ("b", "c", 1), ("c", "a", 1),
    ("c", "x", 2), ("x", "d", 1), ("d", "x", 1),
]
NODES = [  # b es fraude duro; contagio compuesto en a y b
    ("a", 0.0, 0.6), ("b", 1.0, 0.9), ("c", 0.0, 0.2),
    ("x", 0.0, 0.1), ("d", 0.0, 0.0),
]


def _bare_risk_substep(spark, tmp_path, monkeypatch):
    """Substep mínimo con nodos/aristas/contagio precargados en caché."""
    from pipelines.features.ceps.risk_features import CepsRiskFeaturesSubStep
    step = object.__new__(CepsRiskFeaturesSubStep)
    step.sqlContext = spark
    step.is_dynamic = True
    step.tmp_paths = []
    step._decorated_cache = {}
    step.output_hive = {
        key: {"table_or_hdfs": str(tmp_path / key), "keep_or_delete": "delete"}
        for key in ("risk_node_features", "pair_features",
                    "pair_features_node", "checkpoint")
    }
    step.__dict__["_nodes_join_target_cache"] = spark.createDataFrame(
        NODES, ["id", "target_lovelace", "contagion_composed"])
    step.__dict__["_edges_cache"] = spark.createDataFrame(
        EDGES, ["src", "dst", "count_txn"])
    step.__dict__["_contagion_df_cache"] = spark.createDataFrame(
        [(i, c) for i, _t, c in NODES], ["id", "contagion_composed"])
    # checkpoint real bajo tmp_path; define_checkpoint del substep a no-op
    spark.sparkContext.setCheckpointDir(str(tmp_path / "checkpoint"))
    monkeypatch.setattr(step, "define_checkpoint", lambda checkpoint_hdfs: None)
    return step


class TestSeedFrames:
    def test_hard_and_contagion_seeds(self, spark, tmp_path, monkeypatch):
        step = _bare_risk_substep(spark, tmp_path, monkeypatch)
        seeds = step.seed_frames
        assert set(seeds) == {"target_lovelace", "contagion_composed"}
        hard = {r["id"]: r["seed_score"] for r in
                seeds["target_lovelace"].collect()}
        assert hard["b"] == 1.0 and hard["a"] == 0.0

    def test_seed_ids_thresholds(self, spark, tmp_path, monkeypatch):
        step = _bare_risk_substep(spark, tmp_path, monkeypatch)
        hard_ids = step._seed_ids("target_lovelace",
            step.seed_frames["target_lovelace"])
        assert hard_ids == ["b"]
        # contagio con umbral 0.5: a (0.6) y b (0.9)
        soft_ids = sorted(step._seed_ids("contagion_composed",
            step.seed_frames["contagion_composed"]))
        assert soft_ids == ["a", "b"]


@pytest.mark.graphframes
class TestRiskNodeFeatures:
    def test_columns_and_values(self, spark, tmp_path, monkeypatch):
        step = _bare_risk_substep(spark, tmp_path, monkeypatch)
        result = step.risk_node_features
        rows = {r["id"]: r.asDict() for r in result.collect()}
        assert set(rows) == {"a", "b", "c", "x", "d"}
        # columnas de las familias con sufijo de semilla dura
        for column in ("risk_in_sum_target_lovelace",
                       "risk_out_frac_target_lovelace",
                       "risk_2hop_target_lovelace",
                       "risk_outflow_target_lovelace",
                       "risk_dist_to_target_lovelace",
                       "risk_dist_from_target_lovelace",
                       "risk_ppr_max_target_lovelace"):
            assert column in result.columns, column
        # mismas familias con sufijo de contagio
        assert "risk_ppr_mean_contagion_composed" in result.columns
        # c recibe de b (fraude) y a: frac_in = 0.5
        assert rows["c"]["risk_in_frac_target_lovelace"] == pytest.approx(0.5)
        # a emite 4 txn, 3 hacia b -> outflow 0.75
        assert rows["a"]["risk_outflow_target_lovelace"] == pytest.approx(0.75)
        # distancia desde el fraude: b=0, c=1, a=2, x=2, d=3
        assert rows["x"]["risk_dist_from_target_lovelace"] == 2
        assert rows["d"]["risk_dist_from_target_lovelace"] == 3


@pytest.mark.graphframes
class TestPairFeaturesStep:
    def test_candidates_from_hard_seed(self, spark, tmp_path, monkeypatch):
        step = _bare_risk_substep(spark, tmp_path, monkeypatch)
        pairs = {(r["a"], r["x"]): r["hops"]
                 for r in step.pair_candidates.collect()}
        # b semilla: vecinos directos {c} y a 2 saltos {a, x} (b->c->a/x)
        assert pairs == {("b", "c"): 1, ("b", "a"): 2, ("b", "x"): 2}

    def test_pair_features_values(self, spark, tmp_path, monkeypatch):
        step = _bare_risk_substep(spark, tmp_path, monkeypatch)
        rows = {(r["a"], r["x"]): r.asDict()
                for r in step.pair_features.collect()}
        bx = rows[("b", "x")]
        # N(b)={a,c}, N(x)={c,d}: CN=1; distancia b->x = 2
        assert bx["pair_common_neighbors"] == 1
        assert bx["pair_min_distance"] == 2
        # b->c->x: 1 camino de longitud 2
        assert bx["pair_paths_len2"] == 1

    def test_pair_features_node_aggregates_x(self, spark, tmp_path, monkeypatch):
        step = _bare_risk_substep(spark, tmp_path, monkeypatch)
        rows = {r["id"]: r.asDict()
                for r in step.pair_features_node.collect()}
        # solo a, c, x son X de algún par
        assert set(rows) == {"a", "c", "x"}
        assert "pair_jaccard_max" in rows["x"]
        assert "pair_min_distance_mean" in rows["x"]


class TestStepWiring:
    def test_variant_suffixes_step_name(self):
        from pipelines.features.ceps.risk_features import CepsRiskFeaturesStep
        import inspect
        assert "variant" in inspect.signature(
            CepsRiskFeaturesStep.__init__).parameters
        assert CepsRiskFeaturesStep.STEP_NAME_PREFIX == \
            "Standard_Risk_Features_Step"

    def test_vector_assembler_sources_include_risk(self):
        import config.features.ceps.vector_assembler as cvas
        for variant in ("rfc", "cta"):
            sources = cvas.build_feature_sources(variant)
            assert sources["risk_node_features"]["mode"] == "simple"
            assert sources["pair_features_node"]["mode"] == "simple"
            if variant == "cta":
                assert "cta" in str(
                    sources["risk_node_features"]["path"])

    def test_cluster_sources_include_risk(self):
        import config.features.ceps.cluster_features as cclf
        sources = cclf.build_graph_feature_sources("cta")
        assert "risk_node_features" in sources
        assert "pair_features_node" in sources
