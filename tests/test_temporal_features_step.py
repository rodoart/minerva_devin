"""Tests del step de temporal features CEPS (nodo + par).

La lógica de cálculo se cubre en test_temporal_features.py; aquí se prueban
la config namespaced por variante, la orquestación del substep y el wiring
de las fuentes hacia el assembler y el clustering.
"""
import os
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

from datetime import datetime

import pytest

pytestmark = pytest.mark.spark

TXNS = [
    ("a", "b", datetime(2025, 8, 31, 3, 0), 100.0, 0),
    ("a", "c", datetime(2025, 8, 30, 14, 0), 200.0, 1),
    ("b", "c", datetime(2025, 8, 29, 12, 0), 50.0, 2),
    ("c", "x", datetime(2025, 8, 20, 12, 0), 9000.0, 11),
    ("x", "d", datetime(2025, 8, 20, 13, 0), 8500.0, 11),
]


class TestTemporalConfigBuild:
    def test_rfc_keeps_historical_layout(self):
        import config.features.ceps.temporal_features as ctf
        tf_input, tf_output = ctf.build("rfc")
        assert "temporal_node_features" in tf_output
        assert "temporal_pair_features" in tf_output
        assert "temporal_pair_node" in tf_output
        assert "ceps/temporal_features/temporal_node_features" in str(
            tf_output["temporal_node_features"]["table_or_hdfs"])
        assert "missing_treatment" in tf_input

    def test_cta_is_namespaced(self):
        import config.features.ceps.temporal_features as ctf
        tf_input, tf_output = ctf.build("cta")
        assert "ceps/cta/temporal_features/temporal_node_features" in str(
            tf_output["temporal_node_features"]["table_or_hdfs"])
        # el insumo missing_treatment es el de la variante cta
        assert "cta" in str(tf_input["missing_treatment"]["table_or_hdfs"])

    def test_outputs_are_kept(self):
        import config.features.ceps.temporal_features as ctf
        _, tf_output = ctf.build("rfc")
        for key in ("temporal_node_features", "temporal_pair_features",
                    "temporal_pair_node"):
            assert tf_output[key]["keep_or_delete"] == "keep"


def _bare_temporal_substep(spark, tmp_path):
    """Substep mínimo con las transacciones normalizadas precargadas."""
    from pipelines.features.ceps.temporal_features import (
        CepsTemporalFeaturesSubStep)
    step = object.__new__(CepsTemporalFeaturesSubStep)
    step.sqlContext = spark
    step.is_dynamic = True
    step.tmp_paths = []
    step._decorated_cache = {}
    step.output_hive = {
        key: {"table_or_hdfs": str(tmp_path / key), "keep_or_delete": "delete"}
        for key in ("temporal_node_features", "temporal_pair_features",
                    "temporal_pair_node")
    }
    step.__dict__["_txns_cache"] = spark.createDataFrame(
        TXNS, ["src", "dst", "ts", "oper_mto", "tfrom_days"])
    return step


class TestTemporalSubStep:
    def test_node_features(self, spark, tmp_path):
        step = _bare_temporal_substep(spark, tmp_path)
        result = step.temporal_node_features
        assert "tmp_txn_count" in result.columns
        assert "tmp_burstiness" in result.columns
        rows = {r["id"]: r.asDict() for r in result.collect()}
        assert rows["a"]["tmp_txn_count"] == 2

    def test_pair_features_and_node_aggregate(self, spark, tmp_path):
        step = _bare_temporal_substep(spark, tmp_path)
        pairs = {(r["a"], r["x"]) for r in step.temporal_pair_features.collect()}
        assert ("a", "b") in pairs and ("c", "x") in pairs
        node = step.temporal_pair_node
        assert "tmp_amt_zscore_ax_max" in node.columns
        # solo receptores (x) aparecen: b, c, x, d
        assert {r["id"] for r in node.collect()} == {"b", "c", "x", "d"}


class TestTemporalStepWiring:
    def test_variant_param_and_prefix(self):
        from pipelines.features.ceps.temporal_features import (
            CepsTemporalFeaturesStep)
        import inspect
        assert "variant" in inspect.signature(
            CepsTemporalFeaturesStep.__init__).parameters
        assert CepsTemporalFeaturesStep.STEP_NAME_PREFIX == \
            "Standard_Temporal_Features_Step"

    def test_vector_assembler_sources_include_temporal(self):
        import config.features.ceps.vector_assembler as cvas
        for variant in ("rfc", "cta"):
            sources = cvas.build_feature_sources(variant)
            assert sources["temporal_node_features"]["mode"] == "simple"
            assert sources["temporal_pair_node"]["mode"] == "simple"
            if variant == "cta":
                assert "cta" in str(
                    sources["temporal_node_features"]["path"])

    def test_cluster_sources_include_temporal(self):
        import config.features.ceps.cluster_features as cclf
        sources = cclf.build_graph_feature_sources("cta")
        assert "temporal_node_features" in sources
        assert "temporal_pair_node" in sources
