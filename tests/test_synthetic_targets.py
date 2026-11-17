"""Tests de targets sintéticos (C11): reglas genéricas sobre features de nodo
que producen semillas `target_synthetic_<name>` propagables por el grafo.
"""
import os
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

import pytest

pytestmark = pytest.mark.spark

NODES = [(f"n{i}",) for i in range(1, 11)]                 # n1..n10
SCORES = [(f"n{i}", float(i)) for i in range(1, 11)]       # score = i


def _sources(df_factory):
    return {"pr": df_factory(SCORES, ["id", "pagerank"])}


def _nodes(df_factory):
    return df_factory(NODES, ["id"])


class TestAddSyntheticTargets:
    def test_top_quantile_binary(self, df_factory):
        from libs.functions.features import add_synthetic_targets
        rules = [{"name": "pr_top", "source": "pr", "column": "pagerank",
                  "rule": "top_quantile", "q": 0.9, "value": "binary"}]
        rows = {r["id"]: r["target_synthetic_pr_top"] for r in
                add_synthetic_targets(_nodes(df_factory), _sources(df_factory),
                    rules).collect()}
        assert rows["n10"] == 1.0
        assert rows["n1"] == 0.0
        # cuantil aproximado: marca el top ~10% (1-2 nodos de 10)
        assert 1.0 <= sum(rows.values()) <= 2.0

    def test_top_n_score_value(self, df_factory):
        from libs.functions.features import add_synthetic_targets
        rules = [{"name": "pr2", "source": "pr", "rule": "top_n", "n": 2,
                  "value": "score"}]       # sin "column": primera no-id
        rows = {r["id"]: r["target_synthetic_pr2"] for r in
                add_synthetic_targets(_nodes(df_factory), _sources(df_factory),
                    rules).collect()}
        assert rows["n10"] == pytest.approx(10.0)
        assert rows["n9"] == pytest.approx(9.0)
        assert rows["n8"] == 0.0

    def test_above_threshold(self, df_factory):
        from libs.functions.features import add_synthetic_targets
        rules = [{"name": "pr_hi", "source": "pr", "column": "pagerank",
                  "rule": "above_threshold", "threshold": 8.0}]
        rows = {r["id"]: r["target_synthetic_pr_hi"] for r in
                add_synthetic_targets(_nodes(df_factory), _sources(df_factory),
                    rules).collect()}
        assert sum(rows.values()) == 3.0    # n8, n9, n10

    def test_missing_source_raises(self, df_factory):
        from libs.functions.features import add_synthetic_targets
        rules = [{"name": "bad", "source": "nope", "rule": "top_n", "n": 1}]
        with pytest.raises(KeyError):
            add_synthetic_targets(_nodes(df_factory), _sources(df_factory),
                rules)

    def test_unknown_rule_raises(self, df_factory):
        from libs.functions.features import add_synthetic_targets
        rules = [{"name": "bad", "source": "pr", "rule": "sideways"}]
        with pytest.raises(ValueError):
            add_synthetic_targets(_nodes(df_factory), _sources(df_factory),
                rules)


class TestSyntheticConfig:
    def test_node_columns_include_synthetic(self):
        import config.features.ceps.target_propagation_features as cfcf
        columns = cfcf.propagation_node_columns()
        assert "target_lovelace" in columns
        assert "target_synthetic_pagerank_top" in columns
        # sin reglas sintéticas: solo lovelace (back-compat)
        assert cfcf.propagation_node_columns(synthetic_rules=[]) == \
            ["target_lovelace"]

    def test_contagion_feature_names(self):
        import config.features.ceps.target_propagation_features as cfcf
        assert cfcf._contagion_feature_name(
            "target_lovelace", "composed") == "contagion_composed"
        assert cfcf._contagion_feature_name(
            "target_lovelace_score", "composed") == "contagion_score_composed"
        assert cfcf._contagion_feature_name(
            "target_synthetic_pagerank_top", "composed") == \
            "contagion_synthetic_pagerank_top_composed"

    def test_synthetic_propagated_only_with_synthetic_weights(self):
        import config.features.ceps.target_propagation_features as cfcf
        synthetic = [list(f)[0] for f in cfcf.PROPAGATION_FEATURES
            if list(f.values())[0]["target_column"]
                .startswith("target_synthetic_")]
        expected = [f"contagion_synthetic_pagerank_top_{w}"
            for w in cfcf.SYNTHETIC_WEIGHT_TYPES]
        assert sorted(synthetic) == sorted(expected)
        # las semillas lovelace siguen propagándose con TODOS los pesos
        lovelace = [f for f in cfcf.PROPAGATION_FEATURES
            if list(f.values())[0]["target_column"] == "target_lovelace"]
        assert len(lovelace) == len(cfcf.WEIGHT_TYPES)


class TestSyntheticSubStep:
    def test_nodes_with_synthetic(self, spark, tmp_path, monkeypatch):
        from pipelines.features.ceps.target_propagation_features import (
            LovelaceCepsTargetPropagationSubStep)
        import config.features.ceps.graph_features as cfgf

        pagerank_path = tmp_path / "pagerank"
        spark.createDataFrame(SCORES, ["id", "pagerank"]).write.parquet(
            str(pagerank_path))
        monkeypatch.setattr(cfgf, "build", lambda variant="rfc":
            (None, {"pagerank": {"table_or_hdfs": str(pagerank_path)}}))

        step = object.__new__(LovelaceCepsTargetPropagationSubStep)
        step.sqlContext = spark
        step.__dict__["_nodes_join_target_cache"] = spark.createDataFrame(
            NODES, ["id"])
        result = step.nodes_with_synthetic
        assert "target_synthetic_pagerank_top" in result.columns
        rows = {r["id"]: r["target_synthetic_pagerank_top"]
                for r in result.collect()}
        assert sum(rows.values()) == 1.0     # top 1% de 10 nodos = 1 nodo
        assert rows["n10"] == 1.0

    def test_no_rules_passthrough(self, spark, monkeypatch):
        import config.features.ceps.target_propagation_features as cfcf
        from pipelines.features.ceps.target_propagation_features import (
            LovelaceCepsTargetPropagationSubStep)
        monkeypatch.setattr(cfcf, "SYNTHETIC_TARGETS", [])
        step = object.__new__(LovelaceCepsTargetPropagationSubStep)
        step.sqlContext = spark
        nodes = spark.createDataFrame(NODES, ["id"])
        step.__dict__["_nodes_join_target_cache"] = nodes
        assert step.nodes_with_synthetic is nodes
