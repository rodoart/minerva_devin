"""Tests para libs.functions.aggregations."""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql.functions import col

from libs.functions.aggregations import (
    RECENCY_MAX_COLUMN, RECENCY_WEIGHT_COLUMN,
    recency_auxiliary_columns, standard_group_by_features,
    select_group_by_features, resolve_group_by_expressions,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def txn_df(spark):
    """Transacciones de dos aristas con distinta antigüedad."""
    return spark.createDataFrame(
        [("a", "b", 10.0, 0),
         ("a", "b", 20.0, 10),
         ("a", "b", 30.0, 20),
         ("x", "y", 5.0, 20)],
        ["id_src", "id_dst", "oper_mto", "tfrom_days"],
    )


class TestRecencyAuxiliaryColumns:
    def test_returns_expected_keys(self, spark):
        aux = recency_auxiliary_columns()
        assert set(aux) == {RECENCY_MAX_COLUMN, RECENCY_WEIGHT_COLUMN}

    def test_weight_values(self, txn_df):
        # max tfrom_days = 20 -> peso = 21 - tfrom_days (más reciente pesa más)
        df = txn_df.withColumn(
            RECENCY_MAX_COLUMN,
            recency_auxiliary_columns()[RECENCY_MAX_COLUMN],
        ).withColumn(
            RECENCY_WEIGHT_COLUMN,
            recency_auxiliary_columns()[RECENCY_WEIGHT_COLUMN],
        )
        weights = {r["tfrom_days"]: r[RECENCY_WEIGHT_COLUMN]
                   for r in df.collect()}
        assert weights == {0: 21, 10: 11, 20: 1}
        assert all(r[RECENCY_MAX_COLUMN] == 20 for r in df.collect())

    def test_custom_column_names(self, txn_df):
        aux = recency_auxiliary_columns(
            tfrom_column="tfrom_days", max_column="_mx", weight_column="_w")
        df = txn_df.withColumn("_mx", aux["_mx"]).withColumn("_w", aux["_w"])
        assert "_mx" in df.columns and "_w" in df.columns


class TestStandardGroupByFeatures:
    def test_catalog_keys(self):
        features = standard_group_by_features()
        expected = {"min", "max", "mean", "weighted_mean", "weighted_sum",
                    "std", "count", "weighted_count", "countDistinct", "sum",
                    "last", "first", "curt", "skew", "so"}
        assert set(features) == expected

    def test_weighted_mean_uses_recency(self, txn_df):
        features = standard_group_by_features()
        df = (txn_df
            .withColumn(RECENCY_MAX_COLUMN,
                        recency_auxiliary_columns()[RECENCY_MAX_COLUMN])
            .withColumn(RECENCY_WEIGHT_COLUMN,
                        recency_auxiliary_columns()[RECENCY_WEIGHT_COLUMN])
            .groupBy("id_src", "id_dst")
            .agg(features["weighted_mean"]("oper_mto").alias("wm"))
            .orderBy("id_src"))
        result = df.collect()
        # arista a->b: (10*21 + 20*11 + 30*1) / (21+11+1) = 460/33
        assert result[0]["wm"] == pytest.approx(460 / 33)
        # arista x->y: un solo valor -> 5.0
        assert result[1]["wm"] == pytest.approx(5.0)

    def test_custom_weight_column(self, txn_df):
        features = standard_group_by_features(weight_column="w2")
        df = (txn_df.withColumn("w2", col("tfrom_days") + 1)
            .groupBy("id_src", "id_dst")
            .agg(features["weighted_sum"]("oper_mto").alias("ws"))
            .orderBy("id_src"))
        # arista a->b: 10*1 + 20*11 + 30*21 = 860
        assert df.collect()[0]["ws"] == pytest.approx(860.0)


class TestSelectGroupByFeatures:
    def test_subset(self):
        features = select_group_by_features(["min", "max", "mean"])
        assert set(features) == {"min", "max", "mean"}

    def test_empty(self):
        assert select_group_by_features([]) == {}

    def test_unknown_raises_key_error(self):
        with pytest.raises(KeyError):
            select_group_by_features(["mean", "no_existe"])


class TestResolveGroupByExpressions:
    def test_resolves_and_aliases(self):
        features = select_group_by_features(["min", "mean"])
        exprs = resolve_group_by_expressions(
            ["min_oper_mto", "mean_tfrom_days"], features)
        assert len(exprs) == 2

    def test_longest_prefix_wins(self, txn_df):
        # "weighted_mean_oper_mto" debe resolver "weighted_mean" y no "mean"
        features = select_group_by_features(["mean", "weighted_mean"])
        exprs = resolve_group_by_expressions(
            ["weighted_mean_oper_mto"], features)
        df = (txn_df
            .withColumn(RECENCY_MAX_COLUMN,
                        recency_auxiliary_columns()[RECENCY_MAX_COLUMN])
            .withColumn(RECENCY_WEIGHT_COLUMN,
                        recency_auxiliary_columns()[RECENCY_WEIGHT_COLUMN])
            .groupBy("id_src", "id_dst")
            .agg(*exprs)
            .orderBy("id_src"))
        row = df.collect()[0]
        assert "weighted_mean_oper_mto" in row.asDict()
        assert row["weighted_mean_oper_mto"] == pytest.approx(460 / 33)

    def test_unknown_aggregation_raises(self):
        features = select_group_by_features(["mean"])
        with pytest.raises(ValueError, match="Unknown txn aggregation"):
            resolve_group_by_expressions(["mediana_oper_mto"], features)
