"""Tests para libs.functions.weights."""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql.functions import col

from libs.functions.weights import (
    column_weight, composed_weight, ratio_weight,
    build_weights_map, get_weight,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def edges_df(spark):
    return spark.createDataFrame(
        [("a", "b", 10.0, 2.0, 0.0), ("b", "c", 20.0, 4.0, 5.0)],
        ["src", "dst", "oper_mto", "count_txn", "zero"],
    )


class TestColumnWeight:
    def test_returns_column_values(self, edges_df):
        result = edges_df.select(column_weight("oper_mto").alias("w")).collect()
        assert [r["w"] for r in result] == [10.0, 20.0]

    def test_column_object(self, edges_df):
        w = column_weight("oper_mto")
        df = edges_df.select(w.alias("w"))
        assert df.columns == ["w"]


class TestComposedWeight:
    def test_averages_columns(self, edges_df):
        result = edges_df.select(
            composed_weight("oper_mto", "count_txn").alias("w")).collect()
        # (10+2)/2 = 6 ; (20+4)/2 = 12
        assert [r["w"] for r in result] == [6.0, 12.0]

    def test_single_column_is_identity(self, edges_df):
        result = edges_df.select(
            composed_weight("oper_mto").alias("w")).collect()
        assert [r["w"] for r in result] == [10.0, 20.0]


class TestRatioWeight:
    def test_ratio(self, edges_df):
        result = edges_df.select(
            ratio_weight("oper_mto", "count_txn").alias("w")).collect()
        assert [r["w"] for r in result] == [5.0, 5.0]

    def test_zero_denominator_gives_zero(self, edges_df):
        result = edges_df.select(
            ratio_weight("oper_mto", "zero").alias("w")).collect()
        assert [r["w"] for r in result] == [0.0, 0.0]

    def test_negative_denominator_gives_zero(self, spark):
        df = spark.createDataFrame([(10.0, -2.0)], ["num", "den"])
        result = df.select(ratio_weight("num", "den").alias("w")).collect()
        assert result[0]["w"] == 0.0


class TestBuildWeightsMap:
    def test_map_column_created(self, edges_df):
        df = edges_df.withColumn(
            "weights", build_weights_map({
                "monto": column_weight("oper_mto"),
                "cnt": column_weight("count_txn"),
            }))
        assert "weights" in df.columns
        row = df.first()
        assert row["weights"]["monto"] == 10.0
        assert row["weights"]["cnt"] == 2.0

    def test_custom_alias(self, edges_df):
        df = edges_df.select(
            build_weights_map({"a": column_weight("oper_mto")}, alias="wmap"))
        assert df.columns == ["wmap"]

    def test_map_with_mixed_weights(self, edges_df):
        df = edges_df.withColumn("weights", build_weights_map({
            "direct": column_weight("oper_mto"),
            "avg": composed_weight("oper_mto", "count_txn"),
            "ratio": ratio_weight("oper_mto", "count_txn"),
        }))
        w = df.first()["weights"]
        assert w["direct"] == 10.0
        assert w["avg"] == 6.0
        assert w["ratio"] == 5.0


class TestGetWeight:
    def test_extracts_from_map(self, edges_df):
        df = (edges_df
            .withColumn("weights", build_weights_map(
                {"monto": column_weight("oper_mto")}))
            .withColumn("w", get_weight("weights", "monto")))
        assert df.first()["w"] == 10.0

    def test_accepts_column_object(self, edges_df):
        df = (edges_df
            .withColumn("weights", build_weights_map(
                {"monto": column_weight("oper_mto")}))
            .withColumn("w", get_weight(col("weights"), "monto")))
        assert df.first()["w"] == 10.0

    def test_missing_key_returns_null(self, edges_df):
        df = (edges_df
            .withColumn("weights", build_weights_map(
                {"monto": column_weight("oper_mto")}))
            .withColumn("w", get_weight("weights", "inexistente")))
        assert df.first()["w"] is None
