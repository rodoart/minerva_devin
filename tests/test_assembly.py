"""Tests para libs.functions.assembly."""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql.functions import col, lit

from libs.functions.assembly import (
    weighted_mean, distinct_count, ASSEMBLY_AGGREGATION_FUNCTIONS,
    build_aggregation_expressions, explode_array_column,
    get_feature_columns, assemble_vector,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def exploded_df(spark):
    """Simula nodos explotados a numcliente con peso."""
    return spark.createDataFrame(
        [
            # numcliente, feature, weight
            ("c1", 10.0, 1.0),
            ("c1", 20.0, 3.0),
            ("c2", 5.0, 1.0),
            ("c2", None, 1.0),
        ],
        ["numcliente", "feat", "w"],
    )


class TestWeightedMean:
    def test_math(self, exploded_df):
        result = (exploded_df.groupBy("numcliente")
            .agg(weighted_mean("feat", col("w"))).collect())
        values = {r["numcliente"]: r["feat"] for r in result}
        # c1: (10*1 + 20*3)/(1+3) = 70/4 = 17.5
        assert values["c1"] == pytest.approx(17.5)
        # c2: (5*1 + null*1)/(1+1) = 5/2 = 2.5  (el nulo no aporta al numerador
        # pero sí al denominador)
        assert values["c2"] == pytest.approx(2.5)

    def test_alias_preserved(self, exploded_df):
        result = exploded_df.groupBy("numcliente").agg(
            weighted_mean("feat", col("w")))
        assert "feat" in result.columns


class TestDistinctCount:
    def test_counts_distinct(self, exploded_df):
        result = exploded_df.groupBy("numcliente").agg(
            distinct_count("feat", col("w"))).collect()
        values = {r["numcliente"]: r["distinct_feat"] for r in result}
        assert values["c1"] == 2
        assert values["c2"] == 1   # null no cuenta


class TestAggregationRegistry:
    def test_expected_keys(self):
        assert set(ASSEMBLY_AGGREGATION_FUNCTIONS.keys()) == {
            "mean", "min", "max", "sum", "first",
            "distinct_count", "weighted_mean"}


class TestBuildAggregationExpressions:
    def test_string_aggregation_applies_to_all(self, exploded_df):
        exprs = build_aggregation_expressions(["feat"], "sum")
        result = exploded_df.groupBy("numcliente").agg(*exprs).collect()
        values = {r["numcliente"]: r["feat"] for r in result}
        assert values["c1"] == 30.0
        assert values["c2"] == 5.0

    def test_default_weight_is_one(self, exploded_df):
        # weighted_mean con peso 1 -> media simple
        exprs = build_aggregation_expressions(["feat"], "weighted_mean")
        result = exploded_df.groupBy("numcliente").agg(*exprs).collect()
        values = {r["numcliente"]: r["feat"] for r in result}
        assert values["c1"] == pytest.approx(15.0)

    def test_dict_with_default_and_override(self, exploded_df):
        df = exploded_df.withColumn("other", lit(100.0))
        exprs = build_aggregation_expressions(
            ["feat", "other"],
            {"default": "sum", "other": "max"},
            weight=col("w"),
        )
        result = df.groupBy("numcliente").agg(*exprs).collect()
        row = [r for r in result if r["numcliente"] == "c1"][0]
        assert row["feat"] == 30.0
        assert row["other"] == 100.0

    def test_unknown_aggregation_raises(self):
        with pytest.raises(ValueError, match="Unsupported assembly aggregation"):
            build_aggregation_expressions(["feat"], "median")

    def test_unknown_in_dict_raises(self):
        with pytest.raises(ValueError, match="Unsupported assembly aggregation"):
            build_aggregation_expressions(
                ["feat"], {"default": "sum", "feat": "median"})


class TestExplodeArrayColumn:
    def test_explodes(self, spark):
        df = spark.createDataFrame(
            [("n1", ["a", "b", "c"]), ("n2", ["d"])], ["id", "arr"])
        result = explode_array_column(df, "arr")
        assert result.count() == 4
        assert set(result.columns) == {"id", "arr"}

    def test_explode_into_new_column(self, spark):
        df = spark.createDataFrame([("n1", ["a", "b"])], ["id", "arr"])
        result = explode_array_column(df, "arr", explode_into="elem")
        assert "elem" in result.columns
        assert result.filter("id = 'n1'").count() == 2

    def test_null_array_produces_no_rows(self, spark):
        df = spark.createDataFrame(
            [("n1", None), ("n2", ["a"])], ["id", "arr"])
        result = explode_array_column(df, "arr")
        assert result.count() == 1


class TestGetFeatureColumns:
    def test_excludes_columns(self, spark):
        df = spark.createDataFrame([(1, 2.0, "x")], ["id", "feat", "meta"])
        assert get_feature_columns(df, exclude_columns=["id", "meta"]) == ["feat"]

    def test_excludes_prefixes(self, spark):
        df = spark.createDataFrame(
            [(1, 0.5, 0.9)], ["id", "feat", "target_lovelace"])
        assert get_feature_columns(
            df, exclude_columns=["id"], exclude_prefixes=["target_"]) == ["feat"]

    def test_no_exclusions(self, spark):
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        assert get_feature_columns(df) == ["a", "b"]

    def test_empty_df_columns(self, spark):
        df = spark.createDataFrame([], "id STRING")
        assert get_feature_columns(df, exclude_columns=["id"]) == []


class TestAssembleVector:
    def test_vector_column_created(self, spark):
        from pyspark.ml.linalg import DenseVector
        df = spark.createDataFrame(
            [(1.0, 2.0, "a"), (3.0, 4.0, "b")], ["f1", "f2", "id"])
        result = assemble_vector(df, ["f1", "f2"], output_column="features")
        assert "features" in result.columns
        row = result.filter("id = 'a'").first()
        assert row["features"] == DenseVector([1.0, 2.0])

    def test_handle_invalid_keep_nulls_as_nan(self, spark):
        import math
        df = spark.createDataFrame(
            [(1.0, None), (3.0, 4.0)], ["f1", "f2"])
        result = assemble_vector(df, ["f1", "f2"], handle_invalid="keep")
        vec = result.collect()[0]["features"]
        assert math.isnan(vec[1])

    def test_custom_output_column(self, spark):
        df = spark.createDataFrame([(1.0, 2.0)], ["f1", "f2"])
        result = assemble_vector(df, ["f1", "f2"], output_column="vec")
        assert "vec" in result.columns
        assert "features" not in result.columns
