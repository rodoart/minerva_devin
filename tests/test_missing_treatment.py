"""Tests para libs.functions.missing_treatment."""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql.functions import lit

from libs.functions.missing_treatment import (
    fill_missings_with_value, fill_missings_with_mean,
    fill_missings_with_mean_without_ignoring_null_counts,
    MISSING_TREATMENT_FUNCTION_RELATIONS, apply_missing_treatment,
)

pytestmark = pytest.mark.spark


@pytest.fixture()
def df_with_nulls(spark):
    return spark.createDataFrame(
        [(1, 10.0, "a"), (2, None, "b"), (3, 20.0, "c"), (4, None, "d")],
        ["id", "value", "label"],
    )


class TestFillMissingsWithValue:
    def test_fills_nulls(self, df_with_nulls):
        result = fill_missings_with_value(df_with_nulls, ["value"], 0.0)
        assert result.filter("value = 0.0").count() == 2

    def test_non_target_columns_untouched(self, df_with_nulls):
        result = fill_missings_with_value(df_with_nulls, ["value"], 0.0)
        assert result.columns == ["id", "value", "label"]
        assert result.filter("id = 2").first()["label"] == "b"

    def test_fill_string_column(self, spark):
        df = spark.createDataFrame([(1, None), (2, "x")], ["id", "txt"])
        result = fill_missings_with_value(df, ["txt"], "unknown")
        assert result.filter("txt = 'unknown'").count() == 1

    def test_no_nulls_noop(self, spark):
        df = spark.createDataFrame([(1, 5.0), (2, 6.0)], ["id", "value"])
        result = fill_missings_with_value(df, ["value"], 0.0)
        assert result.collect() == df.collect()


class TestFillMissingsWithMean:
    def test_fills_with_mean_of_non_nulls(self, df_with_nulls):
        # media de [10, 20] = 15
        result = fill_missings_with_mean(df_with_nulls, ["value"])
        nulls_filled = result.filter("value = 15.0").count()
        assert nulls_filled == 2

    def test_keeps_original_values(self, df_with_nulls):
        result = fill_missings_with_mean(df_with_nulls, ["value"])
        values = sorted(r["value"] for r in result.collect())
        assert values == [10.0, 15.0, 15.0, 20.0]

    def test_mean_with_nulls_counts_nulls_as_zero(self, df_with_nulls):
        # media de [10, 0, 20, 0] = 7.5
        result = fill_missings_with_mean_without_ignoring_null_counts(
            df_with_nulls, ["value"])
        assert result.filter("value = 7.5").count() == 2

    def test_mean_vs_mean_with_nulls_differ(self, df_with_nulls):
        r1 = fill_missings_with_mean(df_with_nulls, ["value"])
        r2 = fill_missings_with_mean_without_ignoring_null_counts(
            df_with_nulls, ["value"])
        v1 = sorted(r["value"] for r in r1.collect())
        v2 = sorted(r["value"] for r in r2.collect())
        assert v1 != v2


class TestRegistry:
    def test_expected_keys(self):
        assert set(MISSING_TREATMENT_FUNCTION_RELATIONS.keys()) == {
            "mean", "mean_with_nulls", "value"}

    def test_values_are_callable(self):
        for fn in MISSING_TREATMENT_FUNCTION_RELATIONS.values():
            assert callable(fn)


class TestApplyMissingTreatment:
    def test_string_treatment(self, df_with_nulls):
        result = apply_missing_treatment(df_with_nulls, {"value": "mean"})
        assert result.filter("value = 15.0").count() == 2

    def test_single_item_list_treatment(self, df_with_nulls):
        result = apply_missing_treatment(df_with_nulls, {"value": ["mean_with_nulls"]})
        assert result.filter("value = 7.5").count() == 2

    def test_list_with_args(self, df_with_nulls):
        result = apply_missing_treatment(df_with_nulls, {"value": ["value", -1.0]})
        assert result.filter("value = -1.0").count() == 2

    def test_callable_treatment(self, df_with_nulls):
        result = apply_missing_treatment(
            df_with_nulls,
            {"value": lambda df: df.withColumn("value", lit(999.0))})
        assert result.filter("value = 999.0").count() == 4

    def test_multiple_columns(self, spark):
        df = spark.createDataFrame(
            [(1, None, None), (2, 5.0, "x")], ["id", "num", "txt"])
        result = apply_missing_treatment(
            df, {"num": ["value", 0.0], "txt": ["value", "N/A"]})
        row = result.filter("id = 1").first()
        assert row["num"] == 0.0
        assert row["txt"] == "N/A"

    def test_unknown_string_raises(self, df_with_nulls):
        with pytest.raises(ValueError, match="Unsupported missing treatment"):
            apply_missing_treatment(df_with_nulls, {"value": "median"})

    def test_unknown_list_type_raises(self, df_with_nulls):
        with pytest.raises(ValueError, match="Unsupported missing treatment"):
            apply_missing_treatment(df_with_nulls, {"value": ["median", 1]})

    def test_invalid_type_raises(self, df_with_nulls):
        with pytest.raises(ValueError, match="Invalid treatment type"):
            apply_missing_treatment(df_with_nulls, {"value": 42})

    def test_empty_dict_returns_same(self, df_with_nulls):
        result = apply_missing_treatment(df_with_nulls, {})
        assert result.collect() == df_with_nulls.collect()
