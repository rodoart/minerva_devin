"""Tests para libs.data_engineering_toolbox.pyspark.compare."""
import pytest

pytest.importorskip("pyspark")

from libs.data_engineering_toolbox.pyspark.compare import (
    DifferenceDataFrame, rename_columns, reverse_rename_columns,
)

pytestmark = pytest.mark.spark


class TestRenameHelpers:
    def test_rename_columns(self, spark):
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        result = rename_columns(df, {"a": "x", "b": "y"})
        assert result.columns == ["x", "y"]

    def test_reverse_rename_columns(self, spark):
        df = spark.createDataFrame([(1, 2)], ["a_df1", "b_df1"])
        result = reverse_rename_columns(df, {"a": "a_df1", "b": "b_df1"})
        assert result.columns == ["a", "b"]


class TestTypeCheckHelpers:
    def test_isinstance_column_string_type(self, spark):
        df = spark.createDataFrame([(1, "x", 1.5)], ["num", "txt", "flt"])
        assert DifferenceDataFrame.isinstance_column_string_type(df, "num", "int")
        assert DifferenceDataFrame.isinstance_column_string_type(df, "txt", "string")
        assert DifferenceDataFrame.isinstance_column_string_type(df, "flt", ["int", "double"])
        assert not DifferenceDataFrame.isinstance_column_string_type(df, "num", "string")

    def test_invalid_type_raises(self, spark):
        df = spark.createDataFrame([(1,)], ["n"])
        with pytest.raises(AssertionError, match="Invalid type"):
            DifferenceDataFrame._isinstance_column_one_string_type(df, "n", "bogus")

    def test_rn_suffixes(self):
        assert DifferenceDataFrame.rn("col", "df1") == "col_df1"
        assert DifferenceDataFrame.rn("col", "_x") == "col_x"
        assert DifferenceDataFrame.rn1("a") == "a_df1"
        assert DifferenceDataFrame.rn2("a") == "a_df2"


class TestDifferenceDataFrame:
    def test_identical_frames_no_difference(self, spark):
        df1 = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["id", "v"])
        diff = DifferenceDataFrame(df1, df2, "id", tol=0.01)
        assert diff.difference.count() == 0

    def test_difference_detected(self, spark):
        df1 = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.0), (2, 99.0)], ["id", "v"])
        diff = DifferenceDataFrame(df1, df2, "id", tol=0.01)
        result = diff.difference.collect()
        assert len(result) == 1
        assert result[0]["id"] == 2
        assert result[0]["v_df1"] == 20.0
        assert result[0]["v_df2"] == 99.0
        assert result[0]["v_diff"] == pytest.approx(79.0)

    def test_within_tolerance_not_different(self, spark):
        df1 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.005)], ["id", "v"])
        diff = DifferenceDataFrame(df1, df2, "id", tol=0.01)
        assert diff.difference.count() == 0

    def test_missing_row_is_different(self, spark):
        df1 = spark.createDataFrame([(1, 10.0), (2, 20.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        diff = DifferenceDataFrame(df1, df2, "id", tol=0.01)
        assert diff.difference.count() == 1
        assert diff.difference.first()["id"] == 2

    def test_joined_column_layout(self, spark):
        df1 = spark.createDataFrame([(1, 10.0, "a")], ["id", "v", "s"])
        df2 = spark.createDataFrame([(1, 10.0, "a")], ["id", "v", "s"])
        diff = DifferenceDataFrame(df1, df2, "id", tol=0.01)
        joined = diff.joined
        assert joined.columns == ["id", "v_df1", "v_df2", "s_df1", "s_df2"]

    def test_columns_subset(self, spark):
        df1 = spark.createDataFrame([(1, 10.0, "a")], ["id", "v", "s"])
        df2 = spark.createDataFrame([(1, 10.0, "a")], ["id", "v", "s"])
        diff = DifferenceDataFrame(df1, df2, "id", tol=0.01, columns="v")
        assert diff.columns == ["v"]
        assert diff.joined.columns == ["id", "v_df1", "v_df2"]

    def test_id_column_removed_from_columns(self, spark):
        df1 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        diff = DifferenceDataFrame(df1, df2, "id", tol=0.01,
            columns=["id", "v"])
        assert diff.columns == ["v"]

    def test_duplicate_id_raises(self, spark):
        df1 = spark.createDataFrame([(1, 10.0), (1, 11.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        with pytest.raises(Exception):
            DifferenceDataFrame(df1, df2, "id", tol=0.01)

    def test_null_id_raises(self, spark):
        df1 = spark.createDataFrame([(None, 10.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        with pytest.raises(Exception):
            DifferenceDataFrame(df1, df2, "id", tol=0.01)
