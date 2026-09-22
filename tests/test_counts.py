"""Tests para libs.data_engineering_toolbox.pyspark.counts."""
import pytest

pytest.importorskip("pyspark")

from libs.data_engineering_toolbox.pyspark.counts import (
    count_nulls, count_duplicates)

pytestmark = pytest.mark.spark


class TestCountNulls:
    def test_counts_nulls(self, spark):
        df = spark.createDataFrame(
            [(1.0,), (None,), (3.0,), (None,)], ["v"])
        assert count_nulls(df, "v") == 2

    def test_counts_nan_as_null(self, spark):
        df = spark.createDataFrame([(float("nan"),), (1.0,)], ["v"])
        assert count_nulls(df, "v") == 1

    def test_no_nulls(self, spark):
        df = spark.createDataFrame([(1.0,), (2.0,)], ["v"])
        assert count_nulls(df, "v") == 0


class TestCountDuplicates:
    def test_counts_duplicate_rows(self, spark):
        df = spark.createDataFrame([(1,), (2,), (2,), (2,), (3,)], ["v"])
        # el grupo {2:3 veces} aporta 3 a la suma de duplicados
        assert count_duplicates(df, "v") == 3

    def test_no_duplicates(self, spark):
        df = spark.createDataFrame([(1,), (2,), (3,)], ["v"])
        assert count_duplicates(df, "v") == 0

    def test_multiple_duplicate_groups(self, spark):
        df = spark.createDataFrame(
            [(1,), (1,), (2,), (2,), (3,)], ["v"])
        assert count_duplicates(df, "v") == 4

    def test_empty_df(self, spark):
        df = spark.createDataFrame([], "v INT")
        assert count_duplicates(df, "v") == 0
