"""Tests para libs.data_engineering_toolbox.pyspark.testing (asserts de calidad)."""
import pytest

pytest.importorskip("pyspark")

from libs.data_engineering_toolbox.pyspark.testing.quality import (
    assertColumnHasNoNulls, assertColumnHasNoDuplicates)
from libs.data_engineering_toolbox.pyspark.testing.schemas import assertSchemasEqual
from libs.data_engineering_toolbox.pyspark.testing.compare import assertDataFrameEqual

pytestmark = pytest.mark.spark


class TestAssertColumnHasNoNulls:
    def test_passes_without_nulls(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["v"])
        assertColumnHasNoNulls(df, "v")   # no lanza

    def test_raises_with_nulls(self, spark):
        df = spark.createDataFrame([(1.0,), (None,)], ["v"])
        with pytest.raises(AssertionError, match="valores nulos"):
            assertColumnHasNoNulls(df, "v")


class TestAssertColumnHasNoDuplicates:
    def test_passes_without_duplicates(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["v"])
        assertColumnHasNoDuplicates(df, "v")

    def test_raises_with_duplicates(self, spark):
        df = spark.createDataFrame([(1,), (1,)], ["v"])
        with pytest.raises(AssertionError, match="valores duplicados"):
            assertColumnHasNoDuplicates(df, "v")


class TestAssertSchemasEqual:
    def test_equal_schemas(self, spark):
        df1 = spark.createDataFrame([(1, "x")], ["a", "b"])
        df2 = spark.createDataFrame([(2, "y")], ["a", "b"])
        assertSchemasEqual(df1.schema, df2.schema)

    def test_different_schemas_raise(self, spark):
        df1 = spark.createDataFrame([(1, "x")], ["a", "b"])
        df2 = spark.createDataFrame([(1.0, "x")], ["a", "b"])   # double vs int
        with pytest.raises(AssertionError):
            assertSchemasEqual(df1.schema, df2.schema)


class TestAssertDataFrameEqual:
    def test_equal_frames(self, spark):
        df1 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        assertDataFrameEqual(df1, df2, "id")

    def test_different_frames_raise(self, spark):
        df1 = spark.createDataFrame([(1, 10.0)], ["id", "v"])
        df2 = spark.createDataFrame([(1, 11.0)], ["id", "v"])
        with pytest.raises(AssertionError):
            assertDataFrameEqual(df1, df2, "id", tol=0.01)
