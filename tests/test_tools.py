"""Tests para libs.data_engineering_toolbox.pyspark.tools."""
import pytest

pytest.importorskip("pyspark")
pd = pytest.importorskip("pandas")

from pyspark.sql.functions import col, lit

from libs.data_engineering_toolbox.path import HivePath
from libs.data_engineering_toolbox.pyspark.tools import (
    get_column_alias, is_table_or_parquet, check_if_table_exists,
    convert_partitions_to_spark_filter, ColumnRenamer,
)


class TestGetColumnAlias:
    pytestmark = pytest.mark.spark

    def test_explicit_alias(self):
        assert get_column_alias(col("id_ban_ben").alias("id_ban_dst")) == "id_ban_dst"

    def test_no_alias_returns_expression(self):
        result = get_column_alias(col("customer_id"))
        assert "customer_id" in result

    def test_lit_alias(self):
        assert get_column_alias(lit(1).alias("uno")) == "uno"


class TestIsTableOrParquet:
    @pytest.mark.parametrize("value,expected", [
        ("db.tabla", "table"),          # punto sin slash -> tabla hive
        ("db.esquema.tabla", "table"),
        ("/data/root/path", "hdfs"),    # slash -> hdfs
        ("data/root.con.punto", "hdfs"),# slash tiene prioridad sobre punto
        ("simple_name", "hdfs"),        # sin punto ni slash -> hdfs
    ])
    def test_string_inputs(self, value, expected):
        assert is_table_or_parquet(value) == expected

    def test_hivepath_is_hdfs(self):
        assert is_table_or_parquet(HivePath("/a/b.c")) == "hdfs"


class TestCheckIfTableExists:
    pytestmark = pytest.mark.spark

    def test_temp_view_is_describable(self, spark):
        df = spark.createDataFrame([(1,)], ["n"])
        df.createOrReplaceTempView("test_view_exists")
        assert check_if_table_exists("test_view_exists", spark) is True

    def test_missing_table_returns_false(self, spark):
        assert check_if_table_exists("no_existe_esta_tabla_xyz", spark) is False


class TestConvertPartitionsToSparkFilter:
    def test_single_partition_single_key(self):
        assert convert_partitions_to_spark_filter(
            [{"mis_date": "202307"}]) == "(mis_date = '202307')"

    def test_single_partition_multiple_keys(self):
        result = convert_partitions_to_spark_filter(
            [{"mis_date": "202307", "process_date": "2023-08-05"}])
        assert result == "(mis_date = '202307' AND process_date = '2023-08-05')"

    def test_multiple_partitions_or(self):
        result = convert_partitions_to_spark_filter([
            {"mis_date": "202307"}, {"mis_date": "202306"}])
        assert result == "(mis_date = '202307') OR (mis_date = '202306')"

    def test_empty_list(self):
        assert convert_partitions_to_spark_filter([]) == ""


class TestColumnRenamer:
    pytestmark = pytest.mark.spark

    @pytest.fixture()
    def renamer(self):
        mapping = pd.DataFrame({
            "original": ["a", "b"],
            "masked": ["x", "y"],
        })
        return ColumnRenamer(mapping, "original", "masked")

    def test_rename_forward(self, spark, renamer):
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        result = renamer.rename_columns(df)
        assert result.columns == ["x", "y"]

    def test_rename_reverse(self, spark, renamer):
        df = spark.createDataFrame([(1, 2)], ["x", "y"])
        result = renamer.rename_columns(df, reverse=True)
        assert result.columns == ["a", "b"]

    def test_missing_columns_ignored(self, spark, renamer):
        df = spark.createDataFrame([(1, "z")], ["a", "other"])
        result = renamer.rename_columns(df)
        assert result.columns == ["x", "other"]

    def test_bidirectional_roundtrip(self, spark, renamer):
        df = spark.createDataFrame([(1, 2)], ["a", "b"])
        renamed = renamer.rename_columns(df)
        back = renamer.rename_columns(renamed, reverse=True)
        assert back.columns == ["a", "b"]
        assert back.collect() == df.collect()
