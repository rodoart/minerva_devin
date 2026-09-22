"""Tests para libs.data_engineering_toolbox.pyspark.tools.partition_lags.

Las funciones que consultan HDFS (`get_partitions_from_parquet_path`,
`SparkTwoPartition*`, `delete_partition_from_table`) no son testeables en local;
se cubren las que operan sobre DataFrames/paths puros.
"""
import pytest

pytest.importorskip("pyspark")

from libs.data_engineering_toolbox.path import HivePath
from libs.data_engineering_toolbox.pyspark.tools.partition_lags import (
    get_partitions_from_dataframe, get_partition_hdfs_path,
)

pytestmark = pytest.mark.spark


class TestGetPartitionsFromDataframe:
    def test_single_partition_column(self, spark):
        df = spark.createDataFrame(
            [("202307",), ("202306",), ("202307",)], ["mis_date"])
        result = get_partitions_from_dataframe(df, "mis_date")
        assert sorted(result, key=lambda d: d["mis_date"]) == [
            {"mis_date": "202306"}, {"mis_date": "202307"}]

    def test_two_partition_columns(self, spark):
        df = spark.createDataFrame(
            [
                ("202307", "2023-08-01"),
                ("202307", "2023-08-02"),
                ("202306", "2023-07-01"),
            ],
            ["mis_date", "process_date"],
        )
        result = get_partitions_from_dataframe(df, ["mis_date", "process_date"])
        assert len(result) == 3
        assert all(set(p.keys()) == {"mis_date", "process_date"}
            for p in result)

    def test_values_cast_to_string(self, spark):
        df = spark.createDataFrame([(202307,)], ["mis_date"])
        result = get_partitions_from_dataframe(df, "mis_date")
        assert result[0]["mis_date"] == "202307"
        assert isinstance(result[0]["mis_date"], str)


class TestGetPartitionHdfsPath:
    def test_single_key(self):
        path = get_partition_hdfs_path(
            HivePath("/data/root"), {"mis_date": "202307"})
        assert str(path) == "/data/root/mis_date=202307"

    def test_two_keys_order_preserved(self):
        path = get_partition_hdfs_path(
            HivePath("/data/root"),
            {"mis_date": "202307", "process_date": "2023-08-05"})
        assert str(path) == "/data/root/mis_date=202307/process_date=2023-08-05"
