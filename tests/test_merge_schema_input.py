"""Tests para StandardVectorAssemblerSubStep._partition_suffix y
get_merge_schema_input (pipelines/features/vector_assembler.py).

Mode-aware según MINERVA_TEST_MODE:
- "local" (default): los parquets se escriben en `tmp_path` del filesystem
  local y `HivePath.listparquets` —que consulta HDFS— se parchea con
  monkeypatch para devolver los leaves escritos.
- "cluster": los parquets se escriben en `MINERVA_TMP_TESTS_DIR_HDFS` y se
  usa el `listparquets` real de HDFS (sin parche).
"""
import os

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

from libs.data_engineering_toolbox.path import HivePath
from pipelines.features.vector_assembler import StandardVectorAssemblerSubStep

TEST_MODE = os.environ.get("MINERVA_TEST_MODE", "local")


@pytest.fixture()
def real_listparquets(monkeypatch):
    """Registra los leaves escritos; en local parchea el listparquets de HDFS.

    Devuelve una lista mutable: los tests añaden sus leaves y el monkeypatch
    los sirve. En modo "cluster" no se parchea nada (HDFS real).
    """
    leaves = []
    if TEST_MODE == "local":
        monkeypatch.setattr(HivePath, "listparquets",
            lambda self, recursive=True, sorted_by_time=False: iter(leaves))
    return leaves


class TestPartitionSuffix:
    def test_sanitizes_partition_segments(self):
        base = HivePath("/data/features/weighted_pagerank")
        leaf = HivePath("/data/features/weighted_pagerank/max_iter=10/reset_prob=0.15")
        suffix = StandardVectorAssemblerSubStep._partition_suffix(base, leaf)
        # '=' -> '_eq_' y '.' -> '_dot_' según SYMBOL_REPLACEMENTS
        assert suffix == "max_iter_eq_10_reset_prob_eq_0_dot_15"

    def test_multi_segment_suffix(self):
        base = HivePath("/data/target_propagation")
        leaf = HivePath(
            "/data/target_propagation/weight_type=composed/max_iter=20")
        suffix = StandardVectorAssemblerSubStep._partition_suffix(base, leaf)
        assert suffix == "weight_type_eq_composed_max_iter_eq_20"

    def test_leaf_equals_base_gives_empty_suffix(self):
        base = HivePath("/data/features")
        assert StandardVectorAssemblerSubStep._partition_suffix(base, base) == ""


def _make_step(spark):
    """Instancia mínima del substep con solo `sqlContext` (sin __init__)."""
    step = object.__new__(StandardVectorAssemblerSubStep)
    step.sqlContext = spark
    return step


class TestGetMergeSchemaInput:
    def test_renames_columns_with_partition_suffix_and_joins(
            self, spark, tmp_hdfs, real_listparquets):
        base = tmp_hdfs / "weighted_pagerank"
        leaf_a = base / "max_iter=10" / "reset_prob=0.15"
        leaf_b = base / "max_iter=20" / "reset_prob=0.15"
        (spark.createDataFrame([("a", 0.5), ("b", 0.7)], ["id", "weighted_pagerank"])
            .write.parquet(str(leaf_a)))
        (spark.createDataFrame([("a", 0.9)], ["id", "weighted_pagerank"])
            .write.parquet(str(leaf_b)))
        real_listparquets.extend([HivePath(str(leaf_a)), HivePath(str(leaf_b))])

        step = _make_step(spark)
        df = step.get_merge_schema_input(HivePath(str(base)))

        col_a = "weighted_pagerank_max_iter_eq_10_reset_prob_eq_0_dot_15"
        col_b = "weighted_pagerank_max_iter_eq_20_reset_prob_eq_0_dot_15"
        assert set(df.columns) == {"id", col_a, col_b}
        # join por `id` (inner): solo sobreviven los ids presentes en TODAS las hojas
        rows = {r["id"]: r for r in df.collect()}
        assert set(rows) == {"a"}
        assert rows["a"][col_a] == pytest.approx(0.5)
        assert rows["a"][col_b] == pytest.approx(0.9)

    def test_single_leaf(self, spark, tmp_hdfs, real_listparquets):
        base = tmp_hdfs / "target_propagation"
        leaf = base / "weight_type=composed"
        (spark.createDataFrame([("x", 1.0), ("y", 2.0)], ["id", "contagion_composed"])
            .write.parquet(str(leaf)))
        real_listparquets.append(HivePath(str(leaf)))

        step = _make_step(spark)
        df = step.get_merge_schema_input(HivePath(str(base)))

        assert set(df.columns) == {"id", "contagion_composed_weight_type_eq_composed"}
        assert df.count() == 2
