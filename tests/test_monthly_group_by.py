"""Tests para el group-by mensual incremental.

Cubre el nuevo flujo de agregación en dos fases:
- `standard_group_by_txn_monthly` / `standard_group_by_id_monthly`:
  agregados parciales por (grupo, mes) persistidos como particiones mensuales.
- `merge_monthly_group_by` / `standard_merge_group_by_txn_monthly` /
  `standard_merge_group_by_id_monthly`: combinación de las parciales en el
  agregado de toda la ventana, equivalente al group-by completo.
- `libs.framework.ensure_monthly_partitions`: cómputo incremental (solo los
  meses ausentes) sobre filesystem local mediante un shim de HivePath.
- `libs.framework._window_months`.
"""
import os
from datetime import date

import pytest

pytest.importorskip("pyspark")

# Los tests usan un vintage fijo (date_treatment construido a mano).

from pyspark.sql.functions import (col, lit, to_date, datediff,
    collect_set, max as spark_max, sum as spark_sum)

import libs.framework as ppf
import libs.functions.aggregations as lfa
from pipelines.graph_making.group_by import StandardGroupBySubStep

pytestmark = pytest.mark.spark

REFERENCE_DATE = "2025-08-31"

# nombres "{func}_{variable}" al estilo de GROUP_TXN_AGGREGATIONS
TXN_AGGREGATIONS = [
    "min_oper_mto", "max_oper_mto", "mean_oper_mto", "sum_oper_mto",
    "count_oper_mto", "max_tfrom_days", "weighted_mean_oper_mto",
    "weighted_sum_oper_mto", "weighted_count_oper_mto", "so_oper_mto",
]
ID_SET_COLUMNS = ["numcliente", "nom", "cta", "id_ban"]
ID_SUM_COLUMNS = ["oper_mto"]


# ----------------------------------------------------------------------------
# Helpers / fixtures
# ----------------------------------------------------------------------------

class _ParentStep(ppf.Step):
    """Step padre mínimo con los atributos que heredan los substeps."""
    def __init__(self, spark=None, **kwargs):
        super().__init__(**kwargs)
        self.cohort = "SBX"
        self.is_dynamic = False
        self.sqlContext = spark
        self.standard_load_parquet_or_table = lambda *a, **k: None
        self.date_treatment = {
            "vintage": "202508",
            "process_date_str": "2025-08-05",
            "last_day_of_current_month_date_str": "2025-08-31",
        }


def make_substep(spark, **kwargs) -> StandardGroupBySubStep:
    kwargs.setdefault("input_hive", {})
    kwargs.setdefault("output_hive", {})
    kwargs.setdefault("input_parameters", {
        "vintage": "202508",
        "vintage_date": date(2025, 8, 31),
        "process_date": date(2025, 8, 5),
    })
    return StandardGroupBySubStep(_ParentStep(spark, **kwargs))


@pytest.fixture()
def txns_df(spark):
    """Transacciones de 3 meses para dos aristas, con `tfrom_days` precalculado."""
    rows = [
        ("A", "B", 100.0, "2025-06-10"),
        ("A", "B", 200.0, "2025-06-20"),
        ("A", "B", 300.0, "2025-07-15"),
        ("A", "B", 400.0, "2025-08-05"),
        ("A", "C",  50.0, "2025-08-01"),
    ]
    return (spark.createDataFrame(
            rows, ["id_src", "id_dst", "oper_mto", "information_date"])
        .withColumn("tfrom_days",
            datediff(lit(REFERENCE_DATE), to_date(col("information_date")))))


@pytest.fixture()
def txn_sides(spark):
    """Proyecciones origen/destino tipo `txn_src`/`txn_dst`."""
    columns = ["id", "numcliente", "nom", "cta", "id_ban",
        "oper_mto", "information_date"]
    src = spark.createDataFrame([
        ("a", "c1", "nomA", "cta1", "b1", 10.0, "2025-06-10"),
        ("a", "c1", "nomA", "cta1", "b1", 20.0, "2025-08-05"),
        ("b", "c2", "nomB", "cta2", "b2", 30.0, "2025-08-20"),
    ], columns)
    dst = spark.createDataFrame([
        ("b", "c9", "nomB9", "cta9", "b9", 5.0, "2025-07-15"),
    ], columns)
    return src, dst


def make_monthly_step(spark, path, vintage_date, process_date) -> ppf.Step:
    """Step mínimo con una salida particionada mensual en `path`."""
    step = ppf.Step(
        input_hive={},
        output_hive={"monthly": {
            "table_or_hdfs": path,
            "information_date_column": "month_partition",
            "process_date_column": "process_date",
            "lag": 0,
            "history": 3,
            "information_date_mode": "each",
            "process_date_mode": "last",
        }},
        input_parameters={
            "vintage": "202508",
            "vintage_date": vintage_date,
            "process_date": process_date,
        })
    step.is_dynamic = True
    step.sqlContext = spark
    return step


# ----------------------------------------------------------------------------
# _window_months
# ----------------------------------------------------------------------------

class TestWindowMonths:
    def test_last_three_months(self):
        assert ppf._window_months(date(2025, 8, 31), 3, 0) == [
            "2025-06", "2025-07", "2025-08"]

    def test_twelve_month_window(self):
        result = ppf._window_months(date(2025, 7, 31), 12, 0)
        assert len(result) == 12
        assert result[0] == "2024-08" and result[-1] == "2025-07"

    def test_lag_shifts_window_back(self):
        assert ppf._window_months(date(2025, 8, 31), 3, 1) == [
            "2025-05", "2025-06", "2025-07"]


# ----------------------------------------------------------------------------
# Agregados parciales mensuales por arista
# ----------------------------------------------------------------------------

class TestGroupByTxnMonthly:
    def test_partial_columns(self, spark, txns_df):
        sub = make_substep(spark)
        monthly = sub.standard_group_by_txn_monthly(
            txns_df, value_columns=["oper_mto"],
            txn_id_columns=["id_src", "id_dst"])
        expected = {
            "id_src", "id_dst", "month_partition",
            "min_oper_mto", "max_oper_mto", "sum_oper_mto", "count_oper_mto",
            "sum2_oper_mto", "sum3_oper_mto", "sum4_oper_mto",
            "wsum_oper_mto", "vtsum_oper_mto",
            "min_t_rel_days", "max_t_rel_days", "sum_t_rel_days",
            "count_t_rel_days", "information_date",
        }
        assert expected.issubset(set(monthly.columns))

    def test_partial_values(self, spark, txns_df):
        sub = make_substep(spark)
        monthly = sub.standard_group_by_txn_monthly(
            txns_df, value_columns=["oper_mto"],
            txn_id_columns=["id_src", "id_dst"])
        rows = {(r["id_src"], r["id_dst"], str(r["month_partition"])): r
            for r in monthly.collect()}
        june = rows[("A", "B", "2025-06-30")]
        assert june["sum_oper_mto"] == 300.0
        assert june["count_oper_mto"] == 2
        assert june["min_oper_mto"] == 100.0
        # t_rel: jun-10 -> 20, jun-20 -> 10 (días hasta fin de junio)
        assert june["max_t_rel_days"] == 20
        assert june["sum_t_rel_days"] == 30
        assert june["vtsum_oper_mto"] == 100*20 + 200*10
        assert june["information_date"] == "2025-06-20"
        # agosto: una sola txn
        august = rows[("A", "B", "2025-08-31")]
        assert august["count_oper_mto"] == 1
        assert august["max_t_rel_days"] == 26   # ago-05 -> ago-31

    def test_months_filter(self, spark, txns_df):
        sub = make_substep(spark)
        monthly = sub.standard_group_by_txn_monthly(
            txns_df, value_columns=["oper_mto"], months=["2025-08"])
        months = {str(r["month_partition"]) for r in monthly.collect()}
        assert months == {"2025-08-31"}


# ----------------------------------------------------------------------------
# Merge mensual == group-by completo
# ----------------------------------------------------------------------------

class TestMergeEquivalence:
    def _full(self, sub, df):
        """Group-by completo de referencia (misma lógica que CepsGroupBySubStep)."""
        aggregations = lfa.resolve_group_by_expressions(
            TXN_AGGREGATIONS, lfa.standard_group_by_features())
        return sub.standard_group_by_txn(
            input_df=df, aggregations=aggregations,
            txn_id_columns=["id_src", "id_dst"],
            auxiliary_columns=lfa.recency_auxiliary_columns("tfrom_days"))

    def _merged(self, sub, df):
        monthly = sub.standard_group_by_txn_monthly(
            df, value_columns=["oper_mto"],
            txn_id_columns=["id_src", "id_dst"])
        return sub.standard_merge_group_by_txn_monthly(
            monthly_df=monthly, aggregations=TXN_AGGREGATIONS,
            feature_names=lfa.standard_group_by_features(),
            reference_date=REFERENCE_DATE,
            txn_id_columns=["id_src", "id_dst"])

    def test_same_aggregated_values(self, spark, txns_df):
        sub = make_substep(spark)
        full = {(r["id_src"], r["id_dst"]): r for r in self._full(sub, txns_df).collect()}
        merged = {(r["id_src"], r["id_dst"]): r for r in self._merged(sub, txns_df).collect()}
        assert set(full) == set(merged)
        for key in full:
            for name in TXN_AGGREGATIONS:
                assert merged[key][name] == pytest.approx(full[key][name]), \
                    f"{key} {name}: {merged[key][name]} != {full[key][name]}"
            assert merged[key]["information_date"] == full[key]["information_date"]

    def test_weighted_values_are_exact(self, spark, txns_df):
        """Los pesos por recencia usan el máximo global, como el group-by completo."""
        sub = make_substep(spark)
        merged = {(r["id_src"], r["id_dst"]): r
            for r in self._merged(sub, txns_df).collect()}
        ab = merged[("A", "B")]
        # G=82 (jun-10) -> pesos 1,11,36,57
        assert ab["weighted_sum_oper_mto"] == pytest.approx(
            100*1 + 200*11 + 300*36 + 400*57)
        assert ab["weighted_count_oper_mto"] == pytest.approx(1 + 11 + 36 + 57)
        assert ab["weighted_mean_oper_mto"] == pytest.approx(
            (100*1 + 200*11 + 300*36 + 400*57) / (1 + 11 + 36 + 57))
        assert ab["max_tfrom_days"] == 82
        ac = merged[("A", "C")]
        assert ac["weighted_sum_oper_mto"] == pytest.approx(50 * (82 - 30 + 1))

    def test_non_mergeable_aggregations_raise(self, spark, txns_df):
        sub = make_substep(spark)
        monthly = sub.standard_group_by_txn_monthly(
            txns_df, value_columns=["oper_mto"])
        for bad_name in ("countDistinct_oper_mto", "last_oper_mto",
                "first_oper_mto"):
            with pytest.raises(ValueError):
                sub.standard_merge_group_by_txn_monthly(
                    monthly_df=monthly, aggregations=[bad_name],
                    feature_names=lfa.standard_group_by_features(),
                    reference_date=REFERENCE_DATE)


# ----------------------------------------------------------------------------
# Group-by por nodo mensual + merge
# ----------------------------------------------------------------------------

class TestGroupByIdMonthly:
    def test_monthly_partial_values(self, spark, txn_sides):
        src, dst = txn_sides
        sub = make_substep(spark)
        monthly = sub.standard_group_by_id_monthly(
            src, dst, set_columns=ID_SET_COLUMNS, sum_columns=ID_SUM_COLUMNS)
        rows = {(r["id"], str(r["month_partition"])): r
            for r in monthly.collect()}
        assert rows[("a", "2025-06-30")]["oper_mto"] == 10.0
        assert rows[("a", "2025-08-31")]["oper_mto"] == 20.0
        assert rows[("b", "2025-07-31")]["oper_mto"] == 5.0
        assert rows[("b", "2025-08-31")]["oper_mto"] == 30.0
        assert sorted(rows[("b", "2025-08-31")]["numcliente"]) == ["c2"]

    def test_merge_equivalent_to_full(self, spark, txn_sides):
        src, dst = txn_sides
        sub = make_substep(spark)
        monthly = sub.standard_group_by_id_monthly(
            src, dst, set_columns=ID_SET_COLUMNS, sum_columns=ID_SUM_COLUMNS)
        merged = {r["id"]: r for r in sub.standard_merge_group_by_id_monthly(
            monthly, set_columns=ID_SET_COLUMNS,
            sum_columns=ID_SUM_COLUMNS).collect()}
        # nodo a: dos filas origen en junio y agosto
        assert sorted(merged["a"]["numcliente"]) == ["c1"]
        assert sorted(merged["a"]["nom"]) == ["nomA"]
        assert merged["a"]["oper_mto"] == pytest.approx(30.0)
        assert merged["a"]["information_date"] == "2025-08-05"
        # nodo b: una fila origen (ago) y una destino (jul)
        assert sorted(merged["b"]["numcliente"]) == ["c2", "c9"]
        assert sorted(merged["b"]["id_ban"]) == ["b2", "b9"]
        assert merged["b"]["oper_mto"] == pytest.approx(35.0)
        assert merged["b"]["information_date"] == "2025-08-20"

    def test_merge_matches_standard_columns(self, spark, txn_sides):
        """El merge produce las mismas columnas que `standard_group_by_id`."""
        src, dst = txn_sides
        sub = make_substep(spark)
        monthly = sub.standard_group_by_id_monthly(
            src, dst, set_columns=ID_SET_COLUMNS, sum_columns=ID_SUM_COLUMNS)
        merged = sub.standard_merge_group_by_id_monthly(
            monthly, set_columns=ID_SET_COLUMNS, sum_columns=ID_SUM_COLUMNS)
        full = sub.standard_group_by_id(
            src, dst,
            aggregations=[
                *[collect_set(c).alias(c) for c in ID_SET_COLUMNS],
                spark_max("information_date").alias("information_date"),
                spark_sum("oper_mto").alias("oper_mto")],
            id_columns="id")
        assert set(merged.columns) == set(full.columns)


# ----------------------------------------------------------------------------
# ensure_monthly_partitions (cómputo incremental sobre fs local)
# ----------------------------------------------------------------------------

class TestEnsureMonthlyPartitions:
    @pytest.fixture()
    def monthly_source(self, spark):
        rows = [
            ("2025-06-30", "e1", 1.0),
            ("2025-07-31", "e2", 2.0),
            ("2025-08-31", "e3", 3.0),
            ("2025-09-30", "e4", 4.0),
        ]
        return spark.createDataFrame(rows, ["month_partition", "id", "v"])

    def test_computes_only_missing_months(self, spark, tmp_path, local_hdfs,
            monthly_source):
        path = (tmp_path / "monthly").as_posix()
        calls = []

        def compute(months):
            calls.append(list(months))
            return monthly_source.filter(
                col("month_partition").substr(1, 7).isin(*months))

        # primera corrida (vintage ago): los 3 meses de la ventana faltan
        step_aug = make_monthly_step(
            spark, path, date(2025, 8, 31), date(2025, 8, 5))
        result_aug = ppf.ensure_monthly_partitions(step_aug, "monthly", compute)
        assert calls == [["2025-06", "2025-07", "2025-08"]]
        assert {str(r["month_partition"]) for r in result_aug.collect()} == {
            "2025-06-30", "2025-07-31", "2025-08-31"}

        # segunda corrida (vintage sep): la ventana se desplaza; solo falta sep
        step_sep = make_monthly_step(
            spark, path, date(2025, 9, 30), date(2025, 9, 5))
        result_sep = ppf.ensure_monthly_partitions(step_sep, "monthly", compute)
        assert calls[-1] == ["2025-09"]
        # la ventana [jul-sep] ya no incluye junio
        assert {str(r["month_partition"]) for r in result_sep.collect()} == {
            "2025-07-31", "2025-08-31", "2025-09-30"}

        # tercera corrida (mismo vintage): nada que computar
        result_again = ppf.ensure_monthly_partitions(step_sep, "monthly", compute)
        assert len(calls) == 2
        assert result_again.count() == 3

    def test_writes_partition_dirs(self, spark, tmp_path, local_hdfs,
            monthly_source):
        path = tmp_path / "monthly"
        step = make_monthly_step(
            spark, path.as_posix(), date(2025, 8, 31), date(2025, 8, 5))
        ppf.ensure_monthly_partitions(
            step, "monthly", lambda months: monthly_source.filter(
                col("month_partition").substr(1, 7).isin(*months)))
        assert (path / "month_partition=2025-08-31" /
                "process_date=2025-08-05").is_dir()

    def test_not_dynamic_recomputes_everything(self, spark, tmp_path, local_hdfs,
            monthly_source):
        path = (tmp_path / "monthly").as_posix()
        calls = []

        def compute(months):
            calls.append(list(months))
            return monthly_source.filter(
                col("month_partition").substr(1, 7).isin(*months))

        step = make_monthly_step(
            spark, path, date(2025, 8, 31), date(2025, 8, 5))
        ppf.ensure_monthly_partitions(step, "monthly", compute)
        step.is_dynamic = False
        ppf.ensure_monthly_partitions(step, "monthly", compute)
        # con is_dynamic=False ignora las particiones existentes y recomputa
        assert calls[-1] == ["2025-06", "2025-07", "2025-08"]
