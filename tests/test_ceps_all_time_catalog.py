"""Tests del catálogo de imputación CEP all-time incremental (C3).

Cubre:
- `libs.framework.months_to_date_ranges`: conversión de meses "YYYY-MM" a
  rangos contiguos de días para predicados BETWEEN con partition pruning.
- `group_ceps_flattened` / `combine_ceps_groupby`: los parciales mensuales del
  groupby entidad-identificador son combinables y su recombinación equivale al
  groupby directo de toda la historia (cnt/tot suman, lst=max, fst=min).
- `libs.framework.ensure_monthly_partitions` con `expected_months` explícitos:
  los meses esperados gobiernan sobre la ventana lag/history y la segunda
  corrida no recomputa nada.
"""
import os
from datetime import date

import pytest

pytest.importorskip("pyspark")

# config.job exige MINERVA_TODAY en import; los tests usan un vintage fijo.
os.environ.setdefault("MINERVA_TODAY", "2025-08-31")

from pyspark.sql.functions import col

import libs.framework as ppf

pytestmark = pytest.mark.spark


# ----------------------------------------------------------------------------
# months_to_date_ranges
# ----------------------------------------------------------------------------

class TestMonthsToDateRanges:
    def test_empty(self):
        assert ppf.months_to_date_ranges([]) == []

    def test_single_month(self):
        assert ppf.months_to_date_ranges(["2025-08"]) == [
            ("2025-08-01", "2025-08-31")]

    def test_contiguous_months_merge(self):
        assert ppf.months_to_date_ranges(
            ["2025-06", "2025-07", "2025-08"]) == [
            ("2025-06-01", "2025-08-31")]

    def test_gaps_split_ranges(self):
        assert ppf.months_to_date_ranges(
            ["2025-06", "2025-08", "2025-09"]) == [
            ("2025-06-01", "2025-06-30"), ("2025-08-01", "2025-09-30")]

    def test_year_boundary_and_unsorted_input(self):
        assert ppf.months_to_date_ranges(
            ["2026-01", "2025-11", "2025-12"]) == [
            ("2025-11-01", "2026-01-31")]

    def test_february_leap_year(self):
        assert ppf.months_to_date_ranges(["2024-02"]) == [
            ("2024-02-01", "2024-02-29")]


# ----------------------------------------------------------------------------
# group_ceps_flattened / combine_ceps_groupby
# ----------------------------------------------------------------------------

FLATTENED_COLUMNS = ["nom", "cta", "id_ban", "tipo_cta", "rfc_curp",
    "rfc_curp_kind", "is_rfc_curp_valid", "fec_informacion", "oper_mto",
    "hora_oper", "source"]

GROUPBY_KEYS = ["cta", "nom", "id_ban", "tipo_cta", "rfc_curp",
    "rfc_curp_kind", "source"]
AGG_COLUMNS = ["cnt", "tot_oper_mto", "lst_fec_informacion_hora_oper",
    "fst_fec_informacion_hora_oper", "is_rfc_curp_valid",
    "rfc_curp_ends_with_xxx", "RFC_CURP_KIND_PRIORITY", "SOURCE_PRIORITY"]


@pytest.fixture()
def rnr():
    """Módulo del pipeline (import diferido: usa sesión Spark del conftest)."""
    import pipelines.ceps.rfc_nom_ranking as m
    return m


@pytest.fixture()
def flattened_df(spark):
    """Aplanado de dos meses para dos entidades (misma llave en ambos meses)."""
    rows = [
        # junio
        ("NOMBRE A", "CTA1", "b1", "T", "XAXX010101000", "rfc_fisica", 1,
            "2025-06-10", 100.0, "10:00:00", "s264_ceps"),
        ("NOMBRE A", "CTA1", "b1", "T", "XAXX010101000", "rfc_fisica", 1,
            "2025-06-20", 200.0, "11:00:00", "s264_ceps"),
        # julio (misma llave)
        ("NOMBRE A", "CTA1", "b1", "T", "XAXX010101000", "rfc_fisica", 1,
            "2025-07-05", 300.0, "09:00:00", "s264_ceps"),
        # otra entidad, tag enmascarado (termina en XX)
        ("NOMBRE B", "CTA2", "b2", "T", "CUSA650802SXXX", "rfc_fisica", 1,
            "2025-07-15", 50.0, "12:00:00", "s264_ceps"),
    ]
    return spark.createDataFrame(rows, FLATTENED_COLUMNS)


def _by_key(df):
    return {tuple(r[k] for k in GROUPBY_KEYS): r for r in df.collect()}


class TestGroupAndCombineCepsFlattened:
    def test_combine_matches_direct_groupby(self, spark, rnr, flattened_df):
        prepared = rnr.prepare_flattened_for_groupby(flattened_df)
        direct = _by_key(rnr.group_ceps_flattened(prepared))
        #
        monthly_jun = prepared.where(col("fec_informacion_hora_oper") < "2025-07")
        monthly_jul = prepared.where(col("fec_informacion_hora_oper") >= "2025-07")
        partials = rnr.group_ceps_flattened(monthly_jun).unionByName(
            rnr.group_ceps_flattened(monthly_jul))
        combined = _by_key(rnr.combine_ceps_groupby(partials))
        #
        assert set(direct) == set(combined)
        for key in direct:
            for column in AGG_COLUMNS:
                assert combined[key][column] == direct[key][column], \
                    f"{key} {column}: {combined[key][column]} != {direct[key][column]}"

    def test_first_and_last_observation_accumulate(self, spark, rnr,
            flattened_df):
        prepared = rnr.prepare_flattened_for_groupby(flattened_df)
        combined = _by_key(rnr.combine_ceps_groupby(
            rnr.group_ceps_flattened(prepared)))
        key = ("CTA1", "NOMBRE A", "b1", "T", "XAXX010101000", "rfc_fisica",
            "s264_ceps")
        row = combined[key]
        assert row["cnt"] == 3
        assert row["tot_oper_mto"] == 600.0
        assert row["fst_fec_informacion_hora_oper"] == "2025-06-10#10:00:00"
        assert row["lst_fec_informacion_hora_oper"] == "2025-07-05#09:00:00"
        assert row["is_rfc_curp_valid"] == 3

    def test_xxx_flag_and_priorities_take_max(self, spark, rnr, flattened_df):
        prepared = rnr.prepare_flattened_for_groupby(flattened_df)
        combined = _by_key(rnr.combine_ceps_groupby(
            rnr.group_ceps_flattened(prepared)))
        key_b = ("CTA2", "NOMBRE B", "b2", "T", "CUSA650802SXXX",
            "rfc_fisica", "s264_ceps")
        assert combined[key_b]["rfc_curp_ends_with_xxx"] == 1

    def test_monthly_partials_keep_mis_date_key(self, spark, rnr,
            flattened_df):
        with_month = flattened_df.withColumn(
            "mis_date",
            col("fec_informacion").substr(1, 7))
        partials = rnr.group_ceps_flattened(
            rnr.prepare_flattened_for_groupby(with_month),
            extra_keys=["mis_date"])
        months = {r["mis_date"] for r in partials.collect()}
        assert months == {"2025-06", "2025-07"}


# ----------------------------------------------------------------------------
# ensure_monthly_partitions con expected_months explícitos
# ----------------------------------------------------------------------------

def make_all_time_step(spark, path, vintage_date, process_date,
        history=1200) -> ppf.Step:
    """Step mínimo con una salida mensual de historia all-time en `path`."""
    step = ppf.Step(
        input_hive={},
        output_hive={"monthly": {
            "table_or_hdfs": path,
            "information_date_column": "month_partition",
            "process_date_column": "process_date",
            "lag": 0,
            "history": history,
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


class TestEnsureMonthlyPartitionsExpectedMonths:
    @pytest.fixture()
    def monthly_source(self, spark):
        rows = [(f"2025-{month:02d}-01", f"e{month}", float(month))
            for month in range(1, 9)]
        return spark.createDataFrame(rows, ["month_partition", "id", "v"])

    def test_expected_months_govern_over_window(self, spark, tmp_path,
            local_hdfs, monthly_source):
        """expected_months manda en el CÓMPUTO aunque se salga de la ventana.

        Con history=1 la ventana lag/history solo cubre ago-2025, pero se
        escriben las particiones de los 3 meses esperados; el DataFrame
        devuelto sigue limitado a la ventana de lectura (ago-2025).
        """
        path = tmp_path / "monthly"
        calls = []

        def compute(months):
            calls.append(list(months))
            return monthly_source.filter(
                col("month_partition").substr(1, 7).isin(*months))

        step = make_all_time_step(
            spark, path.as_posix(), date(2025, 8, 31), date(2025, 8, 5),
            history=1)
        result = ppf.ensure_monthly_partitions(
            step, "monthly", compute,
            expected_months=["2025-06", "2025-07", "2025-08"])
        assert calls == [["2025-06", "2025-07", "2025-08"]]
        # las 3 particiones quedan materializadas en disco
        for month in ("2025-06", "2025-07", "2025-08"):
            assert (path / f"month_partition={month}-01" /
                    "process_date=2025-08-05").is_dir()
        # la lectura devuelta respeta la ventana del config (history=1)
        assert {str(r["month_partition"]) for r in result.collect()} == {
            "2025-08-01"}

    def test_second_run_computes_nothing(self, spark, tmp_path, local_hdfs,
            monthly_source):
        """Con historia all-time la segunda corrida relee sin recomputar."""
        path = (tmp_path / "monthly").as_posix()
        calls = []

        def compute(months):
            calls.append(list(months))
            return monthly_source.filter(
                col("month_partition").substr(1, 7).isin(*months))

        expected = ["2025-06", "2025-07", "2025-08"]
        step = make_all_time_step(
            spark, path, date(2025, 8, 31), date(2025, 8, 5))
        ppf.ensure_monthly_partitions(
            step, "monthly", compute, expected_months=expected)
        assert calls == [expected]
        #
        step_again = make_all_time_step(
            spark, path, date(2025, 8, 31), date(2025, 8, 6))
        result = ppf.ensure_monthly_partitions(
            step_again, "monthly", compute, expected_months=expected)
        assert calls == [expected]          # nada nuevo que computar
        assert result.count() == 3

    def test_only_new_source_months_are_computed(self, spark, tmp_path,
            local_hdfs, monthly_source):
        """Un mes nuevo de la fuente se computa solo, sin tocar los viejos."""
        path = (tmp_path / "monthly").as_posix()
        calls = []

        def compute(months):
            calls.append(list(months))
            return monthly_source.filter(
                col("month_partition").substr(1, 7).isin(*months))

        step = make_all_time_step(
            spark, path, date(2025, 7, 31), date(2025, 7, 5))
        ppf.ensure_monthly_partitions(
            step, "monthly", compute,
            expected_months=["2025-06", "2025-07"])
        assert calls == [["2025-06", "2025-07"]]
        #
        step_sep = make_all_time_step(
            spark, path, date(2025, 9, 30), date(2025, 9, 5))
        result = ppf.ensure_monthly_partitions(
            step_sep, "monthly", compute,
            expected_months=["2025-06", "2025-07", "2025-08"])
        assert calls[-1] == ["2025-08"]     # solo el mes nuevo
        assert result.count() == 3
