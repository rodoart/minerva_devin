"""Tests para StandardExtractSubStep.calculate_tfroms (soporte de lags).

La fecha de referencia es el último día del mes vintage desplazada por el
"lag" (en meses) configurado en el input correspondiente:
referencia = last_day_of_current_month - lag meses.
"""
import os
from datetime import date

import pytest

pytest.importorskip("pyspark")

# config.job exige RG49392_TODAY en import; los tests usan un vintage fijo.
os.environ.setdefault("RG49392_TODAY", "2025-08-31")

import libs.framework as ppf
from pipelines.graph_making.special_treatment import StandardExtractSubStep

pytestmark = pytest.mark.spark


# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------

class _ParentStep(ppf.Step):
    """Step padre mínimo con los atributos que heredan los substeps."""
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.cohort = "SBX"
        self.is_dynamic = False
        self.standard_load_parquet_or_table = lambda *a, **k: None
        self.date_treatment = {
            "last_day_of_current_month_date_str": "2025-08-31",
        }


def make_parent(lag: int = None) -> _ParentStep:
    input_hive = {"src": {}}
    if lag is not None:
        input_hive["src"]["lag"] = lag
    return _ParentStep(
        input_hive=input_hive,
        output_hive={},
        input_parameters={
            "vintage": "202507",
            "vintage_date": date(2025, 7, 31),
        },
    )


@pytest.fixture()
def info_df(spark):
    """Una fila con information_date fija a mediados de agosto."""
    return spark.createDataFrame(
        [("a", "2025-08-15")], ["id", "information_date"])


# ----------------------------------------------------------------------------
# calculate_tfroms
# ----------------------------------------------------------------------------

class TestCalculateTfromsLag:
    def test_no_key_uses_vintage_end(self, info_df):
        sub = StandardExtractSubStep(make_parent())
        row = sub.calculate_tfroms(info_df).first()
        # referencia 2025-08-31 -> 16 días, 0 meses completos
        assert row["tfrom_days"] == 16
        assert row["tfrom_months"] == 0

    def test_key_without_lag_uses_vintage_end(self, info_df):
        sub = StandardExtractSubStep(make_parent())
        row = sub.calculate_tfroms(info_df, "src").first()
        assert row["tfrom_days"] == 16

    def test_lag_zero_same_as_vintage(self, info_df):
        sub = StandardExtractSubStep(make_parent(lag=0))
        row = sub.calculate_tfroms(info_df, "src").first()
        assert row["tfrom_days"] == 16

    def test_positive_lag_shifts_reference_back(self, info_df):
        sub = StandardExtractSubStep(make_parent(lag=2))
        row = sub.calculate_tfroms(info_df, "src").first()
        # referencia = 2025-08-31 - 2 meses = 2025-06-30
        # -> la fila es "futura" respecto a la referencia: -46 días, -2 meses
        assert row["tfrom_days"] == -46
        assert row["tfrom_months"] == -2

    def test_negative_lag_shifts_reference_forward(self, info_df):
        sub = StandardExtractSubStep(make_parent(lag=-1))
        row = sub.calculate_tfroms(info_df, "src").first()
        # referencia = 2025-08-31 + 1 mes = 2025-09-30 -> 46 días, 1 mes
        assert row["tfrom_days"] == 46
        assert row["tfrom_months"] == 1

    def test_tfrom_months_computed_for_full_dates(self, info_df):
        # Con fechas completas yyyy-MM-dd el tfrom mensual no debe salir nulo.
        sub = StandardExtractSubStep(make_parent(lag=1))
        row = sub.calculate_tfroms(info_df, "src").first()
        assert row["tfrom_months"] is not None
        # referencia = 2025-07-31 -> -0.48 meses -> floor = -1
        assert row["tfrom_months"] == -1
        # y en días: 2025-07-31 - 2025-08-15 = -15
        assert row["tfrom_days"] == -15
