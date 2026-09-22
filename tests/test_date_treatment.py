"""Tests para libs.data_engineering_toolbox.general.date_treatment (puro)."""
from datetime import date

import pytest

from libs.data_engineering_toolbox.general.date_treatment import (
    vintage_yyyymm_to_date, vintage_yyyymm_to_yy_and_mm,
    vintage_yyyymm_plus_months, get_last_day_of_the_month,
    get_first_day_of_the_month, make_date_interval_months,
    make_date_interval_with_lag_months,
)


class TestVintageYyyymm:
    def test_to_date(self):
        assert vintage_yyyymm_to_date("202307") == date(2023, 7, 1)

    def test_to_date_december(self):
        assert vintage_yyyymm_to_date("202312") == date(2023, 12, 1)

    def test_to_yy_and_mm(self):
        assert vintage_yyyymm_to_yy_and_mm("202307") == ("23", "07")

    def test_to_yy_and_mm_january(self):
        assert vintage_yyyymm_to_yy_and_mm("203001") == ("30", "01")

    @pytest.mark.parametrize("yyyymm,months,expected", [
        ("202307", 1, "202308"),
        ("202307", 6, "202401"),
        ("202312", 1, "202401"),
        ("202301", -1, "202212"),
        ("202301", -13, "202112"),
        ("202307", "2", "202309"),   # acepta meses como string
        ("202307", 0, "202307"),
    ])
    def test_plus_months(self, yyyymm, months, expected):
        assert vintage_yyyymm_plus_months(yyyymm, months) == expected


class TestMonthBounds:
    @pytest.mark.parametrize("d,expected", [
        (date(2023, 7, 15), date(2023, 7, 31)),
        (date(2023, 7, 1), date(2023, 7, 31)),
        (date(2023, 7, 31), date(2023, 7, 31)),
        (date(2023, 2, 10), date(2023, 2, 28)),   # no bisiesto
        (date(2024, 2, 10), date(2024, 2, 29)),   # bisiesto
        (date(2023, 4, 30), date(2023, 4, 30)),
        (date(2023, 12, 5), date(2023, 12, 31)),
    ])
    def test_last_day(self, d, expected):
        assert get_last_day_of_the_month(d) == expected

    @pytest.mark.parametrize("d", [
        date(2023, 7, 1), date(2023, 7, 15), date(2023, 7, 31),
    ])
    def test_first_day(self, d):
        assert get_first_day_of_the_month(d) == date(2023, 7, 1)


class TestMakeDateIntervalMonths:
    def test_positive_history(self):
        # history=3 desde jul-2023: inicio 1-may, fin 1-ago (fin de jul + 1 día)
        start, end = make_date_interval_months(date(2023, 7, 15), 3)
        assert start == date(2023, 5, 1)
        assert end == date(2023, 8, 1)

    def test_history_one(self):
        start, end = make_date_interval_months(date(2023, 7, 15), 1)
        assert start == date(2023, 7, 1)
        assert end == date(2023, 8, 1)

    def test_history_minus_one_uses_current_month_only(self):
        # rama history == -1: mismo comportamiento que history=1
        start, end = make_date_interval_months(date(2023, 7, 15), -1)
        assert start == date(2023, 7, 1)
        assert end == date(2023, 8, 1)

    def test_negative_history_future_interval(self):
        # history=-3: desde primer día del mes actual hasta +3 meses
        start, end = make_date_interval_months(date(2023, 7, 15), -3)
        assert start == date(2023, 7, 1)
        assert end == date(2023, 10, 1)

    def test_zero_history_raises(self):
        with pytest.raises(AssertionError, match="History must be different"):
            make_date_interval_months(date(2023, 7, 15), 0)

    def test_year_boundary(self):
        start, end = make_date_interval_months(date(2023, 1, 20), 3)
        assert start == date(2022, 11, 1)
        assert end == date(2023, 2, 1)


class TestMakeDateIntervalWithLag:
    def test_docstring_case(self):
        # current=2023-12-31, history=6, lag=2 -> desplaza a 2023-10-31
        # y aplica intervalo de 6 meses: (2023-05-01, 2023-11-01)
        start, end = make_date_interval_with_lag_months(date(2023, 12, 31), 6, 2)
        assert start == date(2023, 5, 1)
        assert end == date(2023, 11, 1)

    def test_zero_lag_equals_no_lag(self):
        d = date(2023, 7, 15)
        assert make_date_interval_with_lag_months(d, 3, 0) == \
            make_date_interval_months(d, 3)

    def test_negative_lag_shifts_forward(self):
        start, end = make_date_interval_with_lag_months(date(2023, 7, 15), 2, -1)
        # lag -1 -> current pasa a ago-2023: (2023-07-01, 2023-09-01)
        assert start == date(2023, 7, 1)
        assert end == date(2023, 9, 1)

    def test_lag_across_year(self):
        start, end = make_date_interval_with_lag_months(date(2023, 2, 10), 2, 1)
        # current -> ene-2023: (2022-12-01, 2023-02-01)
        assert start == date(2022, 12, 1)
        assert end == date(2023, 2, 1)
