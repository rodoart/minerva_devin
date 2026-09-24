"""Tests para sanitize_path_name y sanitize_column_name de libs/framework/utils.py.

El archivo se carga de forma aislada con importlib: importar el paquete
`libs.framework` arrastraría dependencias de Spark que no son necesarias para
probar estas utilidades.
"""
import importlib.util
from pathlib import Path

import pytest

_utils_path = Path(__file__).resolve().parents[1] / "libs" / "framework" / "utils.py"
_spec = importlib.util.spec_from_file_location("framework_utils", _utils_path)
_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_utils)
sanitize_path_name = _utils.sanitize_path_name
sanitize_column_name = _utils.sanitize_column_name


class TestSanitizePathName:
    @pytest.mark.parametrize("value,expected", [
        (None, "empty"),
        ("", ""),
        ("/", ""),
    ])
    def test_empty_inputs(self, value, expected):
        assert sanitize_path_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        ("a/b/c", "a/b/c"),                    # conserva la estructura de directorios
        ("/a/b/", "a/b"),                      # strip de / al inicio y final
        ("a//b///c", "a/b/c"),                 # colapsa repeticiones de /
        ("max_iter=10/reset_prob=0.15",
            "max_iter_10/reset_prob_0.15"),    # '=' -> '_'; '.' se conserva
        ("weight_type=composed", "weight_type_composed"),
        ("a-b.c", "a-b.c"),                    # '-' y '.' se conservan
        ("a__b", "a_b"),                       # colapsa repeticiones de _
    ])
    def test_path_structure(self, value, expected):
        assert sanitize_path_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        (True, "True"),                        # no hace lower() (a diferencia de property/column)
        (10, "10"),
        (0.15, "0.15"),
        ("información", "informacion"),
        ("espacio en medio", "espacio_en_medio"),
    ])
    def test_values(self, value, expected):
        assert sanitize_path_name(value) == expected


class TestSanitizeColumnName:
    @pytest.mark.parametrize("value,expected", [
        (None, "empty"),
        (True, "true"),
        (False, "false"),
        ("Ünïcödé", "unicode"),                # lower + quitar acentos
        ("10x", "col_10x"),                    # prefijo col_ si empieza por dígito
        ("_a_", "a"),
    ])
    def test_basic(self, value, expected):
        assert sanitize_column_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        ("a+b", "a_plus_b"),
        ("a-b", "a_minus_b"),
        ("a*b", "a_times_b"),
        ("a/b", "a_div_b"),
        ("a%b", "a_pct_b"),
        ("a=b", "a_eq_b"),
        ("a>b", "a_gt_b"),
        ("a<b", "a_lt_b"),
        ("a&b", "a_and_b"),
        ("a|b", "a_or_b"),
        ("a@b", "a_at_b"),
        ("a$b", "a_dollar_b"),
        ("a#b", "a_num_b"),
        ("a.b", "a_dot_b"),
    ])
    def test_single_char_symbols(self, value, expected):
        assert sanitize_column_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        ("a!=b", "a_neq_b"),                   # operadores largos primero (no '_lt__eq_')
        ("a>=b", "a_gte_b"),
        ("a<=b", "a_lte_b"),
        ("x>=10 AND y", "x_gte_10_and_y"),
        ("max_iter=10", "max_iter_eq_10"),
        ("reset_prob=0.15", "reset_prob_eq_0_dot_15"),
    ])
    def test_multi_char_operators(self, value, expected):
        assert sanitize_column_name(value) == expected
