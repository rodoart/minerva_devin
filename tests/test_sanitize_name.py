"""Tests para libs/framework/utils.py (sanitize_property_name, módulo puro).

El archivo se carga de forma aislada con importlib: importar el paquete
`libs.framework` arrastraría dependencias de Spark que no son necesarias para
probar esta utilidad.
"""
import importlib.util
from pathlib import Path

import pytest

_utils_path = Path(__file__).resolve().parents[1] / "libs" / "framework" / "utils.py"
_spec = importlib.util.spec_from_file_location("framework_utils", _utils_path)
_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_utils)
sanitize_property_name = _utils.sanitize_property_name


class TestSanitizeName:
    @pytest.mark.parametrize("value,expected", [
        (None, "empty"),
        ("", "empty"),
        ("___", "empty"),
        ("   ", "empty"),
    ])
    def test_empty_inputs(self, value, expected):
        assert sanitize_property_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        (True, "true"),
        (False, "false"),
        (10, "prop_10"),
        (0, "prop_0"),
        (3.14, "prop_3_14"),
        (0.15, "prop_0_15"),
        (-2, "prop_2"),
    ])
    def test_scalar_inputs(self, value, expected):
        assert sanitize_property_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        ("pagerank", "pagerank"),
        ("weighted_pagerank", "weighted_pagerank"),
        ("contagion-score", "contagion_score"),
        ("contagion_composed", "contagion_composed"),
    ])
    def test_names_passthrough(self, value, expected):
        assert sanitize_property_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        ("weight_type=composed", "weight_type_composed"),
        ("alpha=0.15", "alpha_0_15"),
        ("a/b/c", "a_b_c"),
        ("key:value", "key_value"),
        ("espacio en medio", "espacio_en_medio"),
    ])
    def test_special_chars_replaced(self, value, expected):
        assert sanitize_property_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        ("operación", "operacion"),
        ("información", "informacion"),
        ("niño", "nino"),
        ("Ünïcödé", "unicode"),
    ])
    def test_accents_removed(self, value, expected):
        assert sanitize_property_name(value) == expected

    @pytest.mark.parametrize("value,expected", [
        ("a__b", "a_b"),
        ("_inicio", "inicio"),
        ("final_", "final"),
        ("__ambos__", "ambos"),
        ("a---b", "a_b"),   # los guiones se convierten a _ como cualquier símbolo
    ])
    def test_underscores_collapsed_and_stripped(self, value, expected):
        assert sanitize_property_name(value) == expected
