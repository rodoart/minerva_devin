"""Tests para libs/data_engineering_toolbox/context/logging.py.

El módulo se carga de forma aislada con importlib: importar el paquete
`libs.data_engineering_toolbox.context` arrastraría dependencias de Spark que
no son necesarias para probar estas funciones.
"""
import importlib.util
import logging
from pathlib import Path

import pytest

pytest.importorskip("numpy")  # el módulo importa `from numpy import True_`

_mod_path = (Path(__file__).resolve().parents[1]
    / "libs" / "data_engineering_toolbox" / "context" / "logging.py")
_spec = importlib.util.spec_from_file_location("minerva_logging", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

setup_logging = _mod.setup_logging
get_logger = _mod.get_logger
log_method = _mod.log_method


@pytest.fixture(autouse=True)
def _reset_logging():
    """Restaura la config de logging tras cada test (setup_logging es global)."""
    yield
    setup_logging(level="INFO", force_reload=True)


class TestGetLogger:
    def test_returns_named_logger(self):
        logger = get_logger("minerva")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "minerva"

    def test_logger_has_console_handler(self):
        logger = get_logger("minerva")
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


class TestSetupLogging:
    def test_idempotent_without_force_reload(self):
        setup_logging(level="ERROR", force_reload=True)
        assert logging.getLogger("minerva").level == logging.ERROR
        setup_logging(level="DEBUG")  # ya inicializado: no reconfigura
        assert logging.getLogger("minerva").level == logging.ERROR

    def test_force_reload_changes_level(self):
        setup_logging(level="WARNING", force_reload=True)
        assert logging.getLogger("minerva").level == logging.WARNING
        setup_logging(level="DEBUG", force_reload=True)
        assert logging.getLogger("minerva").level == logging.DEBUG

    def test_invalid_level_falls_back_to_debug(self):
        setup_logging(level="NOT_A_LEVEL", force_reload=True)
        assert logging.getLogger("minerva").level == logging.DEBUG

    def test_log_file_handler(self, tmp_path):
        log_file = tmp_path / "minerva_test.log"
        setup_logging(level="INFO", log_file=str(log_file), force_reload=True)
        get_logger("minerva").info("hola-minerva-test")
        for handler in logging.getLogger("minerva").handlers:
            handler.flush()
        assert "hola-minerva-test" in log_file.read_text()


class TestLogMethod:
    def test_calls_function_and_returns_result(self):
        calls = []

        class Dummy:
            @log_method
            def work(self, x):
                calls.append(x)
                return x * 2

        assert Dummy().work(21) == 42
        assert calls == [21]

    def test_propagates_exception(self):
        class Dummy:
            @log_method
            def fail(self):
                raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            Dummy().fail()
