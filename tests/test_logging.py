"""Tests para libs/data_engineering_toolbox/context/logging.py.

El módulo se carga de forma aislada con importlib: importar el paquete
`libs.data_engineering_toolbox.context` arrastraría dependencias de Spark que
no son necesarias para probar estas funciones.
"""
import importlib.util
import logging
from pathlib import Path

import pytest

_mod_path = (Path(__file__).resolve().parents[1]
    / "libs" / "data_engineering_toolbox" / "context" / "logging.py")
_spec = importlib.util.spec_from_file_location("framework_logging", _mod_path)
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
        logger = get_logger("app")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "app"

    def test_logger_has_console_handler(self):
        logger = get_logger("app")
        assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)


class TestSetupLogging:
    def test_idempotent_without_force_reload(self):
        setup_logging(level="ERROR", force_reload=True)
        assert logging.getLogger("app").level == logging.ERROR
        setup_logging(level="DEBUG")  # ya inicializado: no reconfigura
        assert logging.getLogger("app").level == logging.ERROR

    def test_force_reload_changes_level(self):
        setup_logging(level="WARNING", force_reload=True)
        assert logging.getLogger("app").level == logging.WARNING
        setup_logging(level="DEBUG", force_reload=True)
        assert logging.getLogger("app").level == logging.DEBUG

    def test_invalid_level_falls_back_to_debug(self):
        setup_logging(level="NOT_A_LEVEL", force_reload=True)
        assert logging.getLogger("app").level == logging.DEBUG

    def test_log_file_handler(self, tmp_path):
        log_file = tmp_path / "app_test.log"
        setup_logging(level="INFO", log_file=str(log_file), force_reload=True)
        get_logger("app").info("hola-app-test")
        for handler in logging.getLogger("app").handlers:
            handler.flush()
        assert "hola-app-test" in log_file.read_text()


class TestLogFileResolution:
    """Resolución del .log por variables de entorno (consola + archivo)."""

    def test_app_log_file_env(self, tmp_path, monkeypatch):
        log_file = tmp_path / "env_test.log"
        monkeypatch.setenv("APP_LOG_FILE", str(log_file))
        monkeypatch.delenv("LOGS_DIR_LINUX", raising=False)
        resolved = setup_logging(level="INFO", force_reload=True)
        assert resolved == str(log_file)
        get_logger("app").info("via-env")
        logging.getLogger("app").handlers[-1].flush()
        assert "via-env" in log_file.read_text()

    def test_logs_dir_linux_env(self, tmp_path, monkeypatch):
        monkeypatch.delenv("APP_LOG_FILE", raising=False)
        monkeypatch.setenv("LOGS_DIR_LINUX", str(tmp_path))
        monkeypatch.setenv("APP_NAME", "mi_app")
        resolved = setup_logging(level="INFO", force_reload=True)
        assert resolved is not None
        assert resolved.startswith(tmp_path.as_posix())
        assert "mi_app_" in resolved and resolved.endswith(".log")

    def test_off_disables_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("APP_LOG_FILE", "off")
        monkeypatch.setenv("LOGS_DIR_LINUX", str(tmp_path))
        assert setup_logging(level="INFO", force_reload=True) is None

    def test_no_env_no_file(self, monkeypatch):
        monkeypatch.delenv("APP_LOG_FILE", raising=False)
        monkeypatch.delenv("LOGS_DIR_LINUX", raising=False)
        assert setup_logging(level="INFO", force_reload=True) is None

    def test_console_and_file_both_emit(self, tmp_path, monkeypatch, capsys):
        log_file = tmp_path / "both.log"
        monkeypatch.setenv("APP_LOG_FILE", str(log_file))
        setup_logging(level="INFO", force_reload=True)
        get_logger("app").info("doble-salida")
        logging.getLogger("app").handlers[-1].flush()
        assert "doble-salida" in capsys.readouterr().out
        assert "doble-salida" in log_file.read_text()


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
