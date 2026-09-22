"""Tests para main.py (helpers puros, sin Spark ni env vars del cluster)."""
import logging
import sys
from unittest.mock import patch

import pytest

import main


class TestParseArgs:
    def test_defaults(self):
        with patch.object(sys, "argv", ["main.py"]):
            args = main.parse_args()
        assert args.level == "INFO"
        assert args.log_file is None
        assert args.build_only is False

    def test_level(self):
        with patch.object(sys, "argv", ["main.py", "--level", "DEBUG"]):
            assert main.parse_args().level == "DEBUG"

    def test_invalid_level_rejected(self):
        with patch.object(sys, "argv", ["main.py", "--level", "TRACE"]):
            with pytest.raises(SystemExit):
                main.parse_args()

    def test_log_file(self):
        with patch.object(sys, "argv", ["main.py", "--log-file", "run.log"]):
            assert main.parse_args().log_file == "run.log"

    def test_build_only(self):
        with patch.object(sys, "argv", ["main.py", "--build-only"]):
            assert main.parse_args().build_only is True


class TestConfigureLogging:
    def test_sets_level(self):
        main.configure_logging("WARNING")
        assert logging.getLogger().level == logging.WARNING

    def test_log_file_handler_added(self, tmp_path):
        log_file = tmp_path / "test.log"
        main.configure_logging("INFO", log_file=str(log_file))
        logger = logging.getLogger("minerva")
        logger.info("mensaje de prueba")
        for h in logging.getLogger().handlers:
            h.flush()
        assert "mensaje de prueba" in log_file.read_text()


class TestCheckEnvironment:
    def test_missing_vars_raises(self, monkeypatch):
        for var in main.REQUIRED_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
        with pytest.raises(EnvironmentError, match="Faltan variables"):
            main.check_environment()

    def test_all_vars_passes(self, monkeypatch):
        for var in main.REQUIRED_ENV_VARS:
            monkeypatch.setenv(var, "x")
        main.check_environment()   # no lanza

    def test_reports_only_missing(self, monkeypatch):
        for var in main.REQUIRED_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv(main.REQUIRED_ENV_VARS[0], "x")
        with pytest.raises(EnvironmentError) as exc_info:
            main.check_environment()
        assert main.REQUIRED_ENV_VARS[0] not in str(exc_info.value)
        assert main.REQUIRED_ENV_VARS[1] in str(exc_info.value)


class TestMainFlow:
    def test_fails_without_env_vars(self, monkeypatch, capsys):
        for var in main.REQUIRED_ENV_VARS:
            monkeypatch.delenv(var, raising=False)
        with patch.object(sys, "argv", ["main.py"]):
            exit_code = main.main()
        assert exit_code == 1
        assert "El pipeline falló" in capsys.readouterr().out

    def test_build_only_skips_execute(self, monkeypatch, capsys):
        for var in main.REQUIRED_ENV_VARS:
            monkeypatch.setenv(var, "x")
        fake_step = type("FakeStep", (), {"step_name": "fake_step"})()
        with patch.object(main, "build_spark", return_value="SPARK"), \
             patch.object(main, "build_pipeline", return_value=fake_step) as bp, \
             patch.object(sys, "argv", ["main.py", "--build-only"]):
            exit_code = main.main()
        assert exit_code == 0
        assert bp.call_count == 1
        assert "build-only" in capsys.readouterr().out

    def test_execute_called_on_last_step(self, monkeypatch):
        for var in main.REQUIRED_ENV_VARS:
            monkeypatch.setenv(var, "x")
        calls = []
        fake_step = type("FakeStep", (), {
            "step_name": "fake_step",
            "execute": lambda self: calls.append(1)})()
        with patch.object(main, "build_spark", return_value="SPARK"), \
             patch.object(main, "build_pipeline", return_value=fake_step), \
             patch.object(sys, "argv", ["main.py"]):
            exit_code = main.main()
        assert exit_code == 0
        assert calls == [1]
