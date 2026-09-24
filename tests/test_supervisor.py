"""Tests para supervisor.py (reinicio autónomo + watchdog anti-zombie).

Todo se simula en local: el "pipeline" es un `python -c`/script en tmp_path que
muere, duerme o imprime líneas de Step como haría main.py. La Spark UI se
simula con monkeypatch sobre `supervisor.spark_task_counts`/`_http_json`.
"""
import os
import subprocess
import sys
import time

import pytest

import supervisor


# ----------------------------------------------------------------------------
# PipelineMonitor (tracking de Steps via stdout)
# ----------------------------------------------------------------------------

class TestPipelineMonitor:
    def test_step_start_updates_current_step(self):
        monitor = supervisor.PipelineMonitor()
        monitor.feed_line("[INFO] Executing step: Standard_Graph_Step")
        assert monitor.current_step == "Standard_Graph_Step"

    def test_step_finished_marks_step_done(self):
        monitor = supervisor.PipelineMonitor()
        monitor.feed_line("Executing step: A")
        monitor.feed_line("Step finished: A (1.2s)")
        assert "A" in monitor.finished_steps
        assert monitor.current_step == "(startup)"

    def test_unrelated_lines_do_not_change_state(self):
        monitor = supervisor.PipelineMonitor()
        monitor.feed_line("Executing step: A")
        monitor.feed_line("26/09/24 INFO TaskSetManager: Finished task 3")
        assert monitor.current_step == "A"

    def test_step_transition_resets_clock(self):
        monitor = supervisor.PipelineMonitor()
        monitor.step_started_at = time.time() - 1000
        monitor.feed_line("Executing step: B")
        assert time.time() - monitor.step_started_at < 5


# ----------------------------------------------------------------------------
# spark_task_counts (REST de la Spark UI simulada)
# ----------------------------------------------------------------------------

class TestSparkTaskCounts:
    def test_sums_active_and_complete_tasks(self, monkeypatch):
        def fake_http(url, timeout=5.0):
            if url.endswith("/applications"):
                return [{"id": "app_1"}]
            if url.endswith("stages?status=active"):
                return [
                    {"numActiveTasks": 3, "numCompleteTasks": 10},
                    {"numActiveTasks": 2, "numCompleteTasks": 5},
                ]
            raise AssertionError(url)
        monkeypatch.setattr(supervisor, "_http_json", fake_http)
        assert supervisor.spark_task_counts(4040) == (5, 15)

    def test_no_active_stages(self, monkeypatch):
        def fake_http(url, timeout=5.0):
            if url.endswith("/applications"):
                return [{"id": "app_1"}]
            if url.endswith("stages?status=active"):
                return []
            raise AssertionError(url)
        monkeypatch.setattr(supervisor, "_http_json", fake_http)
        assert supervisor.spark_task_counts(4040) == (0, 0)

    def test_no_applications_returns_none(self, monkeypatch):
        monkeypatch.setattr(supervisor, "_http_json",
            lambda url, timeout=5.0: [])
        assert supervisor.spark_task_counts(4040) is None

    def test_ui_unreachable_returns_none(self, monkeypatch):
        def boom(url, timeout=5.0):
            raise ConnectionRefusedError("no UI")
        monkeypatch.setattr(supervisor, "_http_json", boom)
        assert supervisor.spark_task_counts(4040) is None


# ----------------------------------------------------------------------------
# is_zombie (decisión zombie vs "sigue calculando")
# ----------------------------------------------------------------------------

@pytest.fixture()
def fast_timeouts(monkeypatch):
    monkeypatch.setattr(supervisor, "STEP_TIMEOUT", 10)
    monkeypatch.setattr(supervisor, "STALL_TIMEOUT", 5)


class TestIsZombie:
    def test_within_step_timeout_is_not_zombie(self, fast_timeouts, monkeypatch):
        monkeypatch.setattr(supervisor, "spark_task_counts", lambda p: None)
        monitor = supervisor.PipelineMonitor()  # step acaba de empezar
        assert supervisor.is_zombie(monitor) is False

    def test_timeout_and_ui_down_is_zombie(self, fast_timeouts, monkeypatch):
        """Sin Spark UI solo queda contar: timeout superado -> zombie."""
        monkeypatch.setattr(supervisor, "spark_task_counts", lambda p: None)
        monitor = supervisor.PipelineMonitor()
        monitor.step_started_at = time.time() - 3600
        assert supervisor.is_zombie(monitor) is True

    def test_growing_tasks_means_computing(self, fast_timeouts, monkeypatch):
        """Tareas completadas creciendo -> sigue calculando, no es zombie."""
        counts = iter([(5, 10), (4, 15), (6, 20)])
        monkeypatch.setattr(supervisor, "spark_task_counts",
            lambda p: next(counts))
        monitor = supervisor.PipelineMonitor()
        monitor.step_started_at = time.time() - 3600
        monitor.last_progress_at = time.time() - 3600
        assert supervisor.is_zombie(monitor) is False
        assert supervisor.is_zombie(monitor) is False
        assert supervisor.is_zombie(monitor) is False

    def test_no_task_progress_after_stall_is_zombie(self, fast_timeouts, monkeypatch):
        """Step expirado + tareas sin completar desde hace STALL_TIMEOUT -> zombie."""
        monkeypatch.setattr(supervisor, "spark_task_counts", lambda p: (0, 7))
        monitor = supervisor.PipelineMonitor()
        monitor.step_started_at = time.time() - 3600
        monitor.last_progress_at = time.time() - 3600
        assert supervisor.is_zombie(monitor) is False   # 1ª muestra: solo baseline
        assert supervisor.is_zombie(monitor) is True    # 2ª: sin avance -> zombie

    def test_recent_progress_extends_life(self, fast_timeouts, monkeypatch):
        """Progreso reciente aunque el Step haya expirado -> no zombie aún."""
        monkeypatch.setattr(supervisor, "spark_task_counts", lambda p: (2, 7))
        monitor = supervisor.PipelineMonitor()
        monitor.step_started_at = time.time() - 3600
        # last_progress_at = ahora -> dentro de STALL_TIMEOUT
        assert supervisor.is_zombie(monitor) is False


# ----------------------------------------------------------------------------
# run_child / main (subprocesos reales en local)
# ----------------------------------------------------------------------------

def _cmd(code: str) -> str:
    return f"{sys.executable} -c {code!r}"


@pytest.fixture(autouse=True)
def _fast_watchdog(monkeypatch):
    """Tick del watchdog corto para que los hijos rápidos no esperen 30s."""
    monkeypatch.setattr(supervisor, "WATCHDOG_INTERVAL", 0.05)


class TestRunChild:
    def test_exit_code_zero(self, monkeypatch):
        monkeypatch.setattr(supervisor, "COMMAND", _cmd("import sys; sys.exit(0)"))
        assert supervisor.run_child(supervisor.PipelineMonitor()) == 0

    def test_exit_code_propagated(self, monkeypatch):
        monkeypatch.setattr(supervisor, "COMMAND", _cmd("import sys; sys.exit(3)"))
        assert supervisor.run_child(supervisor.PipelineMonitor()) == 3

    def test_step_tracking_from_stdout(self, monkeypatch):
        monkeypatch.setattr(supervisor, "COMMAND",
            _cmd("print('Executing step: fake_step', flush=True)"))
        monitor = supervisor.PipelineMonitor()
        supervisor.run_child(monitor)
        assert monitor.current_step == "fake_step"

    def test_zombie_child_is_killed_and_restartable(self, monkeypatch):
        """Hijo colgado sin UI: el watchdog lo mata y run_child devuelve -9."""
        monkeypatch.setattr(supervisor, "STEP_TIMEOUT", 0.5)
        monkeypatch.setattr(supervisor, "STALL_TIMEOUT", 0.5)
        monkeypatch.setattr(supervisor, "WATCHDOG_INTERVAL", 0.05)
        monkeypatch.setattr(supervisor, "spark_task_counts", lambda p: None)
        monkeypatch.setattr(supervisor, "COMMAND",
            _cmd("import time; print('Executing step: stuck', flush=True); "
                 "time.sleep(60)"))
        monitor = supervisor.PipelineMonitor()
        started = time.time()
        assert supervisor.run_child(monitor) == -9
        assert time.time() - started < 15
        assert monitor.current_step == "stuck"


class TestMain:
    def test_success_returns_zero(self, monkeypatch):
        monkeypatch.setattr(supervisor, "COMMAND", _cmd("import sys; sys.exit(0)"))
        assert supervisor.main() == 0

    def test_gives_up_after_max_restarts(self, monkeypatch):
        monkeypatch.setattr(supervisor, "COMMAND", _cmd("import sys; sys.exit(1)"))
        monkeypatch.setattr(supervisor, "MAX_RESTARTS", 1)
        assert supervisor.main() == 1

    def test_restarts_and_eventually_succeeds(self, monkeypatch, tmp_path, capsys):
        """Primer intento muere, segundo funciona: el supervisor reanuda solo."""
        marker = tmp_path / "already_ran"
        script = tmp_path / "flaky.py"
        script.write_text(
            "import pathlib, sys\n"
            f"marker = pathlib.Path(r'{marker}')\n"
            "print('Executing step: flaky_step', flush=True)\n"
            "if not marker.exists():\n"
            "    marker.touch()\n"
            "    sys.exit(1)\n"
            "sys.exit(0)\n"
        )
        monkeypatch.setattr(supervisor, "COMMAND", f"{sys.executable} {script}")
        monkeypatch.setattr(supervisor, "MAX_RESTARTS", 3)
        assert supervisor.main() == 0
        assert "Reinicio 1/3" in capsys.readouterr().out

    def test_zombie_death_also_counts_as_restart(self, monkeypatch):
        """Un hijo zombie consume un reinicio igual que una muerte normal."""
        monkeypatch.setattr(supervisor, "STEP_TIMEOUT", 0.5)
        monkeypatch.setattr(supervisor, "STALL_TIMEOUT", 0.5)
        monkeypatch.setattr(supervisor, "WATCHDOG_INTERVAL", 0.05)
        monkeypatch.setattr(supervisor, "MAX_RESTARTS", 1)
        monkeypatch.setattr(supervisor, "spark_task_counts", lambda p: None)
        monkeypatch.setattr(supervisor, "COMMAND",
            _cmd("import time; time.sleep(60)"))
        assert supervisor.main() == 1


# ----------------------------------------------------------------------------
# Wrapper opt/run-supervised.sh
# ----------------------------------------------------------------------------

class TestRunSupervisedScript:
    def test_script_syntax_ok(self):
        script = os.path.join(
            os.path.dirname(supervisor.__file__), "opt", "run-supervised.sh")
        assert os.path.isfile(script)
        subprocess.run(["bash", "-n", script], check=True)

    def test_script_loads_env_and_execs_supervisor(self):
        script = os.path.join(
            os.path.dirname(supervisor.__file__), "opt", "run-supervised.sh")
        content = open(script).read()
        assert "environment_vars.sh" in content
        assert "supervisor.py" in content
