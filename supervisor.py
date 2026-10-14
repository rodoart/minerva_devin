#!/usr/bin/env python3
"""Supervisor autónomo de sesiones de pipeline Spark.

Lanza el comando configurado como subproceso y lo vigila:

- **Muerte del proceso** (OOM-kill, SIGKILL, excepción no controlada): lo
  reinicia de forma autónoma. Como los outputs decorados se recargan desde
  parquet (`_reload_or_recompute_parquet`), el nuevo proceso "reanuda" de
  facto en el Step en el que iba.
- **Step atascado**: si el pipeline lleva más de `SUPERVISOR_STEP_TIMEOUT`
  segundos en el mismo Step, se consulta la Spark UI del driver:
    * si hay tareas activas o las completadas crecen -> sigue calculando:
      se espera;
    * si no hay trabajo vivo durante `SUPERVISOR_STALL_TIMEOUT` -> zombie:
      se mata el grupo de procesos y se reinicia.

Detección de zombie (mejor que solo contar): la Spark UI expone
`/api/v1/applications` en el puerto del driver (`PYSPARK_PORT`, 4040 por
defecto). Si la UI no responde, se cae al conteo puro del tiempo por Step.

El progreso por Step se infiere del stdout del hijo (los mensajes
`Executing step:`/`Step finished:` del logger del framework); no requiere
cambios en el pipeline.

Env vars:
    SUPERVISOR_COMMAND           comando a vigilar
                                 (default "$PYSPARK_DRIVER_PYTHON $APP_MAIN_SCRIPT")
    SUPERVISOR_STEP_TIMEOUT      seg. máx. en el mismo Step (default 7200)
    SUPERVISOR_STALL_TIMEOUT     seg. sin progreso de tareas para declarar zombie (600)
    SUPERVISOR_WATCHDOG_INTERVAL periodo del watchdog en seg. (30)
    SUPERVISOR_MAX_RESTARTS      reinicios antes de rendirse (10)
    PYSPARK_PORT              puerto de la Spark UI      (4040)

Los logs del supervisor y del hijo pasan por la suite de logging del
framework (``libs.data_engineering_toolbox.context.logging``): consola +
archivo ``.log`` bajo ``LOGS_DIR_LINUX`` cuando
``opt/environment_vars.sh`` está cargado.

Uso:
    source opt/environment_vars.sh && python supervisor.py
    # o: opt/run-supervised.sh   (supervisado)
    # o: opt/run.sh              (sin supervisor)
"""
import importlib.util
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import threading
import time
import urllib.request

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _THIS_DIR)


def _load_module(name: str, *relparts: str):
    """Carga un módulo del framework por ruta sin arrastrar los ``__init__``
    de los paquetes (que importan pyspark y exigen ``PYLIB``)."""
    mod_path = os.path.join(_THIS_DIR, *relparts)
    spec = importlib.util.spec_from_file_location(name, mod_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_log_suite = _load_module(
    "framework_logging",
    "libs", "data_engineering_toolbox", "context", "logging.py")
setup_logging = _log_suite.setup_logging
get_logger = _log_suite.get_logger

# Manejo de paths y plataforma con la suite del framework: LocalPath
# (filesystem local — los .log del driver lo son también en el clúster)
# y os_mode() ("unix" default | "wsl" | "windows").
_path_local = _load_module(
    "framework_path_local",
    "libs", "data_engineering_toolbox", "path", "local.py")
LocalPath = _path_local.LocalPath
os_mode = _path_local.os_mode
_IS_WINDOWS = os_mode() == "windows"

STEP_START_RE = re.compile(r"Executing step:\s*(\S+)")
STEP_DONE_RE = re.compile(r"Step finished:\s*(\S+)")


def _env_seconds(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


COMMAND = os.environ.get("SUPERVISOR_COMMAND") or (
    f"{os.environ.get('PYSPARK_DRIVER_PYTHON', 'python')} "
    f"{os.environ.get('APP_MAIN_SCRIPT', 'examples/main.py')}")
STEP_TIMEOUT = _env_seconds("SUPERVISOR_STEP_TIMEOUT", 2 * 3600)
STALL_TIMEOUT = _env_seconds("SUPERVISOR_STALL_TIMEOUT", 600)
WATCHDOG_INTERVAL = _env_seconds("SUPERVISOR_WATCHDOG_INTERVAL", 30)
MAX_RESTARTS = int(os.environ.get("SUPERVISOR_MAX_RESTARTS", 10))
SPARK_UI_PORT = int(os.environ.get("PYSPARK_PORT", 4040))

# Loggers del supervisor: propagan a root -> consola + .log (si
# LOGS_DIR_LINUX/APP_LOG_FILE están configurados).
logger = get_logger("supervisor")
child_logger = get_logger("supervisor.child")


def log(msg: str) -> None:
    logger.info("%s", msg)


class PipelineMonitor:
    """Estado de progreso del subproceso, alimentado línea a línea por su stdout."""

    def __init__(self) -> None:
        self.current_step: str = "(startup)"
        self.step_started_at: float = time.time()
        self.finished_steps: set = set()
        self.last_tasks = None            # (numActiveTasks, numCompleteTasks)
        self.last_progress_at: float = time.time()   # último avance observado

    def feed_line(self, line: str) -> None:
        match = STEP_START_RE.search(line)
        if match:
            self.current_step = match.group(1)
            self.step_started_at = time.time()
            self.last_progress_at = time.time()
            return
        match = STEP_DONE_RE.search(line)
        if match:
            self.finished_steps.add(match.group(1))
            self.current_step = "(startup)"
            self.step_started_at = time.time()
            self.last_progress_at = time.time()


def _http_json(url: str, timeout: float = 5.0):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read())


def spark_task_counts(port: int):
    """(numActiveTasks, numCompleteTasks) de los stages activos de la app.

    Devuelve None si la Spark UI del driver no responde (sesión caída, UI
    deshabilitada o red bloqueada) y (0, 0) si no hay stages activos.
    """
    try:
        apps = _http_json(f"http://localhost:{port}/api/v1/applications")
        if not apps:
            return None
        app_id = apps[0]["id"]
        stages = _http_json(
            f"http://localhost:{port}/api/v1/applications/{app_id}/stages?status=active")
        active = sum(s.get("numActiveTasks", 0) for s in stages)
        complete = sum(s.get("numCompleteTasks", 0) for s in stages)
        return (active, complete)
    except Exception:
        return None


def is_zombie(monitor: PipelineMonitor) -> bool:
    """Decide si la sesión está zombie (sin avance) o sigue calculando.

    Solo se evalúa cuando el Step actual supera STEP_TIMEOUT. Se prefiere la
    evidencia de la Spark UI (tareas completadas creciendo = computando);
    si la UI no responde, se decide solo por el tiempo en el Step.
    """
    now = time.time()
    if now - monitor.step_started_at < STEP_TIMEOUT:
        return False
    #
    counts = spark_task_counts(SPARK_UI_PORT)
    if counts is None:
        # UI inaccesible y el Step ya agotó su ventana: solo queda contar.
        return True
    #
    if monitor.last_tasks is None:
        # Primera muestra: sin baseline no se puede distinguir calculando de
        # zombie; se decide en el siguiente tick del watchdog.
        monitor.last_tasks = counts
        return False
    #
    if counts[1] > monitor.last_tasks[1]:
        monitor.last_progress_at = now    # las tareas completadas crecen: calculando
    monitor.last_tasks = counts
    return now - monitor.last_progress_at >= STALL_TIMEOUT


def _drain_stdout(proc: subprocess.Popen, monitor: PipelineMonitor) -> None:
    """Copia el stdout del hijo al log del supervisor (consola + .log) y
    alimenta el monitor de progreso por Step."""
    for line in proc.stdout:
        line = line.rstrip("\n")
        child_logger.info("%s", line)
        monitor.feed_line(line)


def _kill_tree(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, AttributeError, PermissionError):
        try:
            proc.terminate()
        except ProcessLookupError:
            return
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, AttributeError, PermissionError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass


def run_child(monitor: PipelineMonitor) -> int:
    """Lanza el hijo y lo vigila; devuelve su exit code o -9 si se mató por zombie."""
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    if _IS_WINDOWS:
        # shlex POSIX se comería los "\" de las rutas Windows; posix=False
        # conserva la ruta y las comillas (se quitan después — CreateProcess
        # ya recibe argv, no vuelve a parsear)
        child_args = [a.strip("'\"") for a in shlex.split(COMMAND, posix=False)]
    else:
        # unix / wsl: shlex POSIX normal
        child_args = shlex.split(COMMAND)
    proc = subprocess.Popen(
        child_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        # grupo de procesos propio: killpg mata el árbol (unix/wsl)
        start_new_session=(not _IS_WINDOWS),
    )
    threading.Thread(target=_drain_stdout, args=(proc, monitor), daemon=True).start()
    while proc.poll() is None:
        if is_zombie(monitor):
            log(f"Sesión zombie detectada en step '{monitor.current_step}' "
                f"(sin progreso de tareas en {STALL_TIMEOUT:.0f}s). Matando y reiniciando...")
            _kill_tree(proc)
            return -9
        time.sleep(WATCHDOG_INTERVAL)
    return proc.returncode


def main() -> int:
    log(f"Supervisando '{COMMAND}' "
        f"(step_timeout={STEP_TIMEOUT:.0f}s, stall_timeout={STALL_TIMEOUT:.0f}s, "
        f"max_restarts={MAX_RESTARTS}, ui_port={SPARK_UI_PORT})")
    restarts = 0
    while True:
        monitor = PipelineMonitor()
        return_code = run_child(monitor)
        if return_code == 0:
            log("Pipeline terminado correctamente.")
            return 0
        restarts += 1
        step = monitor.current_step
        log(f"Proceso terminado (rc={return_code}) durante step '{step}'. "
            f"Reinicio {restarts}/{MAX_RESTARTS}; los parquets ya escritos se recargan.")
        if restarts > MAX_RESTARTS:
            log("Máximo de reinicios alcanzado; abandono.")
            return 1
        time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())
