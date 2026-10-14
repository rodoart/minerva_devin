"""Módulo logging con las funciones setup_logging, get_logger."""

import importlib.util
import logging
import logging.config
import os
import functools
from datetime import datetime
from typing import Callable, ParamSpec, TypeVar, Optional
import sys

try:
    # Contexto de paquete (pipeline, tests).
    from ..path.local import LocalPath
except (ImportError, ValueError):
    # Cargado suelto por importlib (supervisor): el __init__ del paquete
    # requeriría pyspark; ``path/local.py`` es autocontenido.
    _local_py = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        os.pardir, "path", "local.py")
    _spec = importlib.util.spec_from_file_location(
        "framework_path_local", _local_py)
    _mod = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    LocalPath = _mod.LocalPath

DEFAULT_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

_initialized = False


def _resolve_log_file(log_file: Optional[str]) -> Optional[str]:
    """Resuelve la ruta del archivo ``.log`` de la sesión.

    Orden de precedencia:

    1. ``log_file`` explícito (parámetro).
    2. ``APP_LOG_FILE``: ruta completa; ``off``/``none`` desactiva el
       archivo (solo consola).
    3. ``LOGS_DIR_LINUX`` (la exporta ``opt/environment_vars.sh``):
       ``<LOGS_DIR_LINUX>/<APP_NAME>_<YYYY-mm-dd_HH-MM-SS>.log``.
    4. Sin nada configurado -> ``None`` (solo consola).
    """
    if log_file:
        return log_file
    env_file = os.getenv("APP_LOG_FILE", "")
    if env_file.lower() in ("off", "none"):
        return None
    if env_file:
        return env_file
    logs_dir = os.getenv("LOGS_DIR_LINUX")
    if not logs_dir:
        return None
    app = os.getenv("APP_NAME", "app")
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return str(LocalPath(logs_dir).joinpath(f"{app}_{ts}.log"))


def setup_logging(level: Optional[str]=None, log_file:Optional[str]=None, force_reload:Optional[bool]=False) -> Optional[str]:
    """Función que configura logging.

    Configura siempre un handler de consola (stdout) y, si se resuelve una
    ruta de log (ver :func:`_resolve_log_file`), un ``FileHandler`` — así
    los logs quedan a la vez en la terminal y en un archivo ``.log``.

    Parameters
    ----------
    level:
        Nivel (``APP_LOG_LEVEL`` si no se pasa; default ``INFO``).
    log_file:
        Ruta del archivo de log; si es ``None`` se resuelve de las
        variables de entorno.
    force_reload:
        ``True`` reconfigura aunque ya estuviera inicializado.

    Returns
    -------
    Optional[str]
        Ruta del ``.log`` activo, o ``None`` si solo hay consola.
    """
    if force_reload is None:
        force_reload = False
    #
    global _initialized
    if _initialized and not force_reload:
        return None
    _initialized = True
    #
    log_file = _resolve_log_file(log_file)
    level = (level or os.getenv("APP_LOG_LEVEL", "INFO")).upper()
    if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        level = "DEBUG"
    #
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": DEFAULT_FORMAT,
                "datefmt": DEFAULT_DATEFMT,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": level,
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "app": {
                "level": level,
                "handlers": ["console"],
                "propagate": False,
            },
        },
        # root con handler de consola: los loggers por módulo
        # (libs.*, pipelines.*, examples.*) propagan aquí y emiten.
        "root": {
            "level": level,
            "handlers": ["console"],
        },
    }
    if log_file is not None:
        LocalPath(log_file).resolve().parent.mkdir(parents=True, exist_ok=True)
        config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "level": level,
            "formatter": "standard",
            "filename": log_file,
            "mode": "a",
        }
        config["loggers"]["app"]["handlers"].append("file")
        config["root"]["handlers"].append("file")
    #
    logging.config.dictConfig(config)
    return log_file


def get_logger(name: str) -> logging.Logger:
    """Función que obtiene logger."""
    setup_logging()
    return logging.getLogger(name)


P = ParamSpec("P")
R = TypeVar("R")


def log_method(func: Callable[P, R]) -> Callable[P, R]:
    logger = logging.getLogger(__name__)
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        self = args[0]
        #
        logger.info("Iniciando %s", func.__qualname__)
        #
        try:
            return func(*args, **kwargs)
        #
        except Exception:
            logger.exception(
                "Error en %s",
                func.__qualname__
            )
            raise
    #
    return wrapper
