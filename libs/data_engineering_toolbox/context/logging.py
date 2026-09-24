"""Módulo logging con las funciones setup_logging, get_logger."""

import logging
import logging.config
import os
import functools
from typing import Callable, ParamSpec, TypeVar, Optional
import sys

from numpy import True_

DEFAULT_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

_initialized = False


def setup_logging(level: Optional[str]=None, log_file:Optional[str]=None, force_reload:Optional[bool]=False) -> None:
    """Función que configura logging."""
    if force_reload is None:
        force_reload = False
    #
    global _initialized
    if _initialized and not force_reload:
        return
    _initialized = True_
    #
    level = (level or os.getenv("MINERVA_LOG_LEVEL", "INFO")).upper()
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
            "minerva": {
                "level": level,
                "handlers": ["console"],
                "propagate": False,
            },
        },
    }
    if log_file is not None:
        config["handlers"]["file"] = {
            "class": "logging.FileHandler",
            "level": level,
            "formatter": "standard",
            "filename": log_file,
            "mode": "a",
        }
        config["loggers"]["minerva"]["handlers"].append("file")
    #
    logging.config.dictConfig(config)


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
