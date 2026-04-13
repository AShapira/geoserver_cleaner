from __future__ import annotations

import json
import logging
import os
import sys
import threading
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any


_STANDARD_RECORD_FIELDS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}
_LOG_LEVEL_ALIASES = {
    "DEBUG": "DEBUG",
    "INFO": "INFO",
    "INFOW": "INFO",
    "WARN": "WARN",
    "WARNING": "WARN",
    "ERROR": "ERROR",
    "FATAL": "FATAL",
    "CRITICAL": "FATAL",
}
_LOG_LEVEL_VALUES = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "FATAL": logging.CRITICAL,
}
_CONFIG_LOCK = threading.Lock()
_LAST_CONFIG: tuple[str, str, int, int] | None = None


def normalize_log_level(value: str) -> str:
    normalized = (value or "INFO").strip().upper()
    if normalized not in _LOG_LEVEL_ALIASES:
        raise ValueError(
            "APP_LOG_LEVEL must be one of DEBUG, INFO, WARN, ERROR, or FATAL."
        )
    return _LOG_LEVEL_ALIASES[normalized]


def default_log_path(database_path: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(database_path))
    return os.path.join(base_dir, "logs", "geoserver_cleaner.log")


def _coerce_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(key): _coerce_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_coerce_json_value(item) for item in value]
    return str(value)


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "process": record.process,
            "thread": record.threadName,
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_FIELDS or key.startswith("_"):
                continue
            payload[key] = _coerce_json_value(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = record.stack_info
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def configure_app_logging(settings: Any) -> None:
    global _LAST_CONFIG

    log_level_name = normalize_log_level(str(getattr(settings, "app_log_level", "INFO")))
    log_path = os.path.abspath(str(getattr(settings, "app_log_path")))
    max_bytes = int(getattr(settings, "app_log_max_bytes"))
    backup_count = int(getattr(settings, "app_log_backup_count"))
    desired_config = (log_level_name, log_path, max_bytes, backup_count)

    with _CONFIG_LOCK:
        if _LAST_CONFIG == desired_config:
            return

        logging.addLevelName(logging.WARNING, "WARN")
        logging.addLevelName(logging.CRITICAL, "FATAL")

        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()

        root_logger.setLevel(_LOG_LEVEL_VALUES[log_level_name])

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(_LOG_LEVEL_VALUES[log_level_name])
        stream_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s %(message)s",
                datefmt="%H:%M:%S",
            )
        )

        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(_LOG_LEVEL_VALUES[log_level_name])
        file_handler.setFormatter(JsonLogFormatter())

        root_logger.addHandler(stream_handler)
        root_logger.addHandler(file_handler)

        for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
            logger = logging.getLogger(logger_name)
            logger.handlers.clear()
            logger.propagate = True
            logger.setLevel(_LOG_LEVEL_VALUES[log_level_name])

        logging.captureWarnings(True)
        _LAST_CONFIG = desired_config


def shutdown_app_logging() -> None:
    global _LAST_CONFIG

    with _CONFIG_LOCK:
        root_logger = logging.getLogger()
        for handler in list(root_logger.handlers):
            root_logger.removeHandler(handler)
            handler.close()
        _LAST_CONFIG = None
        logging.shutdown()
