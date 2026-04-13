from __future__ import annotations

import logging
from dataclasses import dataclass

from app import db
from app.config import Settings
from app.jobs import JobManager
from app.logging_utils import configure_app_logging


LOGGER = logging.getLogger("geoserver_cleaner.runtime")


@dataclass(frozen=True)
class AppRuntime:
    settings: Settings
    job_manager: JobManager


def build_runtime(settings: Settings | None = None) -> AppRuntime:
    runtime_settings = settings or Settings.from_env()
    configure_app_logging(runtime_settings)
    LOGGER.info(
        "Initializing application runtime",
        extra={
            "event": "runtime_init_start",
            "database_path": runtime_settings.database_path,
            "data_dir": runtime_settings.data_dir,
            "export_dir": runtime_settings.export_dir,
            "catalog_source": runtime_settings.catalog_source,
            "workers": runtime_settings.workers,
            "mcp_http_enabled": runtime_settings.enable_mcp_http,
            "mcp_http_path": runtime_settings.mcp_http_path,
            "log_level": runtime_settings.app_log_level,
            "log_path": runtime_settings.app_log_path,
        },
    )
    db.init_db(runtime_settings.database_path)
    LOGGER.info(
        "Database initialized",
        extra={
            "event": "runtime_db_initialized",
            "database_path": runtime_settings.database_path,
        },
    )
    runtime = AppRuntime(
        settings=runtime_settings,
        job_manager=JobManager(runtime_settings, runtime_settings.database_path),
    )
    LOGGER.info(
        "Application runtime initialized",
        extra={
            "event": "runtime_init_complete",
            "database_path": runtime_settings.database_path,
            "log_path": runtime_settings.app_log_path,
        },
    )
    return runtime
