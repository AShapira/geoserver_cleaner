from __future__ import annotations

from dataclasses import dataclass

from app import db
from app.config import Settings
from app.jobs import JobManager


@dataclass(frozen=True)
class AppRuntime:
    settings: Settings
    job_manager: JobManager


def build_runtime(settings: Settings | None = None) -> AppRuntime:
    runtime_settings = settings or Settings.from_env()
    db.init_db(runtime_settings.database_path)
    return AppRuntime(
        settings=runtime_settings,
        job_manager=JobManager(runtime_settings, runtime_settings.database_path),
    )
