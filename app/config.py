from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

import geoserver_store_report as report


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    geoserver_url: str
    geoserver_username: str
    geoserver_password: str
    data_dir: str
    catalog_source: str
    excluded_workspaces_raw: str
    insecure: bool
    timeout: int
    workers: int
    database_path: str
    allow_physical_delete: bool
    allowed_data_roots_raw: str
    page_size_default: int
    page_size_max: int
    app_title: str

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = os.path.abspath(
            os.getenv("GEOSERVER_DATA_DIR", os.path.join(os.getcwd(), "geoserver_test", "geoserver_data"))
        )
        default_root = os.path.join(data_dir, "data")
        return cls(
            geoserver_url=os.getenv("GEOSERVER_URL", "http://localhost:8081/geoserver"),
            geoserver_username=os.getenv("GEOSERVER_USER", "admin"),
            geoserver_password=os.getenv("GEOSERVER_PASSWORD", "geoserver"),
            data_dir=data_dir,
            catalog_source=os.getenv("GEOSERVER_CATALOG_SOURCE", "auto").strip().lower() or "auto",
            excluded_workspaces_raw=os.getenv("GEOSERVER_EXCLUDE_WORKSPACES", ""),
            insecure=_bool_env("GEOSERVER_INSECURE", False),
            timeout=int(os.getenv("GEOSERVER_TIMEOUT", "60")),
            workers=int(os.getenv("GEOSERVER_WORKERS", str(report.worker_default()))),
            database_path=os.path.abspath(
                os.getenv("APP_DATABASE_PATH", os.path.join(os.getcwd(), "app_data", "geoserver_cleaner.sqlite3"))
            ),
            allow_physical_delete=_bool_env("ALLOW_PHYSICAL_DELETE", False),
            allowed_data_roots_raw=os.getenv("ALLOWED_DATA_ROOTS", default_root),
            page_size_default=int(os.getenv("APP_PAGE_SIZE_DEFAULT", "100")),
            page_size_max=int(os.getenv("APP_PAGE_SIZE_MAX", "500")),
            app_title=os.getenv("APP_TITLE", "GeoServer Cleaner"),
        )

    @property
    def excluded_workspaces(self) -> List[str]:
        return sorted(report.parse_excluded_workspaces(self.excluded_workspaces_raw))

    @property
    def allowed_data_roots(self) -> List[str]:
        roots = [item.strip() for item in self.allowed_data_roots_raw.split(",") if item.strip()]
        return [os.path.normcase(os.path.normpath(os.path.abspath(item))) for item in roots]
