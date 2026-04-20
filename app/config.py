from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Tuple

from app.logging_utils import default_log_path, normalize_log_level
from app.reporting.core import ExternalPathMapping, parse_excluded_workspaces, parse_external_path_mappings, worker_default


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalized_http_path(value: str) -> str:
    raw = value.strip()
    if not raw:
        raise ValueError("APP_MCP_HTTP_PATH must not be empty.")
    parts = [segment for segment in raw.split("/") if segment]
    if not parts:
        raise ValueError("APP_MCP_HTTP_PATH must not resolve to the root path.")
    return "/" + "/".join(parts)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError("{} must be greater than 0.".format(name))
    return value


@dataclass(frozen=True)
class Settings:
    geoserver_url: str
    geoserver_username: str
    geoserver_password: str
    data_dir: str
    external_path_mappings: Tuple[ExternalPathMapping, ...]
    catalog_source: str
    excluded_workspaces_raw: str
    insecure: bool
    timeout: int
    workers: int
    database_path: str
    export_dir: str
    page_size_default: int
    page_size_max: int
    app_title: str
    enable_mcp_http: bool
    mcp_http_path: str
    app_log_level: str
    app_log_path: str
    app_log_max_bytes: int
    app_log_backup_count: int

    @classmethod
    def from_env(cls) -> "Settings":
        data_dir = os.path.abspath(
            os.getenv("GEOSERVER_DATA_DIR", os.path.join(os.getcwd(), "geoserver_test", "geoserver_data"))
        )
        database_path = os.path.abspath(
            os.getenv("APP_DATABASE_PATH", os.path.join(os.getcwd(), "app_data", "geoserver_cleaner.sqlite3"))
        )
        return cls(
            geoserver_url=os.getenv("GEOSERVER_URL", "http://localhost:8081/geoserver"),
            geoserver_username=os.getenv("GEOSERVER_USER", "admin"),
            geoserver_password=os.getenv("GEOSERVER_PASSWORD", "geoserver"),
            data_dir=data_dir,
            external_path_mappings=parse_external_path_mappings(
                os.getenv("GEOSERVER_EXTERNAL_PATH_MAPPINGS", "")
            ),
            catalog_source=os.getenv("GEOSERVER_CATALOG_SOURCE", "auto").strip().lower() or "auto",
            excluded_workspaces_raw=os.getenv("GEOSERVER_EXCLUDE_WORKSPACES", ""),
            insecure=_bool_env("GEOSERVER_INSECURE", False),
            timeout=int(os.getenv("GEOSERVER_TIMEOUT", "60")),
            workers=int(os.getenv("GEOSERVER_WORKERS", str(worker_default()))),
            database_path=database_path,
            export_dir=os.path.abspath(
                os.getenv("APP_EXPORT_DIR", os.path.join(os.getcwd(), "app_exports"))
            ),
            page_size_default=int(os.getenv("APP_PAGE_SIZE_DEFAULT", "100")),
            page_size_max=int(os.getenv("APP_PAGE_SIZE_MAX", "500")),
            app_title=os.getenv("APP_TITLE", "GeoServer Cleaner"),
            enable_mcp_http=_bool_env("APP_ENABLE_MCP_HTTP", False),
            mcp_http_path=_normalized_http_path(os.getenv("APP_MCP_HTTP_PATH", "/mcp")),
            app_log_level=normalize_log_level(os.getenv("APP_LOG_LEVEL", "INFO")),
            app_log_path=os.path.abspath(os.getenv("APP_LOG_PATH", default_log_path(database_path))),
            app_log_max_bytes=_int_env("APP_LOG_MAX_BYTES", 10 * 1024 * 1024),
            app_log_backup_count=_int_env("APP_LOG_BACKUP_COUNT", 5),
        )

    @property
    def excluded_workspaces(self) -> List[str]:
        return sorted(parse_excluded_workspaces(self.excluded_workspaces_raw))
