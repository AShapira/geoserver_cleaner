from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from typing import Callable, List, Optional, Set

import geoserver_store_report as report

from app import db
from app.config import Settings


LOGGER = logging.getLogger("geoserver_cleaner.inventory")
ProgressCallback = Callable[[dict, str], None]


def create_client(settings: Settings) -> Optional[report.GeoServerClient]:
    if not settings.geoserver_url:
        return None
    return report.GeoServerClient(
        base_url=settings.geoserver_url,
        username=settings.geoserver_username,
        password=settings.geoserver_password,
        timeout=settings.timeout,
        insecure=settings.insecure,
    )


def effective_catalog_source(settings: Settings) -> str:
    if settings.catalog_source != "auto":
        return settings.catalog_source
    workspaces_root = os.path.join(settings.data_dir, "workspaces")
    return "filesystem" if os.path.isdir(workspaces_root) else "rest"


def infer_store_kind(store_type: str) -> str:
    lowered = (store_type or "").strip().lower()
    if lowered in {"geotiff", "imagemosaic", "worldimage", "arcgrid"}:
        return "coveragestores"
    if lowered:
        return "datastores"
    return ""


def collect_inventory_rows(
    settings: Settings,
    progress_callback: Optional[ProgressCallback] = None,
) -> List[dict]:
    data_dir = os.path.abspath(settings.data_dir)
    data_root = os.path.join(data_dir, "data")
    excluded_workspaces = set(settings.excluded_workspaces)
    catalog_source = effective_catalog_source(settings)
    client = create_client(settings)

    rows: List[dict] = []
    referenced_roots: List[str] = []
    referenced_files: Set[str] = set()

    if progress_callback is not None:
        progress_callback(
            {
                "phase": "discovering",
                "discovered_store_count": 0,
                "processed_stores": 0,
                "total_stores": None,
                "progress_percent": 0.0,
                "eta_seconds": None,
            },
            "Discovering stores in GeoServer catalog",
        )

    def on_discovery_progress(discovered_count: int, workspace: str) -> None:
        if progress_callback is None:
            return
        progress_callback(
            {
                "phase": "discovering",
                "discovered_store_count": discovered_count,
                "current_workspace": workspace,
                "processed_stores": 0,
                "total_stores": None,
                "progress_percent": 0.0,
                "eta_seconds": None,
            },
            "Discovered {} stores so far".format(discovered_count),
        )

    if catalog_source in {"auto", "filesystem"}:
        try:
            workspace_names, catalog_stores = report.list_catalog_workspaces(
                data_dir,
                progress_callback=on_discovery_progress,
            )
            rest_error_rows: List[dict] = []
            LOGGER.info(
                "Discovered %d workspace(s) and %d store(s) via filesystem catalog",
                len(workspace_names),
                len(catalog_stores),
            )
        except Exception as exc:
            if catalog_source == "filesystem":
                raise
            LOGGER.warning("Filesystem catalog discovery failed, falling back to REST: %s", exc)
            workspace_names, catalog_stores, rest_error_rows = report.collect_rest_catalog(
                client,
                data_dir,
                progress_callback=on_discovery_progress,
            )
    else:
        workspace_names, catalog_stores, rest_error_rows = report.collect_rest_catalog(
            client,
            data_dir,
            progress_callback=on_discovery_progress,
        )

    rows.extend(rest_error_rows)
    for workspace in workspace_names:
        if workspace.lower() in excluded_workspaces:
            fallback_root = os.path.join(data_dir, "data", workspace)
            if os.path.isdir(fallback_root):
                referenced_roots.append(report.normalize_path(fallback_root))

    included_stores = [
        item for item in catalog_stores if item.workspace.lower() not in excluded_workspaces
    ]
    total_stores = len(included_stores)
    started_at = time.monotonic()
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "stores",
                "processed_stores": 0,
                "total_stores": total_stores,
                "progress_percent": 0.0,
                "eta_seconds": None,
            },
            "Scanning stores 0/{}".format(total_stores),
        )
    with ThreadPoolExecutor(max_workers=settings.workers or report.worker_default()) as executor:
        future_map = {
            executor.submit(report.process_catalog_store, catalog_store, data_dir): catalog_store
            for catalog_store in included_stores
        }
        completed = 0
        progress_interval = 10 if total_stores <= 100 else 25
        for future in as_completed(future_map):
            catalog_store = future_map[future]
            try:
                processed = future.result()
            except Exception as exc:
                row = report.build_error_row(
                    workspace=catalog_store.workspace,
                    store_name=catalog_store.store_name,
                    status="error",
                    notes=str(exc),
                    store_type=catalog_store.store_type,
                )
            else:
                row = dict(processed.row)
                if processed.referenced_root:
                    referenced_roots.append(processed.referenced_root)
                if processed.referenced_files:
                    referenced_files.update(processed.referenced_files)
            row["store_kind"] = catalog_store.store_kind or infer_store_kind(row.get("store_type", ""))
            row["normalized_path"] = report.normalize_path(row.get("resolved_path", "")) if row.get("resolved_path") else ""
            rows.append(row)
            completed += 1
            if progress_callback is not None and (
                completed == 1
                or completed == total_stores
                or completed % progress_interval == 0
            ):
                elapsed = max(time.monotonic() - started_at, 0.001)
                rate = completed / elapsed
                remaining = max(total_stores - completed, 0)
                eta_seconds = int(remaining / rate) if remaining and rate > 0 else 0
                progress_callback(
                    {
                        "phase": "stores",
                        "processed_stores": completed,
                        "total_stores": total_stores,
                        "progress_percent": round((completed / total_stores) * 100, 1) if total_stores else 100.0,
                        "eta_seconds": eta_seconds,
                    },
                    "Scanning stores {}/{}".format(completed, total_stores),
                )

    if progress_callback is not None:
        progress_callback(
            {
                "phase": "orphans",
                "processed_stores": total_stores,
                "total_stores": total_stores,
                "progress_percent": 100.0,
                "eta_seconds": None,
            },
            "Calculating orphaned data",
        )

    for orphan_row in report.collect_orphans(data_root, referenced_roots, referenced_files):
        orphan_row["store_kind"] = ""
        orphan_row["normalized_path"] = (
            report.normalize_path(orphan_row.get("resolved_path", "")) if orphan_row.get("resolved_path") else ""
        )
        rows.append(orphan_row)

    rows.sort(
        key=lambda item: (
            item.get("row_kind", ""),
            item.get("workspace", ""),
            item.get("store_name", ""),
            item.get("resolved_path", ""),
        )
    )
    return rows


def settings_with_excluded_workspaces(settings: Settings, excluded_workspaces_raw: Optional[str]) -> Settings:
    if excluded_workspaces_raw is None:
        return settings
    return replace(settings, excluded_workspaces_raw=excluded_workspaces_raw)


def run_inventory_scan(
    settings: Settings,
    db_path: str,
    excluded_workspaces_raw: Optional[str] = None,
    progress_callback: Optional[ProgressCallback] = None,
) -> int:
    effective_settings = settings_with_excluded_workspaces(settings, excluded_workspaces_raw)
    catalog_source = effective_catalog_source(effective_settings)
    run_id = db.create_inventory_run(
        db_path,
        catalog_source=catalog_source,
        excluded_workspaces=effective_settings.excluded_workspaces,
        geoserver_url=effective_settings.geoserver_url,
        data_dir=os.path.abspath(effective_settings.data_dir),
    )
    try:
        rows = collect_inventory_rows(effective_settings, progress_callback=progress_callback)
        store_rows = [row for row in rows if row.get("row_kind") == "store"]
        orphan_rows = [row for row in rows if row.get("row_kind") == "orphaned"]
        issue_rows = [row for row in store_rows if row.get("status") != "ok"]
        tracked_size_bytes = sum(int(row.get("size_bytes", 0) or 0) for row in store_rows if row.get("status") == "ok")
        db.replace_run_rows(db_path, run_id, rows)
        db.finalize_inventory_run(
            db_path,
            run_id,
            status="completed",
            store_count=len(store_rows),
            orphan_count=len(orphan_rows),
            issue_count=len(issue_rows),
            tracked_size_bytes=tracked_size_bytes,
            notes="",
        )
        return run_id
    except Exception as exc:
        db.fail_inventory_run(db_path, run_id, str(exc))
        raise
