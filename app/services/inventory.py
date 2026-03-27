from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Set

import geoserver_store_report as report

from app import db
from app.config import Settings


LOGGER = logging.getLogger("cleanup_app.inventory")


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


def collect_inventory_rows(settings: Settings) -> List[dict]:
    data_dir = os.path.abspath(settings.data_dir)
    data_root = os.path.join(data_dir, "data")
    excluded_workspaces = set(settings.excluded_workspaces)
    catalog_source = effective_catalog_source(settings)
    client = create_client(settings)

    rows: List[dict] = []
    referenced_roots: List[str] = []
    referenced_files: Set[str] = set()

    if catalog_source in {"auto", "filesystem"}:
        try:
            workspace_names, catalog_stores = report.list_catalog_workspaces(data_dir)
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
            workspace_names, catalog_stores, rest_error_rows = report.collect_rest_catalog(client, data_dir)
    else:
        workspace_names, catalog_stores, rest_error_rows = report.collect_rest_catalog(client, data_dir)

    rows.extend(rest_error_rows)
    for workspace in workspace_names:
        if workspace.lower() in excluded_workspaces:
            fallback_root = os.path.join(data_dir, "data", workspace)
            if os.path.isdir(fallback_root):
                referenced_roots.append(report.normalize_path(fallback_root))

    included_stores = [
        item for item in catalog_stores if item.workspace.lower() not in excluded_workspaces
    ]
    with ThreadPoolExecutor(max_workers=settings.workers or report.worker_default()) as executor:
        future_map = {
            executor.submit(report.process_catalog_store, catalog_store, data_dir): catalog_store
            for catalog_store in included_stores
        }
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


def run_inventory_scan(settings: Settings, db_path: str) -> int:
    catalog_source = effective_catalog_source(settings)
    run_id = db.create_inventory_run(
        db_path,
        catalog_source=catalog_source,
        excluded_workspaces=settings.excluded_workspaces,
        geoserver_url=settings.geoserver_url,
        data_dir=os.path.abspath(settings.data_dir),
    )
    try:
        rows = collect_inventory_rows(settings)
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
