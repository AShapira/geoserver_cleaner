from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Set

import geoserver_store_report as report

from app import db
from app.config import Settings
from app.services import geoserver

ProgressCallback = Callable[[dict, str], None]


@dataclass
class DeletePlanItem:
    store_id: int
    workspace: str
    store_name: str
    store_type: str
    store_kind: str
    status: str
    resolved_path: str
    normalized_path: str
    can_delete_store: bool
    can_delete_files: bool
    reason: str


def parse_selected_ids(raw: str) -> List[int]:
    values = []
    for item in (raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError:
            continue
    return sorted(set(values))


def build_delete_preview(
    db_path: str,
    settings: Settings,
    run_id: int,
    store_ids: Sequence[int],
) -> Dict[str, object]:
    rows = db.get_rows_by_ids(db_path, run_id, store_ids)
    rows_by_id = {int(row["id"]): row for row in rows}
    ordered_rows = [rows_by_id[store_id] for store_id in store_ids if store_id in rows_by_id]
    path_map = db.get_path_owners(
        db_path,
        run_id,
        [str(row["normalized_path"]) for row in ordered_rows if row["normalized_path"]],
    )
    selected_ids = {int(row["id"]) for row in ordered_rows}
    items: List[DeletePlanItem] = []
    delete_paths: List[str] = []
    warnings: List[str] = []

    for row in ordered_rows:
        reason_parts: List[str] = []
        can_delete_store = True
        can_delete_files = settings.allow_physical_delete
        normalized_path = str(row["normalized_path"] or "")
        resolved_path = str(row["resolved_path"] or "")
        store_kind = str(row["store_kind"] or "")

        if row["row_kind"] != "store":
            can_delete_store = False
            can_delete_files = False
            reason_parts.append("Only store rows can be deleted.")
        if not row["workspace"] or not row["store_name"] or not store_kind:
            can_delete_store = False
            reason_parts.append("Store metadata is incomplete.")
        if normalized_path:
            owners = path_map.get(normalized_path, [])
            owner_ids = {int(owner["id"]) for owner in owners}
            if owner_ids - selected_ids:
                can_delete_files = False
                reason_parts.append("Physical delete blocked because the path is shared with another store.")
            if not report.path_under_any_root(normalized_path, settings.allowed_data_roots):
                can_delete_files = False
                reason_parts.append("Physical delete blocked because the path is outside allowed roots.")
        else:
            can_delete_files = False
        if str(row["path_kind"] or "") == "missing":
            can_delete_files = False
            reason_parts.append("Resolved path is already missing.")
        if not settings.allow_physical_delete:
            can_delete_files = False
            reason_parts.append("Physical delete is disabled by configuration.")

        if can_delete_files and resolved_path:
            delete_paths.append(resolved_path)

        items.append(
            DeletePlanItem(
                store_id=int(row["id"]),
                workspace=str(row["workspace"]),
                store_name=str(row["store_name"]),
                store_type=str(row["store_type"]),
                store_kind=store_kind,
                status=str(row["status"]),
                resolved_path=resolved_path,
                normalized_path=normalized_path,
                can_delete_store=can_delete_store,
                can_delete_files=can_delete_files,
                reason=" ".join(reason_parts).strip(),
            )
        )

    if not items:
        warnings.append("No valid stores were selected.")
    if settings.allow_physical_delete:
        warnings.append("Physical deletion is enabled. Confirm the preview before executing.")
    else:
        warnings.append("Physical deletion is disabled. Only GeoServer configuration will be removed.")

    unique_paths = sorted(set(delete_paths), key=lambda value: value.lower())
    return {
        "items": items,
        "selected_ids": [item.store_id for item in items if item.can_delete_store],
        "delete_paths": unique_paths,
        "warnings": warnings,
        "blocked_count": len([item for item in items if not item.can_delete_store]),
        "file_delete_count": len(unique_paths),
    }


def delete_store_paths(paths: Sequence[str], settings: Settings) -> List[str]:
    deleted: List[str] = []
    for path in paths:
        normalized = report.normalize_path(path)
        if not report.path_under_any_root(normalized, settings.allowed_data_roots):
            raise RuntimeError("Refusing to delete path outside allowed roots: {}".format(path))
        if not os.path.exists(path):
            continue
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        deleted.append(path)
    return deleted


def execute_delete_job(
    db_path: str,
    settings: Settings,
    run_id: int,
    store_ids: Sequence[int],
    progress_callback: Optional[ProgressCallback] = None,
) -> Dict[str, object]:
    preview = build_delete_preview(db_path, settings, run_id, store_ids)
    items: List[DeletePlanItem] = preview["items"]
    deleted_store_keys: List[str] = []
    failed_items: List[str] = []
    deleted_paths: Set[str] = set()
    total_items = len(items)

    if progress_callback is not None:
        progress_callback(
            {
                "phase": "delete",
                "processed_delete_items": 0,
                "total_delete_items": total_items,
                "deleted_count": 0,
                "failed_count": 0,
                "remaining_delete_items": total_items,
            },
            "Deleted 0 stores, remaining {}".format(total_items),
        )

    for index, item in enumerate(items, start=1):
        if not item.can_delete_store:
            failed_items.append("{} / {}: {}".format(item.workspace, item.store_name, item.reason or "blocked"))
        else:
            try:
                geoserver.delete_store(settings, item.workspace, item.store_kind, item.store_name)
                deleted_store_keys.append("{}/{}".format(item.workspace, item.store_name))
            except Exception as exc:
                failed_items.append("{} / {}: {}".format(item.workspace, item.store_name, exc))
            else:
                if item.can_delete_files and item.resolved_path and item.normalized_path not in deleted_paths:
                    delete_store_paths([item.resolved_path], settings)
                    deleted_paths.add(item.normalized_path)
        if progress_callback is not None:
            remaining = max(total_items - index, 0)
            progress_callback(
                {
                    "phase": "delete",
                    "processed_delete_items": index,
                    "total_delete_items": total_items,
                    "deleted_count": len(deleted_store_keys),
                    "failed_count": len(failed_items),
                    "remaining_delete_items": remaining,
                },
                "Deleted {} stores, remaining {}".format(len(deleted_store_keys), remaining),
            )

    db.add_audit_event(
        db_path,
        "delete_execute",
        {
            "run_id": run_id,
            "selected_store_ids": list(store_ids),
            "deleted_stores": deleted_store_keys,
            "deleted_paths": sorted(deleted_paths),
            "failed_items": failed_items,
        },
    )
    return {
        "deleted_stores": deleted_store_keys,
        "deleted_count": len(deleted_store_keys),
        "deleted_paths": sorted(deleted_paths),
        "failed_items": failed_items,
        "failed_count": len(failed_items),
        "processed_delete_items": total_items,
        "total_delete_items": total_items,
        "remaining_delete_items": 0,
    }
