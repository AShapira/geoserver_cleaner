from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence

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
    can_delete_data: bool
    data_scope: str
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
    internal_root = report.normalize_path(settings.data_dir)

    for row in ordered_rows:
        reason_parts: List[str] = []
        can_delete_store = True
        can_delete_data = False
        data_scope = "unknown"
        normalized_path = str(row["normalized_path"] or "")
        resolved_path = str(row["resolved_path"] or "")
        store_kind = str(row["store_kind"] or "")
        path_kind = str(row["path_kind"] or "")

        if row["row_kind"] != "store":
            can_delete_store = False
            data_scope = "orphaned"
            reason_parts.append("Orphan rows are report-only and cannot be deleted here.")
        elif not row["workspace"] or not row["store_name"] or not store_kind:
            can_delete_store = False
            data_scope = "invalid"
            reason_parts.append("Store metadata is incomplete.")
        else:
            if not normalized_path:
                data_scope = "unresolved"
                reason_parts.append("GeoServer will delete store configuration only; path is unresolved or missing.")
            elif path_kind == "missing":
                data_scope = "missing"
                reason_parts.append("GeoServer will delete store configuration only; path is unresolved or missing.")
            elif not report.path_under_any_root(normalized_path, [internal_root]):
                data_scope = "external"
                reason_parts.append("GeoServer will delete store configuration only; data is outside data_dir.")
            else:
                owners = path_map.get(normalized_path, [])
                owner_ids = {int(owner["id"]) for owner in owners}
                if owner_ids - selected_ids:
                    data_scope = "shared"
                    reason_parts.append(
                        "GeoServer will delete store configuration only; the data path is shared with another store."
                    )
                else:
                    data_scope = "internal"
                    can_delete_data = True
                    reason_parts.append("GeoServer will delete store configuration and internal data.")

        if can_delete_data and resolved_path:
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
                can_delete_data=can_delete_data,
                data_scope=data_scope,
                reason=" ".join(reason_parts).strip(),
            )
        )

    if not items:
        warnings.append("No valid stores were selected.")
    else:
        warnings.append("GeoServer REST deletion is always used with recurse=true and purge=all.")
        warnings.append("Delete Data = Yes means GeoServer can purge data inside data_dir for that store.")
        warnings.append("Delete Data = No means store deletion is configuration-only or the data ownership is uncertain.")

    unique_paths = sorted(set(delete_paths), key=lambda value: value.lower())
    return {
        "items": items,
        "selected_ids": [item.store_id for item in items if item.can_delete_store],
        "delete_paths": unique_paths,
        "warnings": warnings,
        "blocked_count": len([item for item in items if not item.can_delete_store]),
        "delete_data_count": len(unique_paths),
    }


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
            "purge_candidate_paths": preview["delete_paths"],
            "failed_items": failed_items,
        },
    )
    return {
        "deleted_stores": deleted_store_keys,
        "deleted_count": len(deleted_store_keys),
        "delete_data_count": int(preview["delete_data_count"]),
        "purge_candidate_paths": list(preview["delete_paths"]),
        "failed_items": failed_items,
        "failed_count": len(failed_items),
        "processed_delete_items": total_items,
        "total_delete_items": total_items,
        "remaining_delete_items": 0,
    }
