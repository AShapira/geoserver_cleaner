from __future__ import annotations

import os
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple

import geoserver_store_report as report

from app import db
from app.config import Settings
from app.services import deletion


DEFAULT_FIND_LIMIT = 50
DEFAULT_HEAVIEST_LIMIT = 5
DEFAULT_ORPHAN_LIMIT = 50


def _row_to_dict(row) -> Dict[str, object]:
    if hasattr(row, "keys"):
        return {key: row[key] for key in row.keys()}
    return dict(row)


def _latest_completed_run(db_path: str):
    run = db.get_latest_completed_run(db_path)
    if run is None:
        raise RuntimeError("No completed inventory snapshot is available.")
    return run


def _get_target_run(db_path: str, run_id: Optional[int] = None):
    if run_id is None:
        return _latest_completed_run(db_path)
    run = db.get_run(db_path, int(run_id))
    if run is None:
        raise RuntimeError("Snapshot run {} was not found.".format(run_id))
    return run


def get_snapshot_metadata(db_path: str, run_id: Optional[int] = None) -> Dict[str, object]:
    run = _get_target_run(db_path, run_id)
    return {
        "run_id": int(run["id"]),
        "status": str(run["status"]),
        "created_at": str(run["created_at"] or ""),
        "started_at": str(run["started_at"] or ""),
        "finished_at": str(run["finished_at"] or ""),
        "catalog_source": str(run["catalog_source"] or ""),
        "excluded_workspaces": sorted(report.parse_excluded_workspaces(str(run["excluded_workspaces"] or ""))),
        "geoserver_url": str(run["geoserver_url"] or ""),
        "data_dir": str(run["data_dir"] or ""),
        "store_count": int(run["store_count"] or 0),
        "orphan_count": int(run["orphan_count"] or 0),
        "issue_count": int(run["issue_count"] or 0),
        "tracked_size_bytes": int(run["tracked_size_bytes"] or 0),
        "tracked_size_gb": "{:.2f}".format(int(run["tracked_size_bytes"] or 0) / (1024.0 ** 3)),
        "notes": str(run["notes"] or ""),
    }


def get_run_rows_dicts(db_path: str, run_id: Optional[int] = None) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    metadata = get_snapshot_metadata(db_path, run_id)
    rows = [_row_to_dict(row) for row in db.get_run_rows(db_path, int(metadata["run_id"]))]
    return metadata, rows


def _matches_text(row: Dict[str, object], query: str) -> bool:
    if not query:
        return True
    needle = query.strip().lower()
    haystacks = [
        str(row.get("workspace") or ""),
        str(row.get("store_name") or ""),
        str(row.get("store_type") or ""),
        str(row.get("layer_names") or ""),
        str(row.get("resolved_path") or ""),
        str(row.get("notes") or ""),
    ]
    return any(needle in value.lower() for value in haystacks)


def _filter_rows(
    rows: Sequence[Dict[str, object]],
    *,
    q: str = "",
    workspace: str = "",
    status: str = "",
    row_kind: str = "",
    store_type: str = "",
) -> List[Dict[str, object]]:
    filtered: List[Dict[str, object]] = []
    for row in rows:
        if workspace and str(row.get("workspace") or "") != workspace:
            continue
        if status and str(row.get("status") or "") != status:
            continue
        if row_kind and str(row.get("row_kind") or "") != row_kind:
            continue
        if store_type and str(row.get("store_type") or "") != store_type:
            continue
        if not _matches_text(row, q):
            continue
        filtered.append(row)
    return filtered


def list_heaviest_stores(
    db_path: str,
    *,
    run_id: Optional[int] = None,
    limit: int = DEFAULT_HEAVIEST_LIMIT,
    workspace: str = "",
    store_type: str = "",
    status: str = "",
    include_orphans: bool = False,
) -> Dict[str, object]:
    metadata, rows = get_run_rows_dicts(db_path, run_id)
    filtered = _filter_rows(
        rows,
        workspace=workspace,
        status=status,
        row_kind="" if include_orphans else "store",
        store_type=store_type,
    )
    if include_orphans:
        filtered = [row for row in filtered if row.get("row_kind") in {"store", "orphaned"}]
    else:
        filtered = [row for row in filtered if row.get("row_kind") == "store"]
    filtered.sort(key=lambda item: (int(item.get("size_bytes") or 0), int(item.get("id") or 0)), reverse=True)
    capped = filtered[: max(int(limit or DEFAULT_HEAVIEST_LIMIT), 1)]
    return {
        "snapshot": metadata,
        "row_count": len(capped),
        "rows": capped,
        "truncated": len(filtered) > len(capped),
    }


def summarize_workspace_usage(
    db_path: str,
    *,
    run_id: Optional[int] = None,
    workspace: str = "",
    include_issues: bool = True,
) -> Dict[str, object]:
    metadata, rows = get_run_rows_dicts(db_path, run_id)
    filtered = [row for row in rows if row.get("row_kind") == "store"]
    if workspace:
        filtered = [row for row in filtered if str(row.get("workspace") or "") == workspace]
    aggregates: Dict[str, Dict[str, object]] = defaultdict(
        lambda: {
            "workspace": "",
            "store_count": 0,
            "total_size_bytes": 0,
            "total_size_gb": "0.00",
            "total_file_count": 0,
            "issue_count": 0,
        }
    )
    for row in filtered:
        workspace_name = str(row.get("workspace") or "")
        bucket = aggregates[workspace_name]
        bucket["workspace"] = workspace_name
        bucket["store_count"] += 1
        bucket["total_size_bytes"] += int(row.get("size_bytes") or 0)
        bucket["total_file_count"] += int(row.get("file_count") or 0)
        if str(row.get("status") or "") != "ok":
            bucket["issue_count"] += 1
    results = []
    for workspace_name in sorted(aggregates):
        item = aggregates[workspace_name]
        item["total_size_gb"] = "{:.2f}".format(int(item["total_size_bytes"]) / (1024.0 ** 3))
        if not include_issues:
            item.pop("issue_count", None)
        results.append(item)
    return {
        "snapshot": metadata,
        "row_count": len(results),
        "workspaces": results,
    }


def list_orphans(
    db_path: str,
    *,
    run_id: Optional[int] = None,
    limit: int = DEFAULT_ORPHAN_LIMIT,
    path_filter: str = "",
    sort_order: str = "size_desc",
) -> Dict[str, object]:
    metadata, rows = get_run_rows_dicts(db_path, run_id)
    filtered = [row for row in rows if row.get("row_kind") == "orphaned"]
    if path_filter:
        needle = path_filter.strip().lower()
        filtered = [row for row in filtered if needle in str(row.get("resolved_path") or "").lower()]
    sort_key = sort_order.strip().lower() or "size_desc"
    if sort_key == "path_asc":
        filtered.sort(key=lambda item: str(item.get("resolved_path") or "").lower())
    elif sort_key == "path_desc":
        filtered.sort(key=lambda item: str(item.get("resolved_path") or "").lower(), reverse=True)
    elif sort_key == "size_asc":
        filtered.sort(key=lambda item: (int(item.get("size_bytes") or 0), int(item.get("id") or 0)))
    else:
        filtered.sort(key=lambda item: (int(item.get("size_bytes") or 0), int(item.get("id") or 0)), reverse=True)
    capped = filtered[: max(int(limit or DEFAULT_ORPHAN_LIMIT), 1)]
    return {
        "snapshot": metadata,
        "row_count": len(capped),
        "rows": capped,
        "truncated": len(filtered) > len(capped),
    }


def find_stores(
    db_path: str,
    *,
    run_id: Optional[int] = None,
    q: str = "",
    workspace: str = "",
    status: str = "",
    store_type: str = "",
    row_kind: str = "store",
    sort_by: str = "size_bytes",
    sort_dir: str = "desc",
    limit: int = DEFAULT_FIND_LIMIT,
) -> Dict[str, object]:
    metadata, rows = get_run_rows_dicts(db_path, run_id)
    filtered = _filter_rows(
        rows,
        q=q,
        workspace=workspace,
        status=status,
        row_kind=row_kind,
        store_type=store_type,
    )
    reverse = str(sort_dir or "desc").lower() != "asc"
    if sort_by == "workspace":
        filtered.sort(key=lambda item: (str(item.get("workspace") or "").lower(), int(item.get("id") or 0)), reverse=reverse)
    elif sort_by == "store_name":
        filtered.sort(key=lambda item: (str(item.get("store_name") or "").lower(), int(item.get("id") or 0)), reverse=reverse)
    elif sort_by == "file_count":
        filtered.sort(key=lambda item: (int(item.get("file_count") or 0), int(item.get("id") or 0)), reverse=reverse)
    elif sort_by == "status":
        filtered.sort(key=lambda item: (str(item.get("status") or "").lower(), int(item.get("id") or 0)), reverse=reverse)
    else:
        filtered.sort(key=lambda item: (int(item.get("size_bytes") or 0), int(item.get("id") or 0)), reverse=reverse)
    capped = filtered[: max(int(limit or DEFAULT_FIND_LIMIT), 1)]
    return {
        "snapshot": metadata,
        "row_count": len(capped),
        "rows": capped,
        "truncated": len(filtered) > len(capped),
    }


def resolve_store_selection(
    db_path: str,
    settings: Settings,
    *,
    run_id: Optional[int] = None,
    store_ids: Optional[Sequence[int]] = None,
    store_keys: Optional[Sequence[str]] = None,
) -> Dict[str, object]:
    metadata, rows = get_run_rows_dicts(db_path, run_id)
    rows_by_id = {int(row["id"]): row for row in rows}
    rows_by_key = {
        "{}/{}".format(str(row.get("workspace") or ""), str(row.get("store_name") or "")): row
        for row in rows
        if row.get("row_kind") == "store" and row.get("workspace") and row.get("store_name")
    }
    selected: List[Dict[str, object]] = []
    blocked: List[Dict[str, object]] = []
    seen_ids = set()

    for store_id in store_ids or []:
        row = rows_by_id.get(int(store_id))
        if row is None:
            blocked.append({"store_id": int(store_id), "reason": "Store row {} was not found in the snapshot.".format(store_id)})
            continue
        if int(row["id"]) in seen_ids:
            continue
        selected.append(row)
        seen_ids.add(int(row["id"]))

    for key in store_keys or []:
        normalized_key = str(key or "").strip()
        row = rows_by_key.get(normalized_key)
        if row is None:
            blocked.append({"store_key": normalized_key, "reason": "Store key {} was not found in the snapshot.".format(normalized_key)})
            continue
        if int(row["id"]) in seen_ids:
            continue
        selected.append(row)
        seen_ids.add(int(row["id"]))

    preview = deletion.build_delete_preview(
        db_path,
        settings,
        int(metadata["run_id"]),
        [int(row["id"]) for row in selected],
    )

    accepted: List[Dict[str, object]] = []
    allowed_ids = set(preview["selected_ids"])
    preview_by_id = {item.store_id: item for item in preview["items"]}
    for row in selected:
        row_id = int(row["id"])
        item = preview_by_id.get(row_id)
        if row_id in allowed_ids and item is not None:
            accepted.append(
                {
                    "store_id": row_id,
                    "workspace": str(row.get("workspace") or ""),
                    "store_name": str(row.get("store_name") or ""),
                    "store_kind": str(row.get("store_kind") or ""),
                    "store_type": str(row.get("store_type") or ""),
                    "can_delete_data": bool(item.can_delete_data),
                    "data_scope": item.data_scope,
                    "reason": item.reason,
                }
            )
        elif item is not None:
            blocked.append(
                {
                    "store_id": row_id,
                    "workspace": str(row.get("workspace") or ""),
                    "store_name": str(row.get("store_name") or ""),
                    "reason": item.reason,
                }
            )
        else:
            blocked.append(
                {
                    "store_id": row_id,
                    "workspace": str(row.get("workspace") or ""),
                    "store_name": str(row.get("store_name") or ""),
                    "reason": "The selected row cannot be deleted.",
                }
            )

    return {
        "snapshot": metadata,
        "accepted": accepted,
        "blocked": blocked,
        "selected_ids": [item["store_id"] for item in accepted],
        "preview": preview,
    }


def build_snapshot_csv_bytes(db_path: str, *, run_id: Optional[int] = None) -> Tuple[Dict[str, object], bytes]:
    metadata, rows = get_run_rows_dicts(db_path, run_id)
    return metadata, report.build_csv_bytes(rows)


def build_snapshot_html_text(
    db_path: str,
    settings: Settings,
    *,
    run_id: Optional[int] = None,
) -> Tuple[Dict[str, object], str]:
    metadata, rows = get_run_rows_dicts(db_path, run_id)
    return (
        metadata,
        report.build_html_report_text(
            rows,
            list(metadata["excluded_workspaces"]),
            str(metadata["geoserver_url"] or settings.geoserver_url),
            str(metadata["data_dir"] or settings.data_dir),
        ),
    )


def write_snapshot_export(
    db_path: str,
    settings: Settings,
    *,
    format_name: str,
    run_id: Optional[int] = None,
) -> Dict[str, object]:
    os.makedirs(settings.export_dir, exist_ok=True)
    if format_name == "csv":
        metadata, content = build_snapshot_csv_bytes(db_path, run_id=run_id)
        path = os.path.join(settings.export_dir, "geoserver_store_report_snapshot_{}.{}".format(int(metadata["run_id"]), "csv"))
        with open(path, "wb") as handle:
            handle.write(content)
    elif format_name == "html":
        metadata, content = build_snapshot_html_text(db_path, settings, run_id=run_id)
        path = os.path.join(settings.export_dir, "geoserver_store_report_snapshot_{}.{}".format(int(metadata["run_id"]), "html"))
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
    else:
        raise RuntimeError("Unsupported export format: {}".format(format_name))
    filename = os.path.basename(path)
    return {
        "snapshot": metadata,
        "path": os.path.abspath(path),
        "filename": filename,
        "format": format_name,
        "size_bytes": os.path.getsize(path),
    }
