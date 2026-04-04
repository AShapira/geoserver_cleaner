from __future__ import annotations

import json
from typing import Dict


def format_duration(seconds) -> str:
    if seconds is None:
        return ""
    total = max(int(seconds), 0)
    if total < 60:
        return "{}s".format(total)
    minutes, remainder = divmod(total, 60)
    if minutes < 60:
        return "{}m {}s".format(minutes, remainder)
    hours, minutes = divmod(minutes, 60)
    return "{}h {}m".format(hours, minutes)


def build_progress_summary(job_type: str, metadata: Dict[str, object], status: str) -> str:
    if job_type == "scan":
        phase = str(metadata.get("phase") or "")
        discovered = metadata.get("discovered_store_count")
        processed = metadata.get("processed_stores")
        total = metadata.get("total_stores")
        if phase == "discovering" and discovered is not None:
            return "Discovered {} stores so far".format(discovered)
        if phase == "stores" and processed is not None and total is not None:
            remaining = max(int(total) - int(processed), 0)
            return "Scanned {} stores, remaining {}".format(processed, remaining)
        if phase == "orphans":
            return "Finished scanning stores. Calculating orphaned data"
        if status == "completed" and total is not None:
            return "Scanned {} stores, remaining 0".format(total)
    if job_type == "delete":
        phase = str(metadata.get("phase") or "")
        deleted_count = int(metadata.get("deleted_count") or 0)
        remaining_delete_items = metadata.get("remaining_delete_items")
        processed = metadata.get("processed_stores")
        total = metadata.get("total_stores")
        if phase == "delete" and remaining_delete_items is not None:
            return "Deleted {} stores, remaining {}".format(deleted_count, remaining_delete_items)
        if phase == "refresh_stores" and processed is not None and total is not None:
            remaining = max(int(total) - int(processed), 0)
            return "Deleted {} stores. Scanned {} stores, remaining {}".format(
                deleted_count,
                processed,
                remaining,
            )
        if phase == "refresh_orphans":
            return "Deleted {} stores. Calculating orphaned data".format(deleted_count)
        if status == "completed":
            return "Deleted {} stores, remaining 0".format(deleted_count)
    return ""


def serialize_job_row(job) -> Dict[str, object]:
    payload = {key: job[key] for key in job.keys()}
    try:
        metadata = json.loads(payload.get("metadata_json") or "{}")
    except json.JSONDecodeError:
        metadata = {}
    payload["metadata"] = metadata
    payload["eta_display"] = format_duration(metadata.get("eta_seconds"))
    payload["progress_summary"] = build_progress_summary(
        str(payload.get("job_type") or ""),
        metadata,
        str(payload.get("status") or ""),
    )
    return payload
