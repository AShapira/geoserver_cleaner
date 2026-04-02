from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple


SORTABLE_COLUMNS = {
    "workspace": "workspace",
    "store_name": "store_name",
    "store_type": "store_type",
    "size_bytes": "size_bytes",
    "file_count": "file_count",
    "status": "status",
    "row_kind": "row_kind",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _connect(db_path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


@contextmanager
def managed_connection(db_path: str):
    connection = _connect(db_path)
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db(db_path: str) -> None:
    parent = os.path.dirname(os.path.abspath(db_path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)
    with managed_connection(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS inventory_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                catalog_source TEXT NOT NULL,
                excluded_workspaces TEXT NOT NULL,
                geoserver_url TEXT NOT NULL,
                data_dir TEXT NOT NULL,
                store_count INTEGER NOT NULL DEFAULT 0,
                orphan_count INTEGER NOT NULL DEFAULT 0,
                issue_count INTEGER NOT NULL DEFAULT 0,
                tracked_size_bytes INTEGER NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS stores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER NOT NULL REFERENCES inventory_runs(id) ON DELETE CASCADE,
                store_kind TEXT NOT NULL DEFAULT '',
                row_kind TEXT NOT NULL,
                workspace TEXT NOT NULL DEFAULT '',
                store_name TEXT NOT NULL DEFAULT '',
                store_type TEXT NOT NULL DEFAULT '',
                layer_names TEXT NOT NULL DEFAULT '',
                configured_path TEXT NOT NULL DEFAULT '',
                resolved_path TEXT NOT NULL DEFAULT '',
                normalized_path TEXT NOT NULL DEFAULT '',
                path_kind TEXT NOT NULL DEFAULT '',
                size_bytes INTEGER NOT NULL DEFAULT 0,
                size_gb TEXT NOT NULL DEFAULT '0.00',
                file_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT ''
            );

            CREATE INDEX IF NOT EXISTS idx_stores_run_id ON stores(run_id);
            CREATE INDEX IF NOT EXISTS idx_stores_run_kind ON stores(run_id, row_kind);
            CREATE INDEX IF NOT EXISTS idx_stores_run_workspace ON stores(run_id, workspace);
            CREATE INDEX IF NOT EXISTS idx_stores_run_status ON stores(run_id, status);
            CREATE INDEX IF NOT EXISTS idx_stores_run_size ON stores(run_id, size_bytes);
            CREATE INDEX IF NOT EXISTS idx_stores_run_path ON stores(run_id, normalized_path);

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                message TEXT NOT NULL DEFAULT '',
                error_text TEXT NOT NULL DEFAULT '',
                run_id INTEGER,
                metadata_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS audit_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            """
        )


def create_job(db_path: str, job_type: str, message: str, metadata: Optional[dict] = None) -> int:
    with managed_connection(db_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO jobs(job_type, status, created_at, message, metadata_json)
            VALUES (?, 'queued', ?, ?, ?)
            """,
            (job_type, utc_now(), message, json.dumps(metadata or {}, ensure_ascii=False)),
        )
        return int(cursor.lastrowid)


def update_job(
    db_path: str,
    job_id: int,
    *,
    status: Optional[str] = None,
    message: Optional[str] = None,
    error_text: Optional[str] = None,
    run_id: Optional[int] = None,
    metadata: Optional[dict] = None,
    started: bool = False,
    finished: bool = False,
) -> None:
    fields = []
    values: List[object] = []
    if status is not None:
        fields.append("status = ?")
        values.append(status)
    if message is not None:
        fields.append("message = ?")
        values.append(message)
    if error_text is not None:
        fields.append("error_text = ?")
        values.append(error_text)
    if run_id is not None:
        fields.append("run_id = ?")
        values.append(run_id)
    if metadata is not None:
        fields.append("metadata_json = ?")
        values.append(json.dumps(metadata, ensure_ascii=False))
    if started:
        fields.append("started_at = ?")
        values.append(utc_now())
    if finished:
        fields.append("finished_at = ?")
        values.append(utc_now())
    if not fields:
        return
    values.append(job_id)
    with managed_connection(db_path) as connection:
        connection.execute(
            "UPDATE jobs SET {} WHERE id = ?".format(", ".join(fields)),
            values,
        )


def get_job(db_path: str, job_id: int) -> Optional[sqlite3.Row]:
    with managed_connection(db_path) as connection:
        return connection.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def list_running_jobs(db_path: str) -> List[sqlite3.Row]:
    with managed_connection(db_path) as connection:
        return connection.execute(
            "SELECT * FROM jobs WHERE status IN ('queued', 'running') ORDER BY id ASC"
        ).fetchall()


def create_inventory_run(
    db_path: str,
    *,
    catalog_source: str,
    excluded_workspaces: Sequence[str],
    geoserver_url: str,
    data_dir: str,
) -> int:
    now = utc_now()
    with managed_connection(db_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO inventory_runs(
                created_at, started_at, status, catalog_source, excluded_workspaces, geoserver_url, data_dir
            )
            VALUES (?, ?, 'running', ?, ?, ?, ?)
            """,
            (now, now, catalog_source, ", ".join(excluded_workspaces), geoserver_url, data_dir),
        )
        return int(cursor.lastrowid)


def replace_run_rows(db_path: str, run_id: int, rows: Sequence[dict]) -> None:
    records = []
    for row in rows:
        records.append(
            (
                run_id,
                row.get("store_kind", ""),
                row.get("row_kind", ""),
                row.get("workspace", ""),
                row.get("store_name", ""),
                row.get("store_type", ""),
                row.get("layer_names", ""),
                row.get("configured_path", ""),
                row.get("resolved_path", ""),
                row.get("normalized_path", ""),
                row.get("path_kind", ""),
                int(row.get("size_bytes", 0) or 0),
                row.get("size_gb", "0.00"),
                int(row.get("file_count", 0) or 0),
                row.get("status", ""),
                row.get("notes", ""),
            )
        )
    with managed_connection(db_path) as connection:
        connection.execute("DELETE FROM stores WHERE run_id = ?", (run_id,))
        connection.executemany(
            """
            INSERT INTO stores(
                run_id, store_kind, row_kind, workspace, store_name, store_type, layer_names,
                configured_path, resolved_path, normalized_path, path_kind, size_bytes, size_gb,
                file_count, status, notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            records,
        )


def finalize_inventory_run(
    db_path: str,
    run_id: int,
    *,
    status: str,
    store_count: int,
    orphan_count: int,
    issue_count: int,
    tracked_size_bytes: int,
    notes: str = "",
) -> None:
    with managed_connection(db_path) as connection:
        connection.execute(
            """
            UPDATE inventory_runs
            SET status = ?, finished_at = ?, store_count = ?, orphan_count = ?,
                issue_count = ?, tracked_size_bytes = ?, notes = ?
            WHERE id = ?
            """,
            (status, utc_now(), store_count, orphan_count, issue_count, tracked_size_bytes, notes, run_id),
        )


def fail_inventory_run(db_path: str, run_id: int, notes: str) -> None:
    finalize_inventory_run(
        db_path,
        run_id,
        status="failed",
        store_count=0,
        orphan_count=0,
        issue_count=0,
        tracked_size_bytes=0,
        notes=notes,
    )


def get_latest_completed_run(db_path: str) -> Optional[sqlite3.Row]:
    with managed_connection(db_path) as connection:
        return connection.execute(
            "SELECT * FROM inventory_runs WHERE status = 'completed' ORDER BY id DESC LIMIT 1"
        ).fetchone()


def get_run(db_path: str, run_id: int) -> Optional[sqlite3.Row]:
    with managed_connection(db_path) as connection:
        return connection.execute("SELECT * FROM inventory_runs WHERE id = ?", (run_id,)).fetchone()


def query_stores(
    db_path: str,
    run_id: int,
    *,
    page: int,
    page_size: int,
    q: str,
    workspace: str,
    status: str,
    row_kind: str,
    store_type: str,
    sort_by: str,
    sort_dir: str,
) -> Tuple[List[sqlite3.Row], int]:
    where = ["run_id = ?"]
    values: List[object] = [run_id]
    if q:
        where.append(
            "("
            "workspace LIKE ? OR store_name LIKE ? OR store_type LIKE ? OR "
            "layer_names LIKE ? OR resolved_path LIKE ? OR notes LIKE ?"
            ")"
        )
        needle = "%{}%".format(q)
        values.extend([needle] * 6)
    if workspace:
        where.append("workspace = ?")
        values.append(workspace)
    if status:
        where.append("status = ?")
        values.append(status)
    if row_kind:
        where.append("row_kind = ?")
        values.append(row_kind)
    if store_type:
        where.append("store_type = ?")
        values.append(store_type)

    sort_column = SORTABLE_COLUMNS.get(sort_by, "size_bytes")
    sort_direction = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
    where_sql = " AND ".join(where)
    with managed_connection(db_path) as connection:
        total = int(
            connection.execute("SELECT COUNT(*) FROM stores WHERE {}".format(where_sql), values).fetchone()[0]
        )
        rows = connection.execute(
            """
            SELECT * FROM stores
            WHERE {where_sql}
            ORDER BY {sort_column} {sort_direction}, id ASC
            LIMIT ? OFFSET ?
            """.format(where_sql=where_sql, sort_column=sort_column, sort_direction=sort_direction),
            [*values, page_size, max(page - 1, 0) * page_size],
        ).fetchall()
    return rows, total


def distinct_store_values(db_path: str, run_id: int, column: str) -> List[str]:
    if column not in {"workspace", "status", "row_kind", "store_type"}:
        raise ValueError("Unsupported distinct column: {}".format(column))
    with managed_connection(db_path) as connection:
        rows = connection.execute(
            "SELECT DISTINCT {column} FROM stores WHERE run_id = ? AND {column} <> '' ORDER BY {column} ASC".format(
                column=column
            ),
            (run_id,),
        ).fetchall()
    return [str(row[0]) for row in rows]


def get_rows_by_ids(db_path: str, run_id: int, store_ids: Sequence[int]) -> List[sqlite3.Row]:
    if not store_ids:
        return []
    placeholders = ", ".join("?" for _ in store_ids)
    with managed_connection(db_path) as connection:
        return connection.execute(
            "SELECT * FROM stores WHERE run_id = ? AND id IN ({}) ORDER BY workspace, store_name, id".format(
                placeholders
            ),
            [run_id, *store_ids],
        ).fetchall()


def get_run_rows(db_path: str, run_id: int) -> List[sqlite3.Row]:
    with managed_connection(db_path) as connection:
        return connection.execute(
            "SELECT * FROM stores WHERE run_id = ? ORDER BY id ASC",
            (run_id,),
        ).fetchall()


def get_path_owners(db_path: str, run_id: int, normalized_paths: Sequence[str]) -> Dict[str, List[sqlite3.Row]]:
    normalized_paths = [item for item in normalized_paths if item]
    if not normalized_paths:
        return {}
    placeholders = ", ".join("?" for _ in normalized_paths)
    with managed_connection(db_path) as connection:
        rows = connection.execute(
            """
            SELECT * FROM stores
            WHERE run_id = ? AND row_kind = 'store' AND normalized_path IN ({})
            ORDER BY normalized_path, id
            """.format(placeholders),
            [run_id, *normalized_paths],
        ).fetchall()
    result: Dict[str, List[sqlite3.Row]] = {}
    for row in rows:
        result.setdefault(str(row["normalized_path"]), []).append(row)
    return result


def add_audit_event(db_path: str, event_type: str, payload: dict) -> None:
    with managed_connection(db_path) as connection:
        connection.execute(
            "INSERT INTO audit_events(created_at, event_type, payload_json) VALUES (?, ?, ?)",
            (utc_now(), event_type, json.dumps(payload, ensure_ascii=False)),
        )


def latest_summary(db_path: str) -> Optional[Dict[str, object]]:
    run = get_latest_completed_run(db_path)
    if run is None:
        return None
    return {
        "id": int(run["id"]),
        "store_count": int(run["store_count"]),
        "orphan_count": int(run["orphan_count"]),
        "issue_count": int(run["issue_count"]),
        "tracked_size_gb": "{:.2f}".format(int(run["tracked_size_bytes"]) / (1024.0 ** 3)),
        "catalog_source": run["catalog_source"],
        "excluded_workspaces": run["excluded_workspaces"],
        "finished_at": run["finished_at"] or "",
        "data_dir": run["data_dir"],
        "geoserver_url": run["geoserver_url"],
    }
