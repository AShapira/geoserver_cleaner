from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Dict
from urllib.parse import urlencode

import geoserver_store_report as report
from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app import db
from app.config import Settings
from app.jobs import JobManager
from app.services import deletion


LOGGER = logging.getLogger("geoserver_cleaner")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))
SETTINGS = Settings.from_env()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

app = FastAPI(title=SETTINGS.app_title)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
db.init_db(SETTINGS.database_path)
app.state.settings = SETTINGS
app.state.job_manager = JobManager(SETTINGS, SETTINGS.database_path)
app.state.asset_version = str(int(time.time()))


def query_string(params: Dict[str, object]) -> str:
    return urlencode({key: value for key, value in params.items() if value not in ("", None)})


def latest_run_or_404() -> object:
    latest_run = db.get_latest_completed_run(SETTINGS.database_path)
    if latest_run is None:
        raise HTTPException(status_code=404, detail="No completed inventory snapshot is available.")
    return latest_run


def build_report_filename(run_id: int, suffix: str) -> str:
    return "geoserver_store_report_snapshot_{}.{}".format(run_id, suffix)


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


def serialize_job(job) -> Dict[str, object]:
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


def build_table_state(request: Request, run_id: int) -> Dict[str, object]:
    settings: Settings = request.app.state.settings
    raw_page = max(int(request.query_params.get("page", "1") or 1), 1)
    raw_page_size = max(
        int(request.query_params.get("page_size", settings.page_size_default) or settings.page_size_default),
        10,
    )
    page_size = min(raw_page_size, settings.page_size_max)
    q = request.query_params.get("q", "").strip()
    workspace = request.query_params.get("workspace", "").strip()
    status = request.query_params.get("status", "").strip()
    row_kind = request.query_params.get("row_kind", "").strip()
    store_type = request.query_params.get("store_type", "").strip()
    sort_by = request.query_params.get("sort_by", "size_bytes").strip() or "size_bytes"
    sort_dir = "asc" if request.query_params.get("sort_dir", "desc").strip().lower() == "asc" else "desc"

    rows, total = db.query_stores(
        settings.database_path,
        run_id,
        page=raw_page,
        page_size=page_size,
        q=q,
        workspace=workspace,
        status=status,
        row_kind=row_kind,
        store_type=store_type,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    total_pages = max(1, math.ceil(total / page_size)) if total else 1
    page = min(raw_page, total_pages)
    if page != raw_page:
        rows, total = db.query_stores(
            settings.database_path,
            run_id,
            page=page,
            page_size=page_size,
            q=q,
            workspace=workspace,
            status=status,
            row_kind=row_kind,
            store_type=store_type,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )

    params = {
        "q": q,
        "workspace": workspace,
        "status": status,
        "row_kind": row_kind,
        "store_type": store_type,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "page_size": page_size,
    }

    def page_link(next_page: int) -> str:
        return "/stores/table?{}".format(query_string({**params, "page": next_page}))

    sort_links = {}
    for key in db.SORTABLE_COLUMNS:
        next_dir = "desc"
        if sort_by == key and sort_dir == "desc":
            next_dir = "asc"
        sort_links[key] = "/stores/table?{}".format(
            query_string({**params, "page": 1, "sort_by": key, "sort_dir": next_dir})
        )

    return {
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "query": q,
        "workspace": workspace,
        "status": status,
        "row_kind": row_kind,
        "store_type": store_type,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "sort_links": sort_links,
        "page_links": {
            "first": page_link(1),
            "prev": page_link(max(page - 1, 1)),
            "next": page_link(min(page + 1, total_pages)),
            "last": page_link(total_pages),
        },
        "filters": {
            "workspaces": db.distinct_store_values(settings.database_path, run_id, "workspace"),
            "statuses": db.distinct_store_values(settings.database_path, run_id, "status"),
            "row_kinds": db.distinct_store_values(settings.database_path, run_id, "row_kind"),
            "store_types": db.distinct_store_values(settings.database_path, run_id, "store_type"),
        },
    }


@app.get("/", response_class=HTMLResponse)
def home() -> RedirectResponse:
    return RedirectResponse(url="/stores", status_code=303)


@app.get("/stores", response_class=HTMLResponse)
def stores_page(request: Request):
    latest_run = db.get_latest_completed_run(SETTINGS.database_path)
    summary = db.latest_summary(SETTINGS.database_path)
    current_excluded_workspaces = ""
    if summary and summary.get("excluded_workspaces"):
        current_excluded_workspaces = str(summary["excluded_workspaces"])
    else:
        current_excluded_workspaces = SETTINGS.excluded_workspaces_raw
    context = {
        "request": request,
        "app_title": SETTINGS.app_title,
        "summary": summary,
        "latest_run": latest_run,
        "running_jobs": [serialize_job(item) for item in db.list_running_jobs(SETTINGS.database_path)],
        "current_excluded_workspaces": current_excluded_workspaces,
    }
    if latest_run is not None:
        context["table_state"] = build_table_state(request, int(latest_run["id"]))
    return TEMPLATES.TemplateResponse(request, "stores.html", context)


@app.get("/stores/table", response_class=HTMLResponse)
def stores_table(request: Request):
    latest_run = db.get_latest_completed_run(SETTINGS.database_path)
    if latest_run is None:
        return HTMLResponse("<div class='empty-panel'>No completed inventory snapshot is available yet.</div>")
    return TEMPLATES.TemplateResponse(
        request,
        "_stores_table.html",
        {
            "table_state": build_table_state(request, int(latest_run["id"])),
        },
    )


@app.post("/scan")
def start_scan(exclude_workspaces: str = Form("")) -> RedirectResponse:
    try:
        job_id = app.state.job_manager.start_scan(exclude_workspaces.strip())
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return RedirectResponse(url="/jobs/{}".format(job_id), status_code=303)


@app.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: int):
    job = db.get_job(SETTINGS.database_path, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return TEMPLATES.TemplateResponse(
        request,
        "job_detail.html",
        {"job": serialize_job(job)},
    )


@app.get("/jobs/{job_id}/header", response_class=HTMLResponse)
def job_header_fragment(request: Request, job_id: int):
    job = db.get_job(SETTINGS.database_path, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return TEMPLATES.TemplateResponse(
        request,
        "_job_header.html",
        {"job": serialize_job(job)},
    )


@app.get("/jobs/{job_id}/status", response_class=HTMLResponse)
def job_status_fragment(request: Request, job_id: int):
    job = db.get_job(SETTINGS.database_path, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return TEMPLATES.TemplateResponse(
        request,
        "_job_status.html",
        {"job": serialize_job(job)},
    )


@app.post("/delete/preview", response_class=HTMLResponse)
def delete_preview(request: Request, selected_ids: str = Form("")):
    latest_run = latest_run_or_404()
    store_ids = deletion.parse_selected_ids(selected_ids)
    preview = deletion.build_delete_preview(
        SETTINGS.database_path,
        SETTINGS,
        int(latest_run["id"]),
        store_ids,
    )
    return TEMPLATES.TemplateResponse(
        request,
        "delete_preview.html",
        {
            "preview": preview,
            "selected_ids": ",".join(str(item) for item in preview["selected_ids"]),
            "run_id": int(latest_run["id"]),
        },
    )


@app.post("/delete/execute")
def delete_execute(
    selected_ids: str = Form(""),
    run_id: int = Form(...),
) -> RedirectResponse:
    store_ids = deletion.parse_selected_ids(selected_ids)
    if not store_ids:
        raise HTTPException(status_code=400, detail="No stores were selected.")
    preview = deletion.build_delete_preview(
        SETTINGS.database_path,
        SETTINGS,
        run_id,
        store_ids,
    )
    valid_store_ids = preview["selected_ids"]
    if not valid_store_ids:
        raise HTTPException(status_code=400, detail="No deletable store rows were selected.")
    run = db.get_run(SETTINGS.database_path, run_id)
    excluded_workspaces_raw = str(run["excluded_workspaces"]) if run is not None else SETTINGS.excluded_workspaces_raw
    try:
        job_id = app.state.job_manager.start_delete(run_id, valid_store_ids, excluded_workspaces_raw)
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return RedirectResponse(url="/jobs/{}".format(job_id), status_code=303)


@app.get("/reports/latest.csv")
def download_latest_csv() -> Response:
    latest_run = latest_run_or_404()
    rows = [dict(row) for row in db.get_run_rows(SETTINGS.database_path, int(latest_run["id"]))]
    filename = build_report_filename(int(latest_run["id"]), "csv")
    return Response(
        content=report.build_csv_bytes(rows),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="{}"'.format(filename)},
    )


@app.get("/reports/latest.html")
def download_latest_html() -> Response:
    latest_run = latest_run_or_404()
    rows = [dict(row) for row in db.get_run_rows(SETTINGS.database_path, int(latest_run["id"]))]
    excluded_workspaces = sorted(report.parse_excluded_workspaces(str(latest_run["excluded_workspaces"] or "")))
    filename = build_report_filename(int(latest_run["id"]), "html")
    return Response(
        content=report.build_html_report_text(
            rows,
            excluded_workspaces,
            str(latest_run["geoserver_url"] or SETTINGS.geoserver_url),
            str(latest_run["data_dir"] or SETTINGS.data_dir),
        ),
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="{}"'.format(filename)},
    )
