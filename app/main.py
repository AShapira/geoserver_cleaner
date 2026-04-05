from __future__ import annotations

import contextlib
import logging
import math
import os
import time
from typing import Dict
from urllib.parse import urlencode

from fastapi import APIRouter, FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app import db
from app.config import Settings
from app.mcp.server import build_streamable_http_server
from app.runtime import AppRuntime, build_runtime
from app.services import deletion, job_status, snapshots


LOGGER = logging.getLogger("geoserver_cleaner")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)

router = APIRouter()


def create_app(settings: Settings | None = None) -> FastAPI:
    runtime = build_runtime(settings)
    app = FastAPI(title=runtime.settings.app_title)

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI):
        if runtime.settings.enable_mcp_http:
            mcp_server = build_streamable_http_server(runtime)
            app.state.mcp_server = mcp_server
            LOGGER.info("MCP HTTP enabled at %s", runtime.settings.mcp_http_path)
            async with mcp_server.session_manager.run():
                yield
            app.state.mcp_server = None
            return
        LOGGER.info("MCP HTTP disabled")
        yield

    app.router.lifespan_context = lifespan
    app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
    app.state.runtime = runtime
    app.state.settings = runtime.settings
    app.state.job_manager = runtime.job_manager
    app.state.asset_version = str(int(time.time()))
    app.include_router(router)

    if runtime.settings.enable_mcp_http:
        async def mcp_http_asgi(scope, receive, send) -> None:
            root_path = scope.get("root_path", "")
            path = scope.get("path", "")
            if path not in {root_path, root_path + "/"}:
                await send({"type": "http.response.start", "status": 404, "headers": []})
                await send({"type": "http.response.body", "body": b""})
                return
            mcp_server = app.state.mcp_server
            await mcp_server.session_manager.handle_request(scope, receive, send)

        app.mount(runtime.settings.mcp_http_path, mcp_http_asgi)
    return app


def request_runtime(request: Request) -> AppRuntime:
    return request.app.state.runtime


def query_string(params: Dict[str, object]) -> str:
    return urlencode({key: value for key, value in params.items() if value not in ("", None)})


def latest_run_or_404(settings: Settings) -> object:
    latest_run = db.get_latest_completed_run(settings.database_path)
    if latest_run is None:
        raise HTTPException(status_code=404, detail="No completed inventory snapshot is available.")
    return latest_run


def build_report_filename(run_id: int, suffix: str) -> str:
    return "geoserver_store_report_snapshot_{}.{}".format(run_id, suffix)


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


@router.get("/", response_class=HTMLResponse)
def home() -> RedirectResponse:
    return RedirectResponse(url="/stores", status_code=303)


@router.get("/stores", response_class=HTMLResponse)
def stores_page(request: Request):
    runtime = request_runtime(request)
    settings = runtime.settings
    latest_run = db.get_latest_completed_run(settings.database_path)
    summary = db.latest_summary(settings.database_path)
    current_excluded_workspaces = ""
    if summary and summary.get("excluded_workspaces"):
        current_excluded_workspaces = str(summary["excluded_workspaces"])
    else:
        current_excluded_workspaces = settings.excluded_workspaces_raw
    context = {
        "request": request,
        "app_title": settings.app_title,
        "summary": summary,
        "latest_run": latest_run,
        "running_jobs": [job_status.serialize_job_row(item) for item in db.list_running_jobs(settings.database_path)],
        "current_excluded_workspaces": current_excluded_workspaces,
    }
    if latest_run is not None:
        context["table_state"] = build_table_state(request, int(latest_run["id"]))
    return TEMPLATES.TemplateResponse(request, "stores.html", context)


@router.get("/stores/table", response_class=HTMLResponse)
def stores_table(request: Request):
    settings = request_runtime(request).settings
    latest_run = db.get_latest_completed_run(settings.database_path)
    if latest_run is None:
        return HTMLResponse("<div class='empty-panel'>No completed inventory snapshot is available yet.</div>")
    return TEMPLATES.TemplateResponse(
        request,
        "_stores_table.html",
        {
            "table_state": build_table_state(request, int(latest_run["id"])),
        },
    )


@router.post("/scan")
def start_scan(request: Request, exclude_workspaces: str = Form("")) -> RedirectResponse:
    try:
        job_id = request.app.state.job_manager.start_scan(exclude_workspaces.strip())
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return RedirectResponse(url="/jobs/{}".format(job_id), status_code=303)


@router.get("/jobs/{job_id}", response_class=HTMLResponse)
def job_detail(request: Request, job_id: int):
    settings = request_runtime(request).settings
    job = db.get_job(settings.database_path, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return TEMPLATES.TemplateResponse(
        request,
        "job_detail.html",
        {"job": job_status.serialize_job_row(job)},
    )


@router.get("/jobs/{job_id}/header", response_class=HTMLResponse)
def job_header_fragment(request: Request, job_id: int):
    settings = request_runtime(request).settings
    job = db.get_job(settings.database_path, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return TEMPLATES.TemplateResponse(
        request,
        "_job_header.html",
        {"job": job_status.serialize_job_row(job)},
    )


@router.get("/jobs/{job_id}/status", response_class=HTMLResponse)
def job_status_fragment(request: Request, job_id: int):
    settings = request_runtime(request).settings
    job = db.get_job(settings.database_path, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return TEMPLATES.TemplateResponse(
        request,
        "_job_status.html",
        {"job": job_status.serialize_job_row(job)},
    )


@router.post("/delete/preview", response_class=HTMLResponse)
def delete_preview(request: Request, selected_ids: str = Form("")):
    settings = request_runtime(request).settings
    latest_run = latest_run_or_404(settings)
    store_ids = deletion.parse_selected_ids(selected_ids)
    preview = deletion.build_delete_preview(
        settings.database_path,
        settings,
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


@router.post("/delete/execute")
def delete_execute(
    request: Request,
    selected_ids: str = Form(""),
    run_id: int = Form(...),
) -> RedirectResponse:
    settings = request_runtime(request).settings
    store_ids = deletion.parse_selected_ids(selected_ids)
    if not store_ids:
        raise HTTPException(status_code=400, detail="No stores were selected.")
    preview = deletion.build_delete_preview(
        settings.database_path,
        settings,
        run_id,
        store_ids,
    )
    valid_store_ids = preview["selected_ids"]
    if not valid_store_ids:
        raise HTTPException(status_code=400, detail="No deletable store rows were selected.")
    run = db.get_run(settings.database_path, run_id)
    excluded_workspaces_raw = str(run["excluded_workspaces"]) if run is not None else settings.excluded_workspaces_raw
    try:
        job_id = request.app.state.job_manager.start_delete(run_id, valid_store_ids, excluded_workspaces_raw)
    except Exception as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return RedirectResponse(url="/jobs/{}".format(job_id), status_code=303)


@router.get("/reports/latest.csv")
def download_latest_csv(request: Request) -> Response:
    settings = request_runtime(request).settings
    latest_run = latest_run_or_404(settings)
    filename = build_report_filename(int(latest_run["id"]), "csv")
    _, content = snapshots.build_snapshot_csv_bytes(settings.database_path, run_id=int(latest_run["id"]))
    return Response(
        content=content,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="{}"'.format(filename)},
    )


@router.get("/reports/latest.html")
def download_latest_html(request: Request) -> Response:
    settings = request_runtime(request).settings
    latest_run = latest_run_or_404(settings)
    filename = build_report_filename(int(latest_run["id"]), "html")
    _, content = snapshots.build_snapshot_html_text(
        settings.database_path,
        settings,
        run_id=int(latest_run["id"]),
    )
    return Response(
        content=content,
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="{}"'.format(filename)},
    )


app = create_app()
SETTINGS = app.state.settings
RUNTIME = app.state.runtime
