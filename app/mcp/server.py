import logging

from mcp.server.fastmcp import FastMCP

from app import db
from app.runtime import AppRuntime, build_runtime
from app.services import job_status, snapshots


LOGGER = logging.getLogger("geoserver_cleaner.mcp")


class GeoServerCleanerMcpService:
    def __init__(self, runtime: AppRuntime) -> None:
        self.runtime = runtime

    @property
    def settings(self):
        return self.runtime.settings

    def _job_payload(self, job_id: int) -> dict:
        job = db.get_job(self.settings.database_path, job_id)
        if job is None:
            raise RuntimeError("Job {} was not found after creation.".format(job_id))
        serialized = job_status.serialize_job_row(job)
        return {
            "job_id": int(serialized["id"]),
            "status": str(serialized["status"]),
            "message": str(serialized["message"]),
            "progress_summary": str(serialized.get("progress_summary") or ""),
            "metadata": serialized.get("metadata") or {},
            "run_id": serialized.get("run_id"),
        }

    def get_latest_snapshot(self) -> dict:
        return snapshots.get_snapshot_metadata(self.settings.database_path)

    def start_inventory_scan(self, excluded_workspaces: list[str] | None = None, excluded_workspaces_raw: str = "") -> dict:
        values: list[str] = []
        if excluded_workspaces:
            values.extend(str(item).strip() for item in excluded_workspaces if str(item).strip())
        if excluded_workspaces_raw.strip():
            values.extend(
                value.strip()
                for value in excluded_workspaces_raw.split(",")
                if value.strip()
            )
        raw = ", ".join(values)
        job_id = self.runtime.job_manager.start_scan(raw)
        return self._job_payload(job_id)

    def get_job_status(self, job_id: int) -> dict:
        job = db.get_job(self.settings.database_path, int(job_id))
        if job is None:
            raise RuntimeError("Job {} was not found.".format(job_id))
        serialized = job_status.serialize_job_row(job)
        return {
            "job_id": int(serialized["id"]),
            "job_type": str(serialized["job_type"]),
            "status": str(serialized["status"]),
            "message": str(serialized["message"]),
            "progress_summary": str(serialized.get("progress_summary") or ""),
            "eta_display": str(serialized.get("eta_display") or ""),
            "created_at": str(serialized.get("created_at") or ""),
            "started_at": str(serialized.get("started_at") or ""),
            "finished_at": str(serialized.get("finished_at") or ""),
            "run_id": serialized.get("run_id"),
            "metadata": serialized.get("metadata") or {},
            "error_text": str(serialized.get("error_text") or ""),
        }

    def list_heaviest_stores(
        self,
        limit: int = 5,
        workspace: str = "",
        store_type: str = "",
        status: str = "",
        include_orphans: bool = False,
    ) -> dict:
        return snapshots.list_heaviest_stores(
            self.settings.database_path,
            limit=limit,
            workspace=workspace.strip(),
            store_type=store_type.strip(),
            status=status.strip(),
            include_orphans=include_orphans,
        )

    def summarize_workspace_usage(self, workspace: str = "", include_issues: bool = True) -> dict:
        return snapshots.summarize_workspace_usage(
            self.settings.database_path,
            workspace=workspace.strip(),
            include_issues=include_issues,
        )

    def list_orphans(self, limit: int = 50, path_filter: str = "", sort_order: str = "size_desc") -> dict:
        return snapshots.list_orphans(
            self.settings.database_path,
            limit=limit,
            path_filter=path_filter.strip(),
            sort_order=sort_order.strip(),
        )

    def find_stores(
        self,
        q: str = "",
        workspace: str = "",
        status: str = "",
        row_kind: str = "store",
        store_type: str = "",
        sort_by: str = "size_bytes",
        sort_dir: str = "desc",
        limit: int = 50,
    ) -> dict:
        return snapshots.find_stores(
            self.settings.database_path,
            q=q,
            workspace=workspace.strip(),
            status=status.strip(),
            row_kind=row_kind.strip(),
            store_type=store_type.strip(),
            sort_by=sort_by.strip(),
            sort_dir=sort_dir.strip(),
            limit=limit,
        )

    def delete_stores(self, store_ids: list[int] | None = None, store_keys: list[str] | None = None) -> dict:
        selection = snapshots.resolve_store_selection(
            self.settings.database_path,
            self.settings,
            store_ids=store_ids or [],
            store_keys=store_keys or [],
        )
        accepted_ids = selection["selected_ids"]
        run_id = int(selection["snapshot"]["run_id"])
        result = {
            "snapshot": selection["snapshot"],
            "accepted": selection["accepted"],
            "blocked": selection["blocked"],
            "accepted_store_ids": accepted_ids,
        }
        if not accepted_ids:
            result.update(
                {
                    "job_id": None,
                    "status": "blocked",
                    "message": "No deletable store rows were selected.",
                }
            )
            return result
        run = db.get_run(self.settings.database_path, run_id)
        excluded_workspaces_raw = str(run["excluded_workspaces"] or "") if run is not None else self.settings.excluded_workspaces_raw
        job_id = self.runtime.job_manager.start_delete(run_id, accepted_ids, excluded_workspaces_raw)
        result.update(self._job_payload(job_id))
        return result

    def export_snapshot_csv(self, run_id: int | None = None) -> dict:
        return snapshots.write_snapshot_export(
            self.settings.database_path,
            self.settings,
            format_name="csv",
            run_id=run_id,
        )

    def export_snapshot_html(self, run_id: int | None = None) -> dict:
        return snapshots.write_snapshot_export(
            self.settings.database_path,
            self.settings,
            format_name="html",
            run_id=run_id,
        )


def build_mcp_server(runtime: AppRuntime) -> FastMCP:
    server = FastMCP("GeoServer Cleaner")
    service = GeoServerCleanerMcpService(runtime)
    server.tool(description="Return metadata for the latest completed inventory snapshot.")(service.get_latest_snapshot)
    server.tool(description="Queue a new inventory scan job and return its job id and initial state.")(service.start_inventory_scan)
    server.tool(description="Return the latest job status, progress summary, and metadata for a job id.")(service.get_job_status)
    server.tool(description="List the heaviest store rows from the latest completed snapshot.")(service.list_heaviest_stores)
    server.tool(description="Summarize store count, size, file count, and issue count by workspace.")(service.summarize_workspace_usage)
    server.tool(description="List orphan rows from the latest completed snapshot. Orphans are report-only and cannot be deleted.")(service.list_orphans)
    server.tool(description="Find snapshot rows by text and optional filters.")(service.find_stores)
    server.tool(description="Queue a GeoServer REST delete job for selected store ids or workspace/store keys.")(service.delete_stores)
    server.tool(description="Write the latest completed snapshot CSV report to the export directory and return the file path.")(service.export_snapshot_csv)
    server.tool(description="Write the latest completed snapshot HTML report to the export directory and return the file path.")(service.export_snapshot_html)
    return server


def build_streamable_http_server(runtime: AppRuntime) -> FastMCP:
    server = build_mcp_server(runtime)
    server.settings.streamable_http_path = "/"
    server.streamable_http_app()
    return server


def run_stdio_server() -> None:
    runtime = build_runtime()
    server = build_mcp_server(runtime)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    LOGGER.info("Starting GeoServer Cleaner MCP stdio server with database %s", runtime.settings.database_path)
    server.run(transport="stdio")


if __name__ == "__main__":
    run_stdio_server()
