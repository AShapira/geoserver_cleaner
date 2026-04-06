import importlib
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client
import uvicorn


def parse_tool_result(result):
    if not result.content:
        return {}
    return json.loads(result.content[0].text)


class UvicornThread:
    def __init__(self, app) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        self.port = int(sock.getsockname()[1])
        sock.close()
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        deadline = time.time() + 10
        while time.time() < deadline:
            if getattr(self.server, "started", False):
                return self
            if not self.thread.is_alive():
                raise RuntimeError("HTTP test server exited before startup.")
            time.sleep(0.05)
        raise RuntimeError("HTTP test server did not start in time.")

    def __exit__(self, exc_type, exc, tb):
        self.server.should_exit = True
        self.thread.join(timeout=10)
        return False


class GeoServerCleanerMcpTests(unittest.TestCase):
    def patch_env(self, temp_dir: str, extra_env: dict[str, str] | None = None):
        env_updates = {
            "APP_DATABASE_PATH": os.path.join(temp_dir, "geoserver_cleaner.sqlite3"),
            "APP_EXPORT_DIR": os.path.join(temp_dir, "exports"),
            "GEOSERVER_DATA_DIR": temp_dir,
            "GEOSERVER_URL": "http://example.test/geoserver",
            "GEOSERVER_USER": "admin",
            "GEOSERVER_PASSWORD": "secret",
        }
        if extra_env:
            env_updates.update(extra_env)
        patcher = patch.dict(os.environ, env_updates, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        return env_updates

    def load_server(self, temp_dir: str):
        self.patch_env(temp_dir)

        for name in ["app.mcp.server", "app.main"]:
            sys.modules.pop(name, None)
        module = importlib.import_module("app.mcp.server")
        runtime = module.build_runtime()
        service = module.GeoServerCleanerMcpService(runtime)
        return module, runtime, service

    def load_http_app(self, temp_dir: str):
        self.patch_env(temp_dir, {"APP_ENABLE_MCP_HTTP": "true"})
        for name in ["app.main", "app.mcp.server"]:
            sys.modules.pop(name, None)
        module = importlib.import_module("app.main")
        return module

    def make_row(self, temp_dir: str, **overrides):
        base_path = os.path.join(temp_dir, "data", "raster", "demo.tif")
        base = {
            "store_kind": "coveragestores",
            "row_kind": "store",
            "workspace": "raster",
            "store_name": "demo",
            "store_type": "GeoTIFF",
            "layer_names": "demo",
            "configured_path": "file:data/raster/demo.tif",
            "resolved_path": base_path,
            "normalized_path": os.path.normcase(os.path.normpath(base_path)),
            "path_kind": "file",
            "size_bytes": 1024,
            "size_gb": "0.00",
            "file_count": 1,
            "status": "ok",
            "notes": "",
        }
        base.update(overrides)
        return base

    def seed_run(self, module, runtime, rows):
        run_id = module.db.create_inventory_run(
            runtime.settings.database_path,
            catalog_source="filesystem",
            excluded_workspaces=[],
            geoserver_url=runtime.settings.geoserver_url,
            data_dir=runtime.settings.data_dir,
        )
        module.db.replace_run_rows(runtime.settings.database_path, run_id, rows)
        module.db.finalize_inventory_run(
            runtime.settings.database_path,
            run_id,
            status="completed",
            store_count=len([row for row in rows if row["row_kind"] == "store"]),
            orphan_count=len([row for row in rows if row["row_kind"] == "orphaned"]),
            issue_count=len([row for row in rows if row["row_kind"] == "store" and row["status"] != "ok"]),
            tracked_size_bytes=sum(int(row["size_bytes"]) for row in rows if row["row_kind"] == "store"),
            notes="",
        )
        return run_id

    def test_mcp_stdio_server_lists_tools_and_reads_latest_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            module, runtime, _service = self.load_server(temp_dir)
            os.makedirs(os.path.join(temp_dir, "data", "raster"), exist_ok=True)
            run_id = self.seed_run(module, runtime, [self.make_row(temp_dir)])

            async def exercise():
                env = os.environ.copy()
                env.update(
                    {
                        "APP_DATABASE_PATH": runtime.settings.database_path,
                        "APP_EXPORT_DIR": runtime.settings.export_dir,
                        "GEOSERVER_DATA_DIR": runtime.settings.data_dir,
                        "GEOSERVER_URL": runtime.settings.geoserver_url,
                        "GEOSERVER_USER": runtime.settings.geoserver_username,
                        "GEOSERVER_PASSWORD": runtime.settings.geoserver_password,
                    }
                )
                server = StdioServerParameters(
                    command=sys.executable,
                    args=["-m", "app.mcp.server"],
                    cwd=os.getcwd(),
                    env=env,
                )
                async with stdio_client(server) as (read, write):
                    async with ClientSession(read, write) as session:
                        await session.initialize()
                        tools = await session.list_tools()
                        names = {tool.name for tool in tools.tools}
                        self.assertIn("get_latest_snapshot", names)
                        self.assertIn("list_heaviest_stores", names)
                        response = await session.call_tool("get_latest_snapshot", {})
                        payload = parse_tool_result(response)
                        self.assertEqual(payload["run_id"], run_id)
                        self.assertEqual(payload["store_count"], 1)

            anyio.run(exercise)

    def test_list_heaviest_stores_returns_sorted_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            module, _runtime, service = self.load_server(temp_dir)
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            self.seed_run(
                module,
                _runtime,
                [
                    self.make_row(temp_dir, workspace="raster", store_name="small", size_bytes=1024),
                    self.make_row(temp_dir, workspace="raster", store_name="big", size_bytes=4096),
                    self.make_row(temp_dir, workspace="raster", store_name="mid", size_bytes=2048),
                ],
            )
            payload = service.list_heaviest_stores(limit=2)
            self.assertEqual([row["store_name"] for row in payload["rows"]], ["big", "mid"])
            self.assertTrue(payload["truncated"])

    def test_summarize_workspace_usage_aggregates_by_workspace(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            module, _runtime, service = self.load_server(temp_dir)
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            self.seed_run(
                module,
                _runtime,
                [
                    self.make_row(temp_dir, workspace="raster", store_name="a", size_bytes=1024, file_count=1),
                    self.make_row(temp_dir, workspace="raster", store_name="b", size_bytes=2048, file_count=3),
                    self.make_row(temp_dir, workspace="cultural", store_name="roads", size_bytes=4096, file_count=5),
                ],
            )
            payload = service.summarize_workspace_usage()
            workspaces = {item["workspace"]: item for item in payload["workspaces"]}
            self.assertEqual(workspaces["raster"]["store_count"], 2)
            self.assertEqual(workspaces["raster"]["total_size_bytes"], 3072)
            self.assertEqual(workspaces["raster"]["total_file_count"], 4)
            self.assertEqual(workspaces["cultural"]["store_count"], 1)

    def test_list_orphans_returns_only_orphan_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            module, _runtime, service = self.load_server(temp_dir)
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            self.seed_run(
                module,
                _runtime,
                [
                    self.make_row(temp_dir),
                    self.make_row(
                        temp_dir,
                        row_kind="orphaned",
                        store_kind="",
                        workspace="",
                        store_name="orphaned_demo",
                        store_type="orphaned",
                        layer_names="",
                        configured_path="",
                        resolved_path=os.path.join(temp_dir, "data", "orphaned_demo"),
                        normalized_path=os.path.normcase(os.path.normpath(os.path.join(temp_dir, "data", "orphaned_demo"))),
                        path_kind="directory",
                        status="orphaned",
                    ),
                ],
            )
            payload = service.list_orphans()
            self.assertEqual(payload["row_count"], 1)
            self.assertEqual(payload["rows"][0]["row_kind"], "orphaned")

    def test_delete_stores_rejects_orphans_and_starts_job_for_store_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            module, runtime, service = self.load_server(temp_dir)
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            run_id = self.seed_run(
                module,
                runtime,
                [
                    self.make_row(temp_dir, store_name="valid_store"),
                    self.make_row(
                        temp_dir,
                        row_kind="orphaned",
                        store_kind="",
                        workspace="",
                        store_name="orphaned_demo",
                        store_type="orphaned",
                        layer_names="",
                        configured_path="",
                        resolved_path=os.path.join(temp_dir, "data", "orphaned_demo"),
                        normalized_path=os.path.normcase(os.path.normpath(os.path.join(temp_dir, "data", "orphaned_demo"))),
                        path_kind="directory",
                        status="orphaned",
                    ),
                ],
            )
            rows = module.db.get_run_rows(runtime.settings.database_path, run_id)
            valid_id = int(rows[0]["id"])
            orphan_id = int(rows[1]["id"])
            with patch.object(runtime.job_manager, "start_delete", return_value=77) as mock_start_delete, patch.object(
                service,
                "_job_payload",
                return_value={"job_id": 77, "status": "queued", "message": "Delete job queued", "progress_summary": "", "metadata": {}, "run_id": run_id},
            ):
                payload = service.delete_stores(store_ids=[valid_id, orphan_id])
            self.assertEqual(payload["job_id"], 77)
            self.assertEqual(payload["accepted_store_ids"], [valid_id])
            self.assertEqual(len(payload["blocked"]), 1)
            self.assertIn("Orphan rows are report-only", payload["blocked"][0]["reason"])
            mock_start_delete.assert_called_once_with(run_id, [valid_id], "")

    def test_export_snapshot_tools_write_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            module, _runtime, service = self.load_server(temp_dir)
            os.makedirs(os.path.join(temp_dir, "data", "raster"), exist_ok=True)
            run_id = self.seed_run(module, _runtime, [self.make_row(temp_dir)])
            csv_payload = service.export_snapshot_csv(run_id=run_id)
            html_payload = service.export_snapshot_html(run_id=run_id)
            self.assertTrue(os.path.isfile(csv_payload["path"]))
            self.assertTrue(os.path.isfile(html_payload["path"]))
            self.assertEqual(csv_payload["format"], "csv")
            self.assertEqual(html_payload["format"], "html")
            self.assertIn("geoserver_cleaner_snapshot_", os.path.basename(csv_payload["path"]))
            self.assertIn("geoserver_cleaner_snapshot_", os.path.basename(html_payload["path"]))
            with open(csv_payload["path"], "r", encoding="utf-8-sig") as handle:
                self.assertIn("workspace,store_name", handle.read())
            with open(html_payload["path"], "r", encoding="utf-8") as handle:
                self.assertIn("GeoServer Cleaner Report", handle.read())

    def test_streamable_http_server_matches_stdio_tooling(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            http_module = self.load_http_app(temp_dir)
            os.makedirs(os.path.join(temp_dir, "data", "raster"), exist_ok=True)
            run_id = self.seed_run(http_module, http_module.RUNTIME, [self.make_row(temp_dir)])

            async def exercise():
                async with streamablehttp_client("http://127.0.0.1:{}/mcp/".format(server.port)) as (read_http, write_http, _):
                    async with ClientSession(read_http, write_http) as http_session:
                        await http_session.initialize()
                        http_tools = await http_session.list_tools()
                        http_names = {tool.name for tool in http_tools.tools}
                        http_response = await http_session.call_tool("get_latest_snapshot", {})
                        http_payload = parse_tool_result(http_response)

                stdio_env = os.environ.copy()
                stdio_server = StdioServerParameters(
                    command=sys.executable,
                    args=["-m", "app.mcp.server"],
                    cwd=os.getcwd(),
                    env=stdio_env,
                )
                async with stdio_client(stdio_server) as (read_stdio, write_stdio):
                    async with ClientSession(read_stdio, write_stdio) as stdio_session:
                        await stdio_session.initialize()
                        stdio_tools = await stdio_session.list_tools()
                        stdio_names = {tool.name for tool in stdio_tools.tools}
                        stdio_response = await stdio_session.call_tool("get_latest_snapshot", {})
                        stdio_payload = parse_tool_result(stdio_response)

                self.assertEqual(http_names, stdio_names)
                self.assertEqual(http_payload, stdio_payload)
                self.assertEqual(http_payload["run_id"], run_id)
                self.assertEqual(http_payload["store_count"], 1)

            with UvicornThread(http_module.app) as server:
                anyio.run(exercise)


if __name__ == "__main__":
    unittest.main()
