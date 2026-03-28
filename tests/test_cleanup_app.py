import importlib
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient


class CleanupAppTests(unittest.TestCase):
    def load_app(self, temp_dir: str, allow_physical_delete: bool = False):
        env_updates = {
            "APP_DATABASE_PATH": os.path.join(temp_dir, "app.sqlite3"),
            "GEOSERVER_DATA_DIR": temp_dir,
            "GEOSERVER_URL": "http://example.test/geoserver",
            "GEOSERVER_USER": "admin",
            "GEOSERVER_PASSWORD": "secret",
            "ALLOW_PHYSICAL_DELETE": "true" if allow_physical_delete else "false",
        }
        patcher = patch.dict(os.environ, env_updates, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

        sys.modules.pop("app.main", None)
        module = importlib.import_module("app.main")
        return module

    def seed_run(self, module, rows):
        run_id = module.db.create_inventory_run(
            module.SETTINGS.database_path,
            catalog_source="filesystem",
            excluded_workspaces=[],
            geoserver_url=module.SETTINGS.geoserver_url,
            data_dir=module.SETTINGS.data_dir,
        )
        module.db.replace_run_rows(module.SETTINGS.database_path, run_id, rows)
        module.db.finalize_inventory_run(
            module.SETTINGS.database_path,
            run_id,
            status="completed",
            store_count=len([row for row in rows if row["row_kind"] == "store"]),
            orphan_count=len([row for row in rows if row["row_kind"] == "orphaned"]),
            issue_count=len([row for row in rows if row["row_kind"] == "store" and row["status"] != "ok"]),
            tracked_size_bytes=sum(int(row["size_bytes"]) for row in rows if row["row_kind"] == "store"),
            notes="",
        )
        return run_id

    def test_stores_page_renders_latest_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            self.seed_run(
                module,
                [
                    {
                        "store_kind": "coveragestores",
                        "row_kind": "store",
                        "workspace": "raster",
                        "store_name": "demo",
                        "store_type": "GeoTIFF",
                        "layer_names": "demo",
                        "configured_path": "file:data/raster/demo.tif",
                        "resolved_path": os.path.join(temp_dir, "data", "raster", "demo.tif"),
                        "normalized_path": os.path.normcase(
                            os.path.normpath(os.path.join(temp_dir, "data", "raster", "demo.tif"))
                        ),
                        "path_kind": "file",
                        "size_bytes": 1024,
                        "size_gb": "0.00",
                        "file_count": 1,
                        "status": "ok",
                        "notes": "",
                    }
                ],
            )
            with TestClient(module.app) as client:
                response = client.get("/stores")
                self.assertEqual(response.status_code, 200)
                self.assertIn("GeoServer Cleanup Console", response.text)
                self.assertIn("demo", response.text)
                self.assertIn("Preview Delete", response.text)

    def test_delete_preview_blocks_shared_path_file_delete(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_path = os.path.join(temp_dir, "data", "shared", "countries.gpkg")
            os.makedirs(os.path.dirname(data_path), exist_ok=True)
            open(data_path, "wb").close()
            module = self.load_app(temp_dir, allow_physical_delete=True)
            run_id = self.seed_run(
                module,
                [
                    {
                        "store_kind": "datastores",
                        "row_kind": "store",
                        "workspace": "cultural",
                        "store_name": "countries_a",
                        "store_type": "GeoPackage",
                        "layer_names": "countries",
                        "configured_path": data_path,
                        "resolved_path": data_path,
                        "normalized_path": os.path.normcase(os.path.normpath(data_path)),
                        "path_kind": "file",
                        "size_bytes": 2048,
                        "size_gb": "0.00",
                        "file_count": 1,
                        "status": "ok",
                        "notes": "",
                    },
                    {
                        "store_kind": "datastores",
                        "row_kind": "store",
                        "workspace": "politics",
                        "store_name": "countries_b",
                        "store_type": "GeoPackage",
                        "layer_names": "countries",
                        "configured_path": data_path,
                        "resolved_path": data_path,
                        "normalized_path": os.path.normcase(os.path.normpath(data_path)),
                        "path_kind": "file",
                        "size_bytes": 2048,
                        "size_gb": "0.00",
                        "file_count": 1,
                        "status": "ok",
                        "notes": "",
                    },
                ],
            )
            first_row = module.db.get_rows_by_ids(module.SETTINGS.database_path, run_id, [1])[0]
            with TestClient(module.app) as client:
                response = client.post("/delete/preview", data={"selected_ids": str(first_row["id"])})
                self.assertEqual(response.status_code, 200)
                self.assertIn("Physical delete blocked because the path is shared with another store.", response.text)

    def test_delete_execute_redirects_to_job(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            run_id = self.seed_run(module, [])
            with TestClient(module.app) as client:
                with patch.object(module.app.state.job_manager, "start_delete", return_value=77):
                    response = client.post(
                        "/delete/execute",
                        data={"selected_ids": "1,2,3", "run_id": str(run_id)},
                        follow_redirects=False,
                    )
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/jobs/77")

    def test_scan_route_passes_excluded_workspaces_to_job_manager(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            with TestClient(module.app) as client:
                with patch.object(module.app.state.job_manager, "start_scan", return_value=41) as mock_start_scan:
                    response = client.post(
                        "/scan",
                        data={"exclude_workspaces": "cultural,politics"},
                        follow_redirects=False,
                    )
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/jobs/41")
                mock_start_scan.assert_called_once_with("cultural,politics")
