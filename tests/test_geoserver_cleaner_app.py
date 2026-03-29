import importlib
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient


class GeoServerCleanerAppTests(unittest.TestCase):
    def load_app(self, temp_dir: str, allow_physical_delete: bool = False):
        env_updates = {
            "APP_DATABASE_PATH": os.path.join(temp_dir, "geoserver_cleaner.sqlite3"),
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
                self.assertIn("GeoServer Cleaner", response.text)
                self.assertIn("demo", response.text)
                self.assertIn("Preview Delete", response.text)
                self.assertIn(">Workspace<", response.text)
                type_index = response.text.index("Type</a></th>")
                size_index = response.text.index("Size (GB)</a></th>")
                files_index = response.text.index("Files</a></th>")
                status_index = response.text.index("Status</a></th>")
                self.assertLess(type_index, size_index)
                self.assertLess(size_index, files_index)
                self.assertLess(files_index, status_index)

    def test_stores_table_filters_by_workspace(self):
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
                        "store_name": "demo_raster",
                        "store_type": "GeoTIFF",
                        "layer_names": "demo_raster",
                        "configured_path": "",
                        "resolved_path": "",
                        "normalized_path": "",
                        "path_kind": "",
                        "size_bytes": 100,
                        "size_gb": "0.00",
                        "file_count": 1,
                        "status": "ok",
                        "notes": "",
                    },
                    {
                        "store_kind": "datastores",
                        "row_kind": "store",
                        "workspace": "cultural",
                        "store_name": "demo_vector",
                        "store_type": "GeoPackage",
                        "layer_names": "demo_vector",
                        "configured_path": "",
                        "resolved_path": "",
                        "normalized_path": "",
                        "path_kind": "",
                        "size_bytes": 100,
                        "size_gb": "0.00",
                        "file_count": 1,
                        "status": "ok",
                        "notes": "",
                    },
                ],
            )
            with TestClient(module.app) as client:
                response = client.get("/stores/table?workspace=raster")
                self.assertEqual(response.status_code, 200)
                self.assertIn("demo_raster", response.text)
                self.assertNotIn("demo_vector", response.text)

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

    def test_job_status_fragment_shows_progress_and_eta(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            job_id = module.db.create_job(
                module.SETTINGS.database_path,
                "scan",
                "Scanning stores 50/200",
                metadata={
                    "phase": "stores",
                    "processed_stores": 50,
                    "total_stores": 200,
                    "progress_percent": 25.0,
                    "eta_seconds": 120,
                },
            )
            module.db.update_job(
                module.SETTINGS.database_path,
                job_id,
                status="running",
                message="Scanning stores 50/200",
            )
            with TestClient(module.app) as client:
                response = client.get(f"/jobs/{job_id}/status")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Progress: 50/200", response.text)
                self.assertIn("Scanned 50 stores, remaining 150", response.text)
                self.assertIn("Estimated remaining time: 2m 0s", response.text)

    def test_scan_job_status_fragment_shows_discovery_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            job_id = module.db.create_job(
                module.SETTINGS.database_path,
                "scan",
                "Discovering stores in GeoServer catalog",
                metadata={
                    "phase": "discovering",
                    "discovered_store_count": 73,
                    "processed_stores": 0,
                    "total_stores": None,
                },
            )
            module.db.update_job(
                module.SETTINGS.database_path,
                job_id,
                status="running",
                message="Discovering stores in GeoServer catalog",
            )
            with TestClient(module.app) as client:
                response = client.get(f"/jobs/{job_id}/status")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Discovered 73 stores so far", response.text)

    def test_delete_job_status_fragment_shows_delete_counts(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            job_id = module.db.create_job(
                module.SETTINGS.database_path,
                "delete",
                "Delete job running",
                metadata={
                    "phase": "delete",
                    "deleted_count": 3,
                    "remaining_delete_items": 5,
                    "processed_delete_items": 3,
                    "total_delete_items": 8,
                },
            )
            module.db.update_job(
                module.SETTINGS.database_path,
                job_id,
                status="running",
                message="Delete job running",
            )
            with TestClient(module.app) as client:
                response = client.get(f"/jobs/{job_id}/status")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Deleted 3 stores, remaining 5", response.text)
