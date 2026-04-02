import importlib
import os
import sys
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient


class DummyResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyOpener:
    def __init__(self):
        self.request = None
        self.timeout = None

    def open(self, request, timeout=None):
        self.request = request
        self.timeout = timeout
        return DummyResponse()


class DummyClient:
    def __init__(self, base_url: str, timeout: int):
        self.base_url = base_url
        self.timeout = timeout
        self.opener = DummyOpener()


class GeoServerCleanerAppTests(unittest.TestCase):
    def load_app(self, temp_dir: str):
        env_updates = {
            "APP_DATABASE_PATH": os.path.join(temp_dir, "geoserver_cleaner.sqlite3"),
            "GEOSERVER_DATA_DIR": temp_dir,
            "GEOSERVER_URL": "http://example.test/geoserver",
            "GEOSERVER_USER": "admin",
            "GEOSERVER_PASSWORD": "secret",
        }
        patcher = patch.dict(os.environ, env_updates, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

        sys.modules.pop("app.main", None)
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

    def test_stores_page_renders_latest_snapshot_and_download_actions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data", "raster"), exist_ok=True)
            module = self.load_app(temp_dir)
            self.seed_run(module, [self.make_row(temp_dir)])
            with TestClient(module.app) as client:
                response = client.get("/stores")
                self.assertEqual(response.status_code, 200)
                self.assertIn("GeoServer Cleaner", response.text)
                self.assertIn("demo", response.text)
                self.assertIn("Preview Delete", response.text)
                self.assertIn("CSV Report", response.text)
                self.assertIn("HTML Report", response.text)
                self.assertNotIn("Physical Delete", response.text)
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
                    self.make_row(temp_dir, workspace="raster", store_name="demo_raster"),
                    self.make_row(
                        temp_dir,
                        store_kind="datastores",
                        workspace="cultural",
                        store_name="demo_vector",
                        store_type="GeoPackage",
                        layer_names="demo_vector",
                        configured_path="",
                        resolved_path="",
                        normalized_path="",
                        path_kind="",
                    ),
                ],
            )
            with TestClient(module.app) as client:
                response = client.get("/stores/table?workspace=raster")
                self.assertEqual(response.status_code, 200)
                self.assertIn("demo_raster", response.text)
                self.assertNotIn("demo_vector", response.text)

    def test_delete_preview_marks_internal_store_as_data_deletable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_path = os.path.join(temp_dir, "data", "raster", "demo.tif")
            os.makedirs(os.path.dirname(data_path), exist_ok=True)
            open(data_path, "wb").close()
            module = self.load_app(temp_dir)
            run_id = self.seed_run(module, [self.make_row(temp_dir, resolved_path=data_path, normalized_path=os.path.normcase(os.path.normpath(data_path)))])
            row = module.db.get_run_rows(module.SETTINGS.database_path, run_id)[0]
            with TestClient(module.app) as client:
                response = client.post("/delete/preview", data={"selected_ids": str(row["id"])})
                self.assertEqual(response.status_code, 200)
                self.assertIn("Delete Data", response.text)
                self.assertIn("GeoServer will delete store configuration and internal data.", response.text)

    def test_delete_preview_marks_external_store_as_configuration_only(self):
        with tempfile.TemporaryDirectory() as temp_dir, tempfile.TemporaryDirectory() as external_dir:
            external_path = os.path.join(external_dir, "external.tif")
            open(external_path, "wb").close()
            module = self.load_app(temp_dir)
            run_id = self.seed_run(
                module,
                [
                    self.make_row(
                        temp_dir,
                        resolved_path=external_path,
                        normalized_path=os.path.normcase(os.path.normpath(external_path)),
                        configured_path=external_path,
                    )
                ],
            )
            row = module.db.get_run_rows(module.SETTINGS.database_path, run_id)[0]
            with TestClient(module.app) as client:
                response = client.post("/delete/preview", data={"selected_ids": str(row["id"])})
                self.assertEqual(response.status_code, 200)
                self.assertIn("GeoServer will delete store configuration only; data is outside data_dir.", response.text)

    def test_delete_preview_marks_unresolved_store_as_configuration_only(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            run_id = self.seed_run(
                module,
                [
                    self.make_row(
                        temp_dir,
                        configured_path="file:data/raster/missing.tif",
                        resolved_path="",
                        normalized_path="",
                        path_kind="",
                        status="unresolved",
                    )
                ],
            )
            row = module.db.get_run_rows(module.SETTINGS.database_path, run_id)[0]
            with TestClient(module.app) as client:
                response = client.post("/delete/preview", data={"selected_ids": str(row["id"])})
                self.assertEqual(response.status_code, 200)
                self.assertIn("GeoServer will delete store configuration only; path is unresolved or missing.", response.text)

    def test_orphan_rows_cannot_be_deleted(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            run_id = self.seed_run(
                module,
                [
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
                        path_kind="dir",
                        status="orphaned",
                    )
                ],
            )
            row = module.db.get_run_rows(module.SETTINGS.database_path, run_id)[0]
            with TestClient(module.app) as client:
                preview_response = client.post("/delete/preview", data={"selected_ids": str(row["id"])})
                self.assertEqual(preview_response.status_code, 200)
                self.assertIn("Orphan rows are report-only and cannot be deleted here.", preview_response.text)
                execute_response = client.post(
                    "/delete/execute",
                    data={"selected_ids": str(row["id"]), "run_id": str(run_id)},
                )
                self.assertEqual(execute_response.status_code, 400)
                self.assertIn("No deletable store rows were selected.", execute_response.text)

    def test_delete_execute_filters_to_valid_store_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data", "raster"), exist_ok=True)
            module = self.load_app(temp_dir)
            run_id = self.seed_run(
                module,
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
                        path_kind="dir",
                        status="orphaned",
                    ),
                ],
            )
            rows = module.db.get_run_rows(module.SETTINGS.database_path, run_id)
            valid_id = int(rows[0]["id"])
            orphan_id = int(rows[1]["id"])
            with TestClient(module.app) as client:
                with patch.object(module.app.state.job_manager, "start_delete", return_value=77) as mock_start_delete:
                    response = client.post(
                        "/delete/execute",
                        data={"selected_ids": "{},{}".format(valid_id, orphan_id), "run_id": str(run_id)},
                        follow_redirects=False,
                    )
                self.assertEqual(response.status_code, 303)
                self.assertEqual(response.headers["location"], "/jobs/77")
                mock_start_delete.assert_called_once_with(run_id, [valid_id], "")

    def test_execute_delete_job_does_not_remove_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_path = os.path.join(temp_dir, "data", "raster", "demo.tif")
            os.makedirs(os.path.dirname(data_path), exist_ok=True)
            open(data_path, "wb").close()
            module = self.load_app(temp_dir)
            run_id = self.seed_run(
                module,
                [
                    self.make_row(
                        temp_dir,
                        resolved_path=data_path,
                        normalized_path=os.path.normcase(os.path.normpath(data_path)),
                    )
                ],
            )
            row = module.db.get_run_rows(module.SETTINGS.database_path, run_id)[0]
            with patch.object(module.deletion.geoserver, "delete_store", return_value=None):
                result = module.deletion.execute_delete_job(
                    module.SETTINGS.database_path,
                    module.SETTINGS,
                    run_id,
                    [int(row["id"])],
                )
            self.assertTrue(os.path.exists(data_path))
            self.assertEqual(result["deleted_count"], 1)
            self.assertEqual(result["delete_data_count"], 1)

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

    def test_job_header_fragment_shows_live_progress_summary(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            job_id = module.db.create_job(
                module.SETTINGS.database_path,
                "delete",
                "Delete job running",
                metadata={
                    "phase": "delete",
                    "deleted_count": 2,
                    "remaining_delete_items": 1,
                    "processed_delete_items": 2,
                    "total_delete_items": 3,
                },
            )
            module.db.update_job(
                module.SETTINGS.database_path,
                job_id,
                status="running",
                message="Delete job running",
            )
            with TestClient(module.app) as client:
                response = client.get(f"/jobs/{job_id}/header")
                self.assertEqual(response.status_code, 200)
                self.assertIn("Delete job running", response.text)
                self.assertNotIn("Deleted 2 stores, remaining 1", response.text)

    def test_completed_job_status_shows_progress_once(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data"), exist_ok=True)
            module = self.load_app(temp_dir)
            job_id = module.db.create_job(
                module.SETTINGS.database_path,
                "delete",
                "Delete job completed and inventory refreshed",
                metadata={
                    "phase": "completed",
                    "deleted_count": 47,
                    "remaining_delete_items": 0,
                    "processed_delete_items": 47,
                    "total_delete_items": 47,
                    "eta_seconds": 0,
                },
            )
            module.db.update_job(
                module.SETTINGS.database_path,
                job_id,
                status="completed",
                message="Delete job completed and inventory refreshed",
                finished=True,
            )
            with TestClient(module.app) as client:
                response = client.get(f"/jobs/{job_id}")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.text.count("Deleted 47 stores, remaining 0"), 1)
                self.assertNotIn("Phase: completed", response.text)
                self.assertNotIn("Estimated remaining time: 0s", response.text)

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

    def test_latest_csv_download_uses_completed_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data", "raster"), exist_ok=True)
            module = self.load_app(temp_dir)
            self.seed_run(
                module,
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
                        path_kind="dir",
                        status="orphaned",
                    ),
                ],
            )
            with TestClient(module.app) as client:
                response = client.get("/reports/latest.csv")
                self.assertEqual(response.status_code, 200)
                self.assertIn("attachment; filename=", response.headers["content-disposition"])
                text = response.content.decode("utf-8-sig")
                self.assertIn("workspace,store_name", text)
                self.assertIn("demo", text)
                self.assertIn("orphaned_demo", text)

    def test_latest_html_download_uses_completed_snapshot(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, "data", "raster"), exist_ok=True)
            module = self.load_app(temp_dir)
            self.seed_run(module, [self.make_row(temp_dir)])
            with TestClient(module.app) as client:
                response = client.get("/reports/latest.html")
                self.assertEqual(response.status_code, 200)
                self.assertIn("attachment; filename=", response.headers["content-disposition"])
                self.assertIn("GeoServer Store Report", response.text)
                self.assertIn("demo", response.text)

    def test_geoserver_delete_uses_recurse_and_purge_all(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            module = self.load_app(temp_dir)
            dummy_client = DummyClient("http://example.test/geoserver/", module.SETTINGS.timeout)
            with patch.object(
                module.deletion.geoserver.report,
                "GeoServerClient",
                return_value=dummy_client,
            ):
                module.deletion.geoserver.delete_store(
                    module.SETTINGS,
                    "raster",
                    "coveragestores",
                    "demo store",
                )
            self.assertIsNotNone(dummy_client.opener.request)
            self.assertIn("recurse=true", dummy_client.opener.request.full_url)
            self.assertIn("purge=all", dummy_client.opener.request.full_url)


if __name__ == "__main__":
    unittest.main()
