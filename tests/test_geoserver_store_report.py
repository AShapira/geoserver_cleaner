import os
import tempfile
import unittest
from unittest.mock import patch

import geoserver_store_report as report


class GeoServerStoreReportTests(unittest.TestCase):
    def test_filesystem_catalog_inventory_uses_local_workspaces(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = os.path.join(temp_dir, "data")
            workspaces_root = os.path.join(temp_dir, "workspaces")
            raster_dir = os.path.join(data_root, "fast_ws", "raster")
            store_dir = os.path.join(workspaces_root, "fast_ws", "fast_store")
            coverage_dir = os.path.join(store_dir, "fast_store")
            os.makedirs(raster_dir, exist_ok=True)
            os.makedirs(coverage_dir, exist_ok=True)

            tif_path = os.path.join(raster_dir, "fast_store.tif")
            with open(tif_path, "wb") as handle:
                handle.write(b"mock")

            with open(os.path.join(workspaces_root, "fast_ws", "workspace.xml"), "w", encoding="utf-8") as handle:
                handle.write("<workspace><name>fast_ws</name></workspace>")
            with open(os.path.join(store_dir, "coveragestore.xml"), "w", encoding="utf-8") as handle:
                handle.write(
                    "<coverageStore><name>fast_store</name><type>GeoTIFF</type>"
                    "<url>file:data/fast_ws/raster/fast_store.tif</url></coverageStore>"
                )
            with open(os.path.join(coverage_dir, "coverage.xml"), "w", encoding="utf-8") as handle:
                handle.write("<coverage><name>fast_layer</name></coverage>")

            with patch.object(report, "list_workspaces", side_effect=AssertionError("REST should not be used")):
                rows, referenced_roots, referenced_files = report.inventory_stores(
                    client=None,
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                    catalog_source="filesystem",
                    workers=2,
                )

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["workspace"], "fast_ws")
            self.assertEqual(rows[0]["store_name"], "fast_store")
            self.assertEqual(rows[0]["layer_names"], "fast_layer")
            self.assertEqual(rows[0]["status"], "ok")
            self.assertFalse(referenced_roots)
            self.assertIn(report.normalize_path(tif_path), referenced_files)

    def test_size_gb_is_rounded_to_two_decimals(self):
        row = report.build_row(
            row_kind="store",
            workspace="demo",
            store_name="sample",
            store_type="GeoTIFF",
            layer_names="layer",
            configured_path="file:data/demo/sample.tif",
            resolved_path=r"C:\data\demo\sample.tif",
            path_kind="file",
            size_bytes=int(1.5 * (1024 ** 3)),
            file_count=1,
            status="ok",
            notes="",
        )
        self.assertEqual(row["size_gb"], "1.50")

    def test_invalid_store_listing_continues(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = os.path.join(temp_dir, "data")
            os.makedirs(os.path.join(data_root, "ws_ok"), exist_ok=True)
            good_tif = os.path.join(data_root, "ws_ok", "good_store.tif")
            with open(good_tif, "wb") as handle:
                handle.write(b"demo")

            def fake_list_store_refs(_client, workspace, store_kind):
                if workspace == "ws_ok" and store_kind == "coveragestores":
                    return ["good_store"]
                if workspace == "ws_bad" and store_kind == "datastores":
                    raise RuntimeError("bad rest response")
                return []

            def fake_get_store_detail(_client, workspace, store_kind, store_name):
                self.assertEqual(workspace, "ws_ok")
                self.assertEqual(store_kind, "coveragestores")
                self.assertEqual(store_name, "good_store")
                return {"type": "GeoTIFF", "url": "file:data/ws_ok/good_store.tif"}

            with patch.object(report, "list_workspaces", return_value=["ws_ok", "ws_bad"]), patch.object(
                report,
                "list_store_refs",
                side_effect=fake_list_store_refs,
            ), patch.object(
                report,
                "get_store_detail",
                side_effect=fake_get_store_detail,
            ), patch.object(
                report,
                "list_store_layers",
                return_value=["layer_a"],
            ):
                rows, referenced_roots, referenced_files = report.inventory_stores(
                    client=None,
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                )

            ok_rows = [row for row in rows if row["status"] == "ok"]
            error_rows = [row for row in rows if row["status"] == "error"]
            self.assertEqual(len(ok_rows), 1)
            self.assertEqual(ok_rows[0]["store_name"], "good_store")
            self.assertTrue(error_rows)
            self.assertIn("bad rest response", error_rows[0]["notes"])
            self.assertFalse(referenced_roots)
            self.assertIn(report.normalize_path(good_tif), referenced_files)

    def test_excluded_workspace_is_not_reported_or_marked_orphan(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = os.path.join(temp_dir, "data")
            included_dir = os.path.join(data_root, "included")
            excluded_dir = os.path.join(data_root, "excluded_ws")
            os.makedirs(included_dir, exist_ok=True)
            os.makedirs(excluded_dir, exist_ok=True)

            included_tif = os.path.join(included_dir, "kept.tif")
            excluded_tif = os.path.join(excluded_dir, "store.tif")
            excluded_extra = os.path.join(excluded_dir, "unused.txt")
            for path, content in (
                (included_tif, b"ok"),
                (excluded_tif, b"skip"),
                (excluded_extra, b"hidden"),
            ):
                with open(path, "wb") as handle:
                    handle.write(content)

            def fake_list_store_refs(_client, workspace, store_kind):
                if store_kind != "coveragestores":
                    return []
                if workspace == "included":
                    return ["included_store"]
                if workspace == "excluded_ws":
                    return ["excluded_store"]
                return []

            def fake_get_store_detail(_client, workspace, _store_kind, store_name):
                if workspace == "included":
                    return {"type": "GeoTIFF", "url": "file:data/included/kept.tif"}
                if workspace == "excluded_ws":
                    return {"type": "GeoTIFF", "url": "file:data/excluded_ws/store.tif"}
                raise AssertionError(store_name)

            with patch.object(report, "list_workspaces", return_value=["included", "excluded_ws"]), patch.object(
                report,
                "list_store_refs",
                side_effect=fake_list_store_refs,
            ), patch.object(
                report,
                "get_store_detail",
                side_effect=fake_get_store_detail,
            ), patch.object(
                report,
                "list_store_layers",
                return_value=["layer_a"],
            ):
                rows, referenced_roots, referenced_files = report.inventory_stores(
                    client=None,
                    data_dir=temp_dir,
                    excluded_workspaces={"excluded_ws"},
                )

            self.assertEqual([row["store_name"] for row in rows], ["included_store"])
            self.assertIn(report.normalize_path(excluded_dir), referenced_roots)
            orphan_rows = report.collect_orphans(data_root, referenced_roots, referenced_files)
            orphan_paths = [row["resolved_path"] for row in orphan_rows]
            self.assertFalse(any("excluded_ws" in path for path in orphan_paths))

    def test_html_report_is_generated_with_sorting_ui(self):
        rows = [
            report.build_row(
                row_kind="store",
                workspace="demo",
                store_name="sample",
                store_type="GeoTIFF",
                layer_names="layer",
                configured_path="file:data/demo/sample.tif",
                resolved_path=r"C:\data\demo\sample.tif",
                path_kind="file",
                size_bytes=123,
                file_count=1,
                status="ok",
                notes="",
            )
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            output_html = os.path.join(temp_dir, "report.html")
            report.write_html_report(
                output_html,
                rows,
                ["skip_me"],
                "http://localhost:8081/geoserver",
                temp_dir,
            )
            with open(output_html, "r", encoding="utf-8") as handle:
                html_text = handle.read()

        self.assertIn("GeoServer Store Report", html_text)
        self.assertIn('id="reportTable"', html_text)
        self.assertIn('class="sortable"', html_text)
        self.assertIn('id="pageSize"', html_text)
        self.assertIn('id="reportRows"', html_text)
        self.assertIn("<tbody></tbody>", html_text)
        self.assertIn("skip_me", html_text)


if __name__ == "__main__":
    unittest.main()
