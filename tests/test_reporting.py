import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.reporting import cli as report_cli
from app.reporting import core as report
from app.reporting import render as report_render
from geoserver_test import populate_external_mapping_demo as mapping_demo


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

    def test_layer_group_membership_adds_store_warning(self):
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

            def fake_group_refs(_client, workspace=""):
                return ["global_group"] if not workspace else ["workspace_group"]

            def fake_group_detail(_client, group_name, workspace=""):
                if workspace:
                    return {"layers": {"layer": [{"name": "fast_ws:fast_layer"}]}}
                return {"layers": {"layer": [{"name": "fast_layer"}]}}

            with patch.object(report, "list_layer_group_refs", side_effect=fake_group_refs), patch.object(
                report,
                "get_layer_group_detail",
                side_effect=fake_group_detail,
            ):
                rows, _referenced_roots, _referenced_files = report.inventory_stores(
                    client=object(),
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                    catalog_source="filesystem",
                )

            self.assertIn("Warning: layer(s) are used by layer group(s):", rows[0]["notes"])
            self.assertIn("global_group", rows[0]["notes"])
            self.assertIn("fast_ws/workspace_group", rows[0]["notes"])

    def test_layer_group_lookup_failure_does_not_fail_inventory(self):
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

            with patch.object(report, "list_layer_group_refs", side_effect=RuntimeError("layergroups unavailable")):
                rows, _referenced_roots, _referenced_files = report.inventory_stores(
                    client=object(),
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                    catalog_source="filesystem",
                )

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "ok")
            self.assertEqual(rows[0]["notes"], "")

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

    def test_orphans_skip_empty_directories_and_small_files_by_default(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = os.path.join(temp_dir, "data")
            empty_dir = os.path.join(data_root, "empty")
            small_file = os.path.join(data_root, "small.txt")
            large_file = os.path.join(data_root, "large.bin")
            os.makedirs(empty_dir, exist_ok=True)
            os.makedirs(data_root, exist_ok=True)
            with open(small_file, "wb") as handle:
                handle.write(b"x" * 1024)
            with open(large_file, "wb") as handle:
                handle.write(b"x" * (101 * 1024))

            orphan_rows = report.collect_orphans(data_root, [], set())
            orphan_paths = {row["resolved_path"] for row in orphan_rows}

            self.assertIn(large_file, orphan_paths)
            self.assertNotIn(small_file, orphan_paths)
            self.assertNotIn(empty_dir, orphan_paths)

    def test_orphan_small_file_threshold_is_configurable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            data_root = os.path.join(temp_dir, "data")
            os.makedirs(data_root, exist_ok=True)
            small_file = os.path.join(data_root, "small.txt")
            with open(small_file, "wb") as handle:
                handle.write(b"x" * 1024)

            default_rows = report.collect_orphans(data_root, [], set())
            permissive_rows = report.collect_orphans(
                data_root,
                [],
                set(),
                small_file_threshold_bytes=512,
            )

            self.assertNotIn(small_file, {row["resolved_path"] for row in default_rows})
            self.assertIn(small_file, {row["resolved_path"] for row in permissive_rows})

    def test_resolve_store_path_maps_windows_external_root_to_local_mount(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            external_root = os.path.join(temp_dir, "ext_osm")
            os.makedirs(os.path.join(external_root, "roads"), exist_ok=True)
            mappings = report.parse_external_path_mappings(
                json.dumps({r"C:\data\osm": external_root})
            )

            resolved = report.resolve_store_path(
                r"C:\data\osm\roads\roads.tif",
                temp_dir,
                external_path_mappings=mappings,
            )

            self.assertEqual(
                resolved.resolved_path,
                os.path.join(external_root, "roads", "roads.tif"),
            )
            self.assertIsNotNone(resolved.mapping)

    def test_resolve_store_path_maps_unc_external_root_to_local_mount(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            external_root = os.path.join(temp_dir, "share_mount")
            mappings = report.parse_external_path_mappings(
                json.dumps({r"\\fileserver\gis": external_root})
            )

            resolved = report.resolve_store_path(
                r"\\fileserver\gis\imagery\demo.tif",
                temp_dir,
                external_path_mappings=mappings,
            )

            self.assertEqual(
                resolved.resolved_path,
                os.path.join(external_root, "imagery", "demo.tif"),
            )
            self.assertIsNotNone(resolved.mapping)

    def test_resolve_store_path_maps_posix_external_root_to_local_mount(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            external_root = os.path.join(temp_dir, "posix_mount")
            mappings = report.parse_external_path_mappings(
                json.dumps({"/srv/geodata": external_root})
            )

            resolved = report.resolve_store_path(
                "/srv/geodata/vector/roads.gpkg",
                temp_dir,
                external_path_mappings=mappings,
            )

            self.assertEqual(
                resolved.resolved_path,
                os.path.join(external_root, "vector", "roads.gpkg"),
            )
            self.assertIsNotNone(resolved.mapping)

    def test_resolve_store_path_prefers_longest_external_mapping_prefix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_mount = os.path.join(temp_dir, "base")
            nested_mount = os.path.join(temp_dir, "nested")
            mappings = report.parse_external_path_mappings(
                json.dumps(
                    {
                        r"C:\data": base_mount,
                        r"C:\data\osm": nested_mount,
                    }
                )
            )

            resolved = report.resolve_store_path(
                r"C:\data\osm\roads\roads.tif",
                temp_dir,
                external_path_mappings=mappings,
            )

            self.assertEqual(
                resolved.resolved_path,
                os.path.join(nested_mount, "roads", "roads.tif"),
            )

    def test_resolve_store_path_keeps_data_relative_paths_internal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            external_root = os.path.join(temp_dir, "ext_osm")
            mappings = report.parse_external_path_mappings(
                json.dumps({r"C:\data\osm": external_root})
            )

            resolved = report.resolve_store_path(
                "file:data/ws/store.tif",
                temp_dir,
                external_path_mappings=mappings,
            )

            self.assertEqual(
                resolved.resolved_path,
                os.path.join(temp_dir, "data", "ws", "store.tif"),
            )
            self.assertIsNone(resolved.mapping)

    def test_inventory_uses_external_path_mapping_for_scan_success(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            external_root = os.path.join(temp_dir, "ext_osm")
            os.makedirs(external_root, exist_ok=True)
            tif_path = os.path.join(external_root, "roads.tif")
            with open(tif_path, "wb") as handle:
                handle.write(b"mapped")

            mappings = report.parse_external_path_mappings(
                json.dumps({r"C:\data\osm": external_root})
            )
            def fake_list_store_refs(_client, _workspace, store_kind):
                return ["roads"] if store_kind == "coveragestores" else []

            with patch.object(report, "list_workspaces", return_value=["ws"]), patch.object(
                report,
                "list_store_refs",
                side_effect=fake_list_store_refs,
            ), patch.object(
                report,
                "get_store_detail",
                return_value={"type": "GeoTIFF", "url": r"C:\data\osm\roads.tif"},
            ), patch.object(
                report,
                "list_store_layers",
                return_value=["roads"],
            ):
                rows, referenced_roots, referenced_files = report.inventory_stores(
                    client=None,
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                    external_path_mappings=mappings,
                )

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "ok")
            self.assertEqual(rows[0]["resolved_path"], tif_path)
            self.assertFalse(referenced_roots)
            self.assertIn(report.normalize_path(tif_path), referenced_files)

    def test_inventory_uses_two_distinct_external_path_mappings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            raster_root = os.path.join(temp_dir, "external_raster")
            vector_root = os.path.join(temp_dir, "external_vector")
            os.makedirs(raster_root, exist_ok=True)
            os.makedirs(vector_root, exist_ok=True)
            raster_path = os.path.join(raster_root, "roads.tif")
            vector_path = os.path.join(vector_root, "parcels.gpkg")
            with open(raster_path, "wb") as handle:
                handle.write(b"raster")
            with open(vector_path, "wb") as handle:
                handle.write(b"vector")

            mappings = report.parse_external_path_mappings(
                json.dumps(
                    {
                        r"C:\external\raster": raster_root,
                        r"D:\external\vector": vector_root,
                    }
                )
            )

            def fake_list_store_refs(_client, _workspace, store_kind):
                if store_kind == "coveragestores":
                    return ["roads"]
                if store_kind == "datastores":
                    return ["parcels"]
                return []

            def fake_get_store_detail(_client, _workspace, store_kind, store_name):
                if store_kind == "coveragestores" and store_name == "roads":
                    return {"type": "GeoTIFF", "url": r"C:\external\raster\roads.tif"}
                if store_kind == "datastores" and store_name == "parcels":
                    return {
                        "type": "GeoPackage",
                        "connectionParameters": {
                            "entry": [
                                {"@key": "database", "$": r"D:\external\vector\parcels.gpkg"}
                            ]
                        },
                    }
                raise AssertionError((store_kind, store_name))

            def fake_list_store_layers(_client, _workspace, _store_kind, store_name):
                return [store_name]

            with patch.object(report, "list_workspaces", return_value=["ws"]), patch.object(
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
                side_effect=fake_list_store_layers,
            ):
                rows, referenced_roots, referenced_files = report.inventory_stores(
                    client=None,
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                    external_path_mappings=mappings,
                )

            rows_by_store = {row["store_name"]: row for row in rows}
            self.assertEqual(rows_by_store["roads"]["status"], "ok")
            self.assertEqual(rows_by_store["roads"]["resolved_path"], raster_path)
            self.assertEqual(rows_by_store["parcels"]["status"], "ok")
            self.assertEqual(rows_by_store["parcels"]["resolved_path"], vector_path)
            self.assertFalse(referenced_roots)
            self.assertIn(report.normalize_path(raster_path), referenced_files)
            self.assertIn(report.normalize_path(vector_path), referenced_files)

    def test_inventory_marks_mapped_external_root_as_missing_when_inaccessible(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            inaccessible_root = os.path.join(temp_dir, "missing_mount")
            mappings = report.parse_external_path_mappings(
                json.dumps({r"C:\data\osm": inaccessible_root})
            )
            def fake_list_store_refs(_client, _workspace, store_kind):
                return ["roads"] if store_kind == "coveragestores" else []

            with patch.object(report, "list_workspaces", return_value=["ws"]), patch.object(
                report,
                "list_store_refs",
                side_effect=fake_list_store_refs,
            ), patch.object(
                report,
                "get_store_detail",
                return_value={"type": "GeoTIFF", "url": r"C:\data\osm\roads.tif"},
            ), patch.object(
                report,
                "list_store_layers",
                return_value=["roads"],
            ):
                rows, _referenced_roots, _referenced_files = report.inventory_stores(
                    client=None,
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                    external_path_mappings=mappings,
                )

            self.assertEqual(rows[0]["status"], "missing")
            self.assertIn("is not accessible from the current runtime", rows[0]["notes"])
            self.assertIn(inaccessible_root, rows[0]["notes"])

    def test_inventory_keeps_unmapped_external_store_missing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            def fake_list_store_refs(_client, _workspace, store_kind):
                return ["roads"] if store_kind == "coveragestores" else []

            with patch.object(report, "list_workspaces", return_value=["ws"]), patch.object(
                report,
                "list_store_refs",
                side_effect=fake_list_store_refs,
            ), patch.object(
                report,
                "get_store_detail",
                return_value={"type": "GeoTIFF", "url": r"C:\data\osm\roads.tif"},
            ), patch.object(
                report,
                "list_store_layers",
                return_value=["roads"],
            ):
                rows, _referenced_roots, _referenced_files = report.inventory_stores(
                    client=None,
                    data_dir=temp_dir,
                    excluded_workspaces=set(),
                )

            self.assertEqual(rows[0]["status"], "missing")
            self.assertEqual(rows[0]["notes"], "Resolved path does not exist on disk.")

    def test_external_mapping_demo_fixture_exercises_internal_and_mapped_external_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = os.path.abspath(temp_dir)
            prepared = mapping_demo.prepare_fixture(Path(base_dir))
            mappings = report.parse_external_path_mappings(
                json.dumps(prepared["local_external_path_mappings"])
            )

            rows, referenced_roots, referenced_files = report.inventory_stores(
                client=None,
                data_dir=str(prepared["data_dir"]),
                excluded_workspaces=set(),
                catalog_source="filesystem",
                workers=2,
                external_path_mappings=mappings,
            )
            rows_by_store = {row["store_name"]: row for row in rows}

            internal_path = os.path.join(
                str(prepared["data_dir"]),
                "data",
                mapping_demo.WORKSPACE,
                "internal",
                "internal.tif",
            )
            windows_path = os.path.join(
                base_dir,
                "geoserver_test",
                "external_data",
                "windows",
                "mapped_windows.tif",
            )
            posix_path = os.path.join(
                base_dir,
                "geoserver_test",
                "external_data",
                "posix",
                "mapped_posix.tif",
            )

            self.assertEqual(rows_by_store[mapping_demo.INTERNAL_STORE]["status"], "ok")
            self.assertEqual(rows_by_store[mapping_demo.INTERNAL_STORE]["resolved_path"], internal_path)
            self.assertEqual(rows_by_store[mapping_demo.WINDOWS_STORE]["status"], "ok")
            self.assertEqual(rows_by_store[mapping_demo.WINDOWS_STORE]["resolved_path"], windows_path)
            self.assertEqual(rows_by_store[mapping_demo.POSIX_STORE]["status"], "ok")
            self.assertEqual(rows_by_store[mapping_demo.POSIX_STORE]["resolved_path"], posix_path)
            self.assertEqual(rows_by_store[mapping_demo.MISSING_STORE]["status"], "missing")
            self.assertIn("Mapped external root", rows_by_store[mapping_demo.MISSING_STORE]["notes"])

            self.assertFalse(referenced_roots)
            self.assertIn(report.normalize_path(internal_path), referenced_files)
            self.assertIn(report.normalize_path(windows_path), referenced_files)
            self.assertIn(report.normalize_path(posix_path), referenced_files)
            orphan_rows = report.collect_orphans(
                os.path.join(str(prepared["data_dir"]), "data"),
                referenced_roots,
                referenced_files,
            )
            self.assertFalse(
                any("external_data" in row["resolved_path"] for row in orphan_rows),
                "external mapped data must not be included in internal orphan detection",
            )

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
            report_render.write_html_report(
                output_html,
                rows,
                ["skip_me"],
                "http://localhost:8081/geoserver",
                temp_dir,
            )
            with open(output_html, "r", encoding="utf-8") as handle:
                html_text = handle.read()

        self.assertIn("GeoServer Cleaner Report", html_text)
        self.assertIn('id="reportTable"', html_text)
        self.assertIn('class="sortable"', html_text)
        self.assertIn('id="pageSize"', html_text)
        self.assertIn('id="reportRows"', html_text)
        self.assertIn("<tbody></tbody>", html_text)
        self.assertIn("skip_me", html_text)

    def test_reporting_cli_uses_app_native_default_output_name(self):
        args = report_cli.parse_args([])
        self.assertTrue(args.output_csv.endswith("geoserver_cleaner_report.csv"))

    def test_reporting_cli_generates_csv_html_and_summary(self):
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

            output_csv = os.path.join(temp_dir, "geoserver_cleaner_report.csv")
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                exit_code = report_cli.main(
                    [
                        "--data-dir",
                        temp_dir,
                        "--catalog-source",
                        "filesystem",
                        "--output-csv",
                        output_csv,
                    ]
                )

            self.assertEqual(exit_code, 0)
            self.assertIn("Wrote 1 store rows and 0 orphan rows", buffer.getvalue())
            self.assertTrue(os.path.isfile(output_csv))
            self.assertTrue(os.path.isfile(os.path.join(temp_dir, "geoserver_cleaner_report.html")))


if __name__ == "__main__":
    unittest.main()
