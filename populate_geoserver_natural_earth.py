#!/usr/bin/env python
"""
Download Natural Earth sample data, place it under the mounted GeoServer data
directory, and register it in GeoServer via REST.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin
from urllib.request import Request, urlopen


COUNTRIES_URL = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip"
POPULATED_PLACES_URL = "https://naciscdn.org/naturalearth/10m/cultural/ne_10m_populated_places.zip"
GRAY_WORLD_URL = "https://naciscdn.org/naturalearth/50m/raster/GRAY_50M_SR_W.zip"

DEFAULT_QGIS_PYTHON = r"C:\Program Files\QGIS 3.44.5\bin\python-qgis.bat"
DEFAULT_GDAL_TRANSLATE = r"C:\Program Files\QGIS 3.44.5\bin\gdal_translate.exe"


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def reset_directory(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink()


def download(url: str, destination: Path) -> None:
    if destination.exists() and destination.stat().st_size > 0:
        print(f"Using cached download: {destination}")
        return
    print(f"Downloading {url}")
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=120) as response:
        destination.write_bytes(response.read())


def extract_zip(zip_path: Path, destination: Path) -> None:
    reset_directory(destination)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(destination)


def zip_directory(source_dir: Path, destination_zip: Path, pattern: str = "*") -> None:
    if destination_zip.exists():
        destination_zip.unlink()
    with zipfile.ZipFile(destination_zip, "w", zipfile.ZIP_DEFLATED) as archive:
        for item in sorted(source_dir.glob(pattern)):
            archive.write(item, arcname=item.name)


def run_process(
    args: list[str],
    input_text: Optional[str] = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            "Command failed ({}):\nSTDOUT:\n{}\nSTDERR:\n{}".format(
                " ".join(args),
                result.stdout,
                result.stderr,
            )
        )
    return result


def create_geopackage(
    qgis_python: Path,
    geopackage_path: Path,
    countries_shp: Path,
    populated_places_shp: Path,
) -> None:
    script = f"""
from osgeo import ogr
from pathlib import Path
ogr.UseExceptions()
out = Path(r"{geopackage_path}")
drv = ogr.GetDriverByName("GPKG")
if out.exists():
    drv.DeleteDataSource(str(out))
out_ds = drv.CreateDataSource(str(out))
if out_ds is None:
    raise RuntimeError("Failed to create GeoPackage")
for src_path, layer_name in [
    (r"{countries_shp}", "countries"),
    (r"{populated_places_shp}", "populated_places"),
]:
    src = ogr.Open(src_path)
    if src is None:
        raise RuntimeError(f"Failed to open {{src_path}}")
    layer = src.GetLayer(0)
    copied = out_ds.CopyLayer(layer, layer_name)
    if copied is None:
        raise RuntimeError(f"Failed to copy layer {{layer_name}}")
    print("copied", layer_name)
out_ds = None
print("created", out)
"""
    result = run_process([str(qgis_python), "-"], input_text=script)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())


def create_mosaic_tiles(
    gdal_translate: Path,
    source_tif: Path,
    destination_dir: Path,
) -> None:
    reset_directory(destination_dir)
    tiles = [
        ("gray_world_ul.tif", 0, 0, 5400, 2700),
        ("gray_world_ur.tif", 5400, 0, 5400, 2700),
        ("gray_world_ll.tif", 0, 2700, 5400, 2700),
        ("gray_world_lr.tif", 5400, 2700, 5400, 2700),
    ]
    for name, xoff, yoff, xsize, ysize in tiles:
        result = run_process(
            [
                str(gdal_translate),
                "-srcwin",
                str(xoff),
                str(yoff),
                str(xsize),
                str(ysize),
                str(source_tif),
                str(destination_dir / name),
            ]
        )
        if result.stdout.strip():
            print(result.stdout.strip())
        if result.stderr.strip():
            print(result.stderr.strip())


class GeoServerRest:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"

    def request(
        self,
        method: str,
        rest_path: str,
        data: Optional[bytes] = None,
        content_type: Optional[str] = None,
        accept: str = "application/json",
        expected: tuple[int, ...] = (200, 201),
    ) -> bytes:
        url = urljoin(self.base_url, rest_path.lstrip("/"))
        headers = {"Authorization": self.auth_header, "Accept": accept}
        if content_type:
            headers["Content-Type"] = content_type
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=120) as response:
                status = response.getcode()
                body = response.read()
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code in expected:
                return body.encode("utf-8")
            raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}") from exc
        except URLError as exc:
            raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc

        if status not in expected:
            raise RuntimeError(f"{method} {url} returned unexpected HTTP {status}")
        return body

    def wait_until_ready(self, timeout_seconds: int = 300) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                self.request(
                    "GET",
                    "rest/about/version.json",
                    accept="application/json",
                    expected=(200,),
                )
                print("GeoServer REST is ready.")
                return
            except Exception:
                time.sleep(5)
        raise RuntimeError("Timed out waiting for GeoServer REST to become ready.")

    def workspace_exists(self, name: str) -> bool:
        try:
            self.request("GET", f"rest/workspaces/{quote(name, safe='')}.json", expected=(200,))
            return True
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                return False
            raise

    def ensure_workspace(self, name: str) -> None:
        if self.workspace_exists(name):
            print(f"Workspace already exists: {name}")
            return
        payload = json.dumps({"workspace": {"name": name, "uri": f"http://{name}"}}).encode("utf-8")
        self.request(
            "POST",
            "rest/workspaces",
            data=payload,
            content_type="application/json",
            expected=(201,),
        )
        print(f"Created workspace: {name}")

    def delete_store_if_exists(
        self,
        workspace: str,
        store: str,
        store_kind: str,
        purge_all: bool = False,
    ) -> None:
        path = f"rest/workspaces/{quote(workspace, safe='')}/{store_kind}/{quote(store, safe='')}.json"
        try:
            self.request("GET", path, expected=(200,))
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                return
            raise

        suffix = "?recurse=true"
        if store_kind == "coveragestores" and purge_all:
            suffix += "&purge=all"
        self.request("DELETE", path + suffix, expected=(200, 202))
        print(f"Deleted existing {store_kind[:-1]}: {store}")

    def create_shapefile_store(
        self,
        workspace: str,
        store: str,
        relative_path: str,
        namespace_uri: str,
        feature_name: str,
    ) -> None:
        store_path = f"rest/workspaces/{quote(workspace, safe='')}/datastores"
        store_payload = {
            "dataStore": {
                "name": store,
                "type": "Shapefile",
                "enabled": True,
                "connectionParameters": {
                    "entry": [
                        {"@key": "url", "$": relative_path},
                        {"@key": "namespace", "$": namespace_uri},
                    ]
                },
            }
        }
        self.request(
            "POST",
            store_path,
            data=json.dumps(store_payload).encode("utf-8"),
            content_type="application/json",
            expected=(201,),
        )

        feature_path = (
            f"rest/workspaces/{quote(workspace, safe='')}/datastores/{quote(store, safe='')}/featuretypes"
        )
        feature_payload = {
            "featureType": {
                "name": feature_name,
                "nativeName": feature_name,
                "title": "Natural Earth Admin 0 Countries",
                "srs": "EPSG:4326",
            }
        }
        self.request(
            "POST",
            feature_path,
            data=json.dumps(feature_payload).encode("utf-8"),
            content_type="application/json",
            expected=(201,),
        )
        print(f"Published shapefile store: {store}")

    def create_geotiff_store(
        self,
        workspace: str,
        store: str,
        relative_path: str,
        coverage_name: str,
        native_name: str,
    ) -> None:
        store_path = f"rest/workspaces/{quote(workspace, safe='')}/coveragestores"
        store_payload = {
            "coverageStore": {
                "name": store,
                "type": "GeoTIFF",
                "enabled": True,
                "url": relative_path,
                "workspace": {"name": workspace},
            }
        }
        self.request(
            "POST",
            store_path,
            data=json.dumps(store_payload).encode("utf-8"),
            content_type="application/json",
            expected=(201,),
        )

        coverage_path = (
            f"rest/workspaces/{quote(workspace, safe='')}/coveragestores/{quote(store, safe='')}/coverages"
        )
        coverage_payload = {
            "coverage": {
                "name": coverage_name,
                "nativeName": native_name,
                "title": "Natural Earth Gray World",
                "srs": "EPSG:4326",
            }
        }
        self.request(
            "POST",
            coverage_path,
            data=json.dumps(coverage_payload).encode("utf-8"),
            content_type="application/json",
            expected=(201,),
        )
        print(f"Published GeoTIFF store: {store}")

    def create_geopackage_store(
        self,
        workspace: str,
        store: str,
        relative_path: str,
        namespace_uri: str,
        layer_names: list[str],
    ) -> None:
        store_path = f"rest/workspaces/{quote(workspace, safe='')}/datastores"
        store_payload = {
            "dataStore": {
                "name": store,
                "type": "GeoPackage",
                "enabled": True,
                "connectionParameters": {
                    "entry": [
                        {"@key": "database", "$": relative_path},
                        {"@key": "dbtype", "$": "geopkg"},
                        {"@key": "namespace", "$": namespace_uri},
                    ]
                },
            }
        }
        self.request(
            "POST",
            store_path,
            data=json.dumps(store_payload).encode("utf-8"),
            content_type="application/json",
            expected=(201,),
        )

        feature_path = (
            f"rest/workspaces/{quote(workspace, safe='')}/datastores/{quote(store, safe='')}/featuretypes"
        )
        for layer_name in layer_names:
            feature_payload = {
                "featureType": {
                    "name": layer_name,
                    "nativeName": layer_name,
                    "title": layer_name.replace("_", " ").title(),
                    "srs": "EPSG:4326",
                }
            }
            self.request(
                "POST",
                feature_path,
                data=json.dumps(feature_payload).encode("utf-8"),
                content_type="application/json",
                expected=(201,),
            )
        print(f"Published GeoPackage store: {store}")

    def upload_imagemosaic_store(
        self,
        workspace: str,
        store: str,
        mosaic_zip_path: Path,
    ) -> None:
        path = (
            f"rest/workspaces/{quote(workspace, safe='')}/coveragestores/"
            f"{quote(store, safe='')}/file.imagemosaic?configure=all"
        )
        self.request(
            "PUT",
            path,
            data=mosaic_zip_path.read_bytes(),
            content_type="application/zip",
            expected=(200, 201),
        )
        print(f"Published ImageMosaic store: {store}")


def create_orphan_test_data(data_root: Path) -> None:
    orphan_dir = data_root / "orphaned_demo"
    ensure_directory(orphan_dir)
    (orphan_dir / "README.txt").write_text(
        "This file is intentionally not registered in GeoServer.\n",
        encoding="utf-8",
    )
    print(f"Created orphan data for testing: {orphan_dir}")


def build_paths(base_dir: Path) -> dict:
    data_dir = base_dir / "docker" / "geoserver_data"
    downloads_dir = base_dir / "docker" / "downloads"
    natural_earth_root = data_dir / "data" / "naturalearth"
    return {
        "data_dir": data_dir,
        "data_root": data_dir / "data",
        "downloads_dir": downloads_dir,
        "vector_dir": natural_earth_root / "vector",
        "raster_dir": natural_earth_root / "raster",
        "populated_extract_dir": downloads_dir / "populated_places_extract",
        "mosaic_stage_dir": downloads_dir / "mosaic_gray_world_upload",
        "mosaic_zip": downloads_dir / "mosaic_gray_world_upload.zip",
    }


def prepare_data(base_dir: Path, qgis_python: Path, gdal_translate: Path) -> dict:
    paths = build_paths(base_dir)
    for key in ("data_root", "downloads_dir", "vector_dir", "raster_dir"):
        ensure_directory(paths[key])

    remove_path(paths["data_root"] / "naturalearth" / "vector_populated_places")
    remove_path(paths["data_root"] / "naturalearth" / "mosaic_gray_world")
    remove_path(paths["data_root"] / "naturalearth" / "mosaic_gray_world_upload")

    countries_zip = paths["downloads_dir"] / "ne_10m_admin_0_countries.zip"
    populated_places_zip = paths["downloads_dir"] / "ne_10m_populated_places.zip"
    gray_world_zip = paths["downloads_dir"] / "GRAY_50M_SR_W.zip"

    download(COUNTRIES_URL, countries_zip)
    download(POPULATED_PLACES_URL, populated_places_zip)
    download(GRAY_WORLD_URL, gray_world_zip)

    extract_zip(countries_zip, paths["vector_dir"])
    extract_zip(gray_world_zip, paths["raster_dir"])
    extract_zip(populated_places_zip, paths["populated_extract_dir"])

    countries_shp = paths["vector_dir"] / "ne_10m_admin_0_countries.shp"
    populated_places_shp = paths["populated_extract_dir"] / "ne_10m_populated_places.shp"
    geopackage_path = paths["vector_dir"] / "naturalearth_multi.gpkg"
    create_geopackage(qgis_python, geopackage_path, countries_shp, populated_places_shp)

    source_tif = paths["raster_dir"] / "GRAY_50M_SR_W.tif"
    create_mosaic_tiles(gdal_translate, source_tif, paths["mosaic_stage_dir"])
    zip_directory(paths["mosaic_stage_dir"], paths["mosaic_zip"], "*.tif")

    create_orphan_test_data(paths["data_root"])
    return paths


def path_to_geoserver_relative(data_dir: Path, target: Path) -> str:
    relative = target.relative_to(data_dir).as_posix()
    return f"file:{relative}"


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Populate a local GeoServer test instance with Natural Earth data.",
    )
    parser.add_argument(
        "--base-dir",
        default=os.getcwd(),
        help="Project base directory that contains docker-compose.geoserver-test.yml",
    )
    parser.add_argument(
        "--geoserver-url",
        default="http://localhost:8081/geoserver",
        help="GeoServer base URL",
    )
    parser.add_argument(
        "--username",
        default="admin",
        help="GeoServer admin username",
    )
    parser.add_argument(
        "--password",
        default="geoserver",
        help="GeoServer admin password",
    )
    parser.add_argument(
        "--workspace",
        default="naturalearth",
        help="Workspace name to create or reuse",
    )
    parser.add_argument(
        "--qgis-python",
        default=DEFAULT_QGIS_PYTHON,
        help="Path to python-qgis.bat",
    )
    parser.add_argument(
        "--gdal-translate",
        default=DEFAULT_GDAL_TRANSLATE,
        help="Path to gdal_translate.exe",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    base_dir = Path(args.base_dir).resolve()
    qgis_python = Path(args.qgis_python)
    gdal_translate = Path(args.gdal_translate)
    if not qgis_python.exists():
        raise SystemExit(f"QGIS Python executable not found: {qgis_python}")
    if not gdal_translate.exists():
        raise SystemExit(f"GDAL translate executable not found: {gdal_translate}")

    paths = prepare_data(base_dir, qgis_python, gdal_translate)
    client = GeoServerRest(args.geoserver_url, args.username, args.password)
    client.wait_until_ready()
    client.ensure_workspace(args.workspace)

    client.delete_store_if_exists(args.workspace, "ne_admin0_countries", "datastores")
    client.delete_store_if_exists(args.workspace, "ne_vector_multi", "datastores")
    client.delete_store_if_exists(args.workspace, "ne_gray_world", "coveragestores", purge_all=True)
    client.delete_store_if_exists(args.workspace, "ne_gray_world_mosaic", "coveragestores")
    client.delete_store_if_exists(args.workspace, "ne_gray_world_mosaic_upload", "coveragestores", purge_all=True)

    countries_shp = paths["vector_dir"] / "ne_10m_admin_0_countries.shp"
    gray_world_tif = paths["raster_dir"] / "GRAY_50M_SR_W.tif"
    geopackage_path = paths["vector_dir"] / "naturalearth_multi.gpkg"

    client.create_shapefile_store(
        workspace=args.workspace,
        store="ne_admin0_countries",
        feature_name=countries_shp.stem,
        relative_path=path_to_geoserver_relative(paths["data_dir"], countries_shp),
        namespace_uri=f"http://{args.workspace}",
    )
    client.create_geotiff_store(
        workspace=args.workspace,
        store="ne_gray_world",
        coverage_name="ne_gray_world",
        native_name=gray_world_tif.stem,
        relative_path=path_to_geoserver_relative(paths["data_dir"], gray_world_tif),
    )
    client.create_geopackage_store(
        workspace=args.workspace,
        store="ne_vector_multi",
        relative_path=path_to_geoserver_relative(paths["data_dir"], geopackage_path),
        namespace_uri=f"http://{args.workspace}",
        layer_names=["countries", "populated_places"],
    )
    client.upload_imagemosaic_store(
        workspace=args.workspace,
        store="ne_gray_world_mosaic_upload",
        mosaic_zip_path=paths["mosaic_zip"],
    )
    print("Natural Earth data population finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
