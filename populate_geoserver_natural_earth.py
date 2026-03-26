#!/usr/bin/env python
"""Populate a GeoServer test fixture with many Natural Earth and NASA stores."""

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
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin, urlsplit
from urllib.request import Request, urlopen


DEFAULT_QGIS_PYTHON = r"C:\Program Files\QGIS 3.44.5\bin\python-qgis.bat"
DEFAULT_GDAL_TRANSLATE = r"C:\Program Files\QGIS 3.44.5\bin\gdal_translate.exe"
NASA_GIBS_WMS = "https://gibs.earthdata.nasa.gov/wms/epsg4326/best/wms.cgi"


def ne_url(theme: str, name: str) -> str:
    return f"https://naciscdn.org/naturalearth/10m/{theme}/{name}.zip"


def nasa_wms_url(layer: str, width: int = 2048, height: int = 1024) -> str:
    params = {
        "SERVICE": "WMS",
        "REQUEST": "GetMap",
        "VERSION": "1.1.1",
        "LAYERS": layer,
        "STYLES": "",
        "SRS": "EPSG:4326",
        "BBOX": "-180,-90,180,90",
        "WIDTH": str(width),
        "HEIGHT": str(height),
        "FORMAT": "image/tiff",
    }
    return f"{NASA_GIBS_WMS}?{urlencode(params)}"


@dataclass(frozen=True)
class VectorStoreSpec:
    workspace: str
    store: str
    dataset_stem: str
    url: str
    title: str


@dataclass(frozen=True)
class GeopackageStoreSpec:
    workspace: str
    store: str
    title: str
    layers: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class RasterStoreSpec:
    workspace: str
    store: str
    title: str
    file_name: str
    source_url: str
    archive_member: Optional[str] = None


@dataclass(frozen=True)
class MosaicStoreSpec:
    workspace: str
    store: str
    source_store: str
    prefix: str


VECTOR_SPECS = [
    VectorStoreSpec(*row)
    for row in [
        ("cultural", "ne_admin0_countries", "ne_10m_admin_0_countries", ne_url("cultural", "ne_10m_admin_0_countries"), "Natural Earth Admin 0 Countries"),
        ("cultural", "ne_populated_places", "ne_10m_populated_places", ne_url("cultural", "ne_10m_populated_places"), "Natural Earth Populated Places"),
        ("cultural", "ne_urban_areas", "ne_10m_urban_areas", ne_url("cultural", "ne_10m_urban_areas"), "Natural Earth Urban Areas"),
        ("cultural", "ne_airports", "ne_10m_airports", ne_url("cultural", "ne_10m_airports"), "Natural Earth Airports"),
        ("cultural", "ne_roads", "ne_10m_roads", ne_url("cultural", "ne_10m_roads"), "Natural Earth Roads"),
        ("politics", "ne_admin0_map_units", "ne_10m_admin_0_map_units", ne_url("cultural", "ne_10m_admin_0_map_units"), "Natural Earth Admin 0 Map Units"),
        ("politics", "ne_admin1_states_provinces", "ne_10m_admin_1_states_provinces", ne_url("cultural", "ne_10m_admin_1_states_provinces"), "Natural Earth Admin 1 States and Provinces"),
        ("politics", "ne_admin1_states_provinces_lines", "ne_10m_admin_1_states_provinces_lines", ne_url("cultural", "ne_10m_admin_1_states_provinces_lines"), "Natural Earth Admin 1 State and Province Lines"),
        ("politics", "ne_admin0_boundary_lines_land", "ne_10m_admin_0_boundary_lines_land", ne_url("cultural", "ne_10m_admin_0_boundary_lines_land"), "Natural Earth Admin 0 Boundary Lines Land"),
        ("politics", "ne_admin0_boundary_lines_disputed", "ne_10m_admin_0_boundary_lines_disputed_areas", ne_url("cultural", "ne_10m_admin_0_boundary_lines_disputed_areas"), "Natural Earth Disputed Boundary Areas"),
        ("physical", "ne_lakes", "ne_10m_lakes", ne_url("physical", "ne_10m_lakes"), "Natural Earth Lakes"),
        ("physical", "ne_rivers_centerlines", "ne_10m_rivers_lake_centerlines", ne_url("physical", "ne_10m_rivers_lake_centerlines"), "Natural Earth Rivers and Lake Centerlines"),
        ("physical", "ne_land", "ne_10m_land", ne_url("physical", "ne_10m_land"), "Natural Earth Land"),
        ("physical", "ne_ocean", "ne_10m_ocean", ne_url("physical", "ne_10m_ocean"), "Natural Earth Ocean"),
        ("physical", "ne_geography_regions", "ne_10m_geography_regions_polys", ne_url("physical", "ne_10m_geography_regions_polys"), "Natural Earth Geography Regions"),
    ]
]

GEOPACKAGE_SPECS = [
    GeopackageStoreSpec("cultural", "cultural_multi", "Natural Earth Cultural Multi Layer", (("countries", "ne_admin0_countries"), ("populated_places", "ne_populated_places"), ("urban_areas", "ne_urban_areas"))),
    GeopackageStoreSpec("politics", "politics_multi", "Natural Earth Politics Multi Layer", (("map_units", "ne_admin0_map_units"), ("states_provinces", "ne_admin1_states_provinces"), ("boundary_lines_land", "ne_admin0_boundary_lines_land"))),
    GeopackageStoreSpec("physical", "physical_multi", "Natural Earth Physical Multi Layer", (("lakes", "ne_lakes"), ("rivers_centerlines", "ne_rivers_centerlines"), ("land", "ne_land"))),
]

RASTER_SPECS = [
    RasterStoreSpec("raster", "ne_gray_world", "Natural Earth Gray World", "GRAY_50M_SR_W.tif", "https://naciscdn.org/naturalearth/50m/raster/GRAY_50M_SR_W.zip", "GRAY_50M_SR_W.tif"),
    RasterStoreSpec("raster", "nasa_blue_marble_bathymetry", "NASA Blue Marble Bathymetry", "nasa_blue_marble_bathymetry.tif", nasa_wms_url("BlueMarble_ShadedRelief_Bathymetry")),
    RasterStoreSpec("raster", "nasa_blue_marble_relief", "NASA Blue Marble Shaded Relief", "nasa_blue_marble_relief.tif", nasa_wms_url("BlueMarble_ShadedRelief")),
    RasterStoreSpec("raster", "nasa_aster_relief", "NASA ASTER GDEM Color Shaded Relief", "nasa_aster_relief.tif", nasa_wms_url("ASTER_GDEM_Color_Shaded_Relief")),
    RasterStoreSpec("raster", "nasa_city_lights", "NASA VIIRS City Lights 2012", "nasa_city_lights.tif", nasa_wms_url("VIIRS_CityLights_2012")),
]

MOSAIC_SPECS = [
    MosaicStoreSpec("raster", "ne_gray_world_mosaic", "ne_gray_world", "gray_world"),
    MosaicStoreSpec("raster", "nasa_blue_marble_bathymetry_mosaic", "nasa_blue_marble_bathymetry", "nasa_blue_marble_bathymetry"),
]

ALL_WORKSPACES = tuple(
    dict.fromkeys(
        [spec.workspace for spec in VECTOR_SPECS]
        + [spec.workspace for spec in GEOPACKAGE_SPECS]
        + [spec.workspace for spec in RASTER_SPECS]
        + [spec.workspace for spec in MOSAIC_SPECS]
    )
)
LEGACY_FIXTURE_WORKSPACES = ("naturalearth",)


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


def copy_file(source: Path, destination: Path) -> None:
    ensure_directory(destination.parent)
    shutil.copy2(source, destination)


def download(url: str, destination: Path) -> None:
    if destination.exists() and destination.stat().st_size > 0:
        print(f"Using cached download: {destination}")
        return
    print(f"Downloading {url}")
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=180) as response:
        destination.write_bytes(response.read())


def extract_zip(zip_path: Path, destination: Path) -> None:
    ensure_directory(destination)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(destination)


def zip_directory(source_dir: Path, destination_zip: Path, pattern: str = "*") -> None:
    if destination_zip.exists():
        destination_zip.unlink()
    with zipfile.ZipFile(destination_zip, "w", zipfile.ZIP_DEFLATED) as archive:
        for item in sorted(source_dir.glob(pattern)):
            archive.write(item, arcname=item.name)


def run_process(args: list[str], input_text: Optional[str] = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, input=input_text, text=True, capture_output=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(
            "Command failed ({}):\nSTDOUT:\n{}\nSTDERR:\n{}".format(" ".join(args), result.stdout, result.stderr)
        )
    return result


def create_geopackage(qgis_python: Path, geopackage_path: Path, layers: list[tuple[Path, str]]) -> None:
    layer_json = json.dumps([{"src": str(source), "layer_name": layer_name} for source, layer_name in layers])
    script = f"""
from osgeo import ogr
import json
from pathlib import Path
ogr.UseExceptions()
specs = json.loads({layer_json!r})
out = Path(r"{geopackage_path}")
drv = ogr.GetDriverByName("GPKG")
if out.exists():
    drv.DeleteDataSource(str(out))
out_ds = drv.CreateDataSource(str(out))
if out_ds is None:
    raise RuntimeError("Failed to create GeoPackage")
for spec in specs:
    src = ogr.Open(spec["src"])
    if src is None:
        raise RuntimeError(f'Failed to open {{spec["src"]}}')
    copied = out_ds.CopyLayer(src.GetLayer(0), spec["layer_name"])
    if copied is None:
        raise RuntimeError(f'Failed to copy layer {{spec["layer_name"]}}')
    print("copied", spec["layer_name"])
out_ds = None
print("created", out)
"""
    result = run_process([str(qgis_python), "-"], input_text=script)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip())


def get_raster_size(qgis_python: Path, raster_path: Path) -> tuple[int, int]:
    script = f"""
from osgeo import gdal
dataset = gdal.Open(r"{raster_path}")
if dataset is None:
    raise RuntimeError("Failed to open raster")
print(f"{{dataset.RasterXSize}} {{dataset.RasterYSize}}")
"""
    result = run_process([str(qgis_python), "-"], input_text=script)
    width, height = result.stdout.strip().split()
    return int(width), int(height)


def create_mosaic_tiles(qgis_python: Path, gdal_translate: Path, source_tif: Path, destination_dir: Path, prefix: str) -> None:
    reset_directory(destination_dir)
    width, height = get_raster_size(qgis_python, source_tif)
    half_width = width // 2
    half_height = height // 2
    tiles = [
        (f"{prefix}_ul.tif", 0, 0, half_width, half_height),
        (f"{prefix}_ur.tif", half_width, 0, width - half_width, half_height),
        (f"{prefix}_ll.tif", 0, half_height, half_width, height - half_height),
        (f"{prefix}_lr.tif", half_width, half_height, width - half_width, height - half_height),
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
            with urlopen(request, timeout=180) as response:
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
                self.request("GET", "rest/about/version.json", accept="application/json", expected=(200,))
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

    def delete_workspace_if_exists(self, name: str) -> None:
        if not self.workspace_exists(name):
            return
        self.request("DELETE", f"rest/workspaces/{quote(name, safe='')}.json?recurse=true", expected=(200, 202))
        print(f"Deleted existing workspace: {name}")

    def ensure_workspace(self, name: str) -> None:
        if self.workspace_exists(name):
            print(f"Workspace already exists: {name}")
            return
        payload = json.dumps({"workspace": {"name": name}}).encode("utf-8")
        self.request("POST", "rest/workspaces", data=payload, content_type="application/json", expected=(201,))
        print(f"Created workspace: {name}")

    def create_shapefile_store(
        self,
        workspace: str,
        store: str,
        relative_path: str,
        namespace_uri: str,
        feature_name: str,
        title: str,
    ) -> None:
        store_payload = {
            "dataStore": {
                "name": store,
                "type": "Shapefile",
                "enabled": True,
                "connectionParameters": {"entry": [{"@key": "url", "$": relative_path}, {"@key": "namespace", "$": namespace_uri}]},
            }
        }
        store_path = f"rest/workspaces/{quote(workspace, safe='')}/datastores"
        self.request("POST", store_path, data=json.dumps(store_payload).encode("utf-8"), content_type="application/json", expected=(201,))
        feature_payload = {"featureType": {"name": feature_name, "nativeName": feature_name, "title": title, "srs": "EPSG:4326"}}
        feature_path = f"rest/workspaces/{quote(workspace, safe='')}/datastores/{quote(store, safe='')}/featuretypes"
        self.request("POST", feature_path, data=json.dumps(feature_payload).encode("utf-8"), content_type="application/json", expected=(201,))
        print(f"Published shapefile store: {workspace}/{store}")

    def create_geopackage_store(
        self,
        workspace: str,
        store: str,
        relative_path: str,
        namespace_uri: str,
        layer_names: list[str],
        title: str,
    ) -> None:
        store_payload = {
            "dataStore": {
                "name": store,
                "type": "GeoPackage",
                "enabled": True,
                "connectionParameters": {"entry": [{"@key": "database", "$": relative_path}, {"@key": "dbtype", "$": "geopkg"}, {"@key": "namespace", "$": namespace_uri}]},
            }
        }
        store_path = f"rest/workspaces/{quote(workspace, safe='')}/datastores"
        self.request("POST", store_path, data=json.dumps(store_payload).encode("utf-8"), content_type="application/json", expected=(201,))
        feature_path = f"rest/workspaces/{quote(workspace, safe='')}/datastores/{quote(store, safe='')}/featuretypes"
        for layer_name in layer_names:
            feature_payload = {
                "featureType": {
                    "name": layer_name,
                    "nativeName": layer_name,
                    "title": f"{title} {layer_name.replace('_', ' ').title()}",
                    "srs": "EPSG:4326",
                }
            }
            self.request("POST", feature_path, data=json.dumps(feature_payload).encode("utf-8"), content_type="application/json", expected=(201,))
        print(f"Published GeoPackage store: {workspace}/{store}")

    def create_geotiff_store(
        self,
        workspace: str,
        store: str,
        relative_path: str,
        coverage_name: str,
        native_name: str,
        title: str,
    ) -> None:
        store_payload = {"coverageStore": {"name": store, "type": "GeoTIFF", "enabled": True, "url": relative_path, "workspace": {"name": workspace}}}
        store_path = f"rest/workspaces/{quote(workspace, safe='')}/coveragestores"
        self.request("POST", store_path, data=json.dumps(store_payload).encode("utf-8"), content_type="application/json", expected=(201,))
        coverage_payload = {"coverage": {"name": coverage_name, "nativeName": native_name, "title": title, "srs": "EPSG:4326"}}
        coverage_path = f"rest/workspaces/{quote(workspace, safe='')}/coveragestores/{quote(store, safe='')}/coverages"
        self.request("POST", coverage_path, data=json.dumps(coverage_payload).encode("utf-8"), content_type="application/json", expected=(201,))
        print(f"Published GeoTIFF store: {workspace}/{store}")

    def upload_imagemosaic_store(self, workspace: str, store: str, mosaic_zip_path: Path) -> None:
        path = f"rest/workspaces/{quote(workspace, safe='')}/coveragestores/{quote(store, safe='')}/file.imagemosaic?configure=all"
        self.request("PUT", path, data=mosaic_zip_path.read_bytes(), content_type="application/zip", expected=(200, 201))
        print(f"Published ImageMosaic store: {workspace}/{store}")


def build_paths(base_dir: Path, workspaces: list[str]) -> dict:
    data_dir = base_dir / "docker" / "geoserver_data"
    downloads_dir = base_dir / "docker" / "downloads"
    workspace_dirs = {}
    for workspace in workspaces:
        root = data_dir / "data" / workspace
        workspace_dirs[workspace] = {"root": root, "vector": root / "vector", "gpkg": root / "gpkg", "raster": root / "raster"}
    return {"data_dir": data_dir, "data_root": data_dir / "data", "downloads_dir": downloads_dir, "workspace_dirs": workspace_dirs, "mosaic_dir": downloads_dir / "mosaic"}


def create_orphan_test_data(data_root: Path) -> None:
    orphan_dir = data_root / "orphaned_demo"
    remove_path(orphan_dir)
    ensure_directory(orphan_dir)
    (orphan_dir / "README.txt").write_text("This file is intentionally not registered in GeoServer.\n", encoding="utf-8")
    print(f"Created orphan data for testing: {orphan_dir}")


def parse_workspace_selection(raw: str) -> list[str]:
    if not raw.strip():
        return list(ALL_WORKSPACES)
    selected = []
    for value in raw.split(","):
        name = value.strip().lower()
        if not name:
            continue
        if name not in ALL_WORKSPACES:
            raise SystemExit("Unknown workspace {!r}. Choose from: {}".format(name, ", ".join(ALL_WORKSPACES)))
        if name not in selected:
            selected.append(name)
    if not selected:
        raise SystemExit("At least one workspace must be selected.")
    return selected


def prepare_data(base_dir: Path, qgis_python: Path, gdal_translate: Path, selected_workspaces: list[str]) -> dict:
    paths = build_paths(base_dir, selected_workspaces)
    ensure_directory(paths["data_root"])
    ensure_directory(paths["downloads_dir"])
    reset_directory(paths["mosaic_dir"])
    for legacy_workspace in LEGACY_FIXTURE_WORKSPACES:
        remove_path(paths["data_root"] / legacy_workspace)
    for workspace in selected_workspaces:
        workspace_paths = paths["workspace_dirs"][workspace]
        reset_directory(workspace_paths["root"])

    vector_paths: Dict[str, Path] = {}
    raster_paths: Dict[str, Path] = {}
    geopackage_paths: Dict[str, Path] = {}
    mosaic_zip_paths: Dict[str, Path] = {}

    for spec in VECTOR_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        zip_path = paths["downloads_dir"] / Path(urlsplit(spec.url).path).name
        workspace_vector_dir = paths["workspace_dirs"][spec.workspace]["vector"]
        download(spec.url, zip_path)
        extract_zip(zip_path, workspace_vector_dir)
        shp_path = workspace_vector_dir / f"{spec.dataset_stem}.shp"
        if not shp_path.exists():
            raise RuntimeError(f"Expected shapefile was not extracted: {shp_path}")
        vector_paths[spec.store] = shp_path

    for spec in GEOPACKAGE_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        geopackage_path = paths["workspace_dirs"][spec.workspace]["gpkg"] / f"{spec.store}.gpkg"
        ensure_directory(geopackage_path.parent)
        create_geopackage(qgis_python, geopackage_path, [(vector_paths[source_store], layer_name) for layer_name, source_store in spec.layers])
        geopackage_paths[spec.store] = geopackage_path

    for spec in RASTER_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        workspace_raster_dir = paths["workspace_dirs"][spec.workspace]["raster"]
        cache_name = spec.file_name if spec.archive_member is None else Path(urlsplit(spec.source_url).path).name
        cache_path = paths["downloads_dir"] / cache_name
        destination_path = workspace_raster_dir / spec.file_name
        download(spec.source_url, cache_path)
        if spec.archive_member:
            extract_zip(cache_path, workspace_raster_dir)
        else:
            copy_file(cache_path, destination_path)
        if not destination_path.exists():
            raise RuntimeError(f"Expected raster file was not prepared: {destination_path}")
        raster_paths[spec.store] = destination_path

    for spec in MOSAIC_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        stage_dir = paths["mosaic_dir"] / spec.store
        zip_path = paths["mosaic_dir"] / f"{spec.store}.zip"
        create_mosaic_tiles(qgis_python, gdal_translate, raster_paths[spec.source_store], stage_dir, spec.prefix)
        zip_directory(stage_dir, zip_path, "*.tif")
        mosaic_zip_paths[spec.store] = zip_path

    create_orphan_test_data(paths["data_root"])
    return {"paths": paths, "vector_paths": vector_paths, "raster_paths": raster_paths, "geopackage_paths": geopackage_paths, "mosaic_zip_paths": mosaic_zip_paths}


def path_to_geoserver_relative(data_dir: Path, target: Path) -> str:
    return "file:" + target.relative_to(data_dir).as_posix()


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Populate a local GeoServer test instance with Natural Earth and NASA data.")
    parser.add_argument("--base-dir", default=os.getcwd(), help="Project base directory that contains docker-compose.geoserver-test.yml")
    parser.add_argument("--geoserver-url", default="http://localhost:8081/geoserver", help="GeoServer base URL")
    parser.add_argument("--username", default="admin", help="GeoServer admin username")
    parser.add_argument("--password", default="geoserver", help="GeoServer admin password")
    parser.add_argument("--workspaces", default=",".join(ALL_WORKSPACES), help="Comma-separated list of workspaces to populate. Defaults to all fixture workspaces.")
    parser.add_argument("--qgis-python", default=DEFAULT_QGIS_PYTHON, help="Path to python-qgis.bat")
    parser.add_argument("--gdal-translate", default=DEFAULT_GDAL_TRANSLATE, help="Path to gdal_translate.exe")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    selected_workspaces = parse_workspace_selection(args.workspaces)
    base_dir = Path(args.base_dir).resolve()
    qgis_python = Path(args.qgis_python)
    gdal_translate = Path(args.gdal_translate)
    if not qgis_python.exists():
        raise SystemExit(f"QGIS Python executable not found: {qgis_python}")
    if not gdal_translate.exists():
        raise SystemExit(f"GDAL translate executable not found: {gdal_translate}")

    prepared = prepare_data(base_dir, qgis_python, gdal_translate, selected_workspaces)
    data_dir = prepared["paths"]["data_dir"]
    client = GeoServerRest(args.geoserver_url, args.username, args.password)
    client.wait_until_ready()

    for workspace in list(LEGACY_FIXTURE_WORKSPACES) + selected_workspaces:
        client.delete_workspace_if_exists(workspace)
    for workspace in selected_workspaces:
        client.ensure_workspace(workspace)

    for spec in VECTOR_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        client.create_shapefile_store(
            workspace=spec.workspace,
            store=spec.store,
            relative_path=path_to_geoserver_relative(data_dir, prepared["vector_paths"][spec.store]),
            namespace_uri=f"http://{spec.workspace}",
            feature_name=spec.dataset_stem,
            title=spec.title,
        )

    for spec in GEOPACKAGE_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        client.create_geopackage_store(
            workspace=spec.workspace,
            store=spec.store,
            relative_path=path_to_geoserver_relative(data_dir, prepared["geopackage_paths"][spec.store]),
            namespace_uri=f"http://{spec.workspace}",
            layer_names=[layer_name for layer_name, _ in spec.layers],
            title=spec.title,
        )

    for spec in RASTER_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        raster_path = prepared["raster_paths"][spec.store]
        client.create_geotiff_store(
            workspace=spec.workspace,
            store=spec.store,
            relative_path=path_to_geoserver_relative(data_dir, raster_path),
            coverage_name=spec.store,
            native_name=raster_path.stem,
            title=spec.title,
        )

    for spec in MOSAIC_SPECS:
        if spec.workspace not in selected_workspaces:
            continue
        client.upload_imagemosaic_store(spec.workspace, spec.store, prepared["mosaic_zip_paths"][spec.store])

    total_store_count = (
        sum(1 for spec in VECTOR_SPECS if spec.workspace in selected_workspaces)
        + sum(1 for spec in GEOPACKAGE_SPECS if spec.workspace in selected_workspaces)
        + sum(1 for spec in RASTER_SPECS if spec.workspace in selected_workspaces)
        + sum(1 for spec in MOSAIC_SPECS if spec.workspace in selected_workspaces)
    )
    print(f"Fixture population finished successfully. Published {total_store_count} stores across {len(selected_workspaces)} workspaces.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
