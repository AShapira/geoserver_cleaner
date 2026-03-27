#!/usr/bin/env python
"""Create large mock GeoServer GeoTIFF catalogs directly in the data directory."""

from __future__ import annotations

import argparse
import base64
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from xml.sax.saxutils import escape


DEFAULT_QGIS_PYTHON = r"C:\Program Files\QGIS 3.44.5\bin\python-qgis.bat"
NATIVE_CRS_WKT = (
    'GEOGCS["GCS_WGS_1984", DATUM["D_WGS_1984", SPHEROID["WGS_1984", 6378137.0, '
    '298.257223563]], PRIMEM["Greenwich", 0.0], UNIT["degree", 0.017453292519943295], '
    'AXIS["Longitude", EAST], AXIS["Latitude", NORTH]]'
)
WIDTH = 10
HEIGHT = 10
MIN_X = -180.0
MAX_X = 180.0
MIN_Y = -90.0
MAX_Y = 90.0
SCALE_X = (MAX_X - MIN_X) / WIDTH
SCALE_Y = -(MAX_Y - MIN_Y) / HEIGHT
TRANSLATE_X = MIN_X + (SCALE_X / 2.0)
TRANSLATE_Y = MAX_Y + (SCALE_Y / 2.0)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink()


def run_process(args: list[str], input_text: Optional[str] = None) -> None:
    result = subprocess.run(
        args,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            "Command failed ({}):\nSTDOUT:\n{}\nSTDERR:\n{}".format(
                " ".join(args),
                result.stdout,
                result.stderr,
            )
        )


def make_id(prefix: str) -> str:
    return "{}-{}".format(prefix, uuid.uuid4())


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


def write_text(path: Path, text: str) -> None:
    ensure_directory(path.parent)
    path.write_text(text, encoding="utf-8")


def create_base_geotiff(qgis_python: Path, output_path: Path) -> None:
    if output_path.exists() and output_path.stat().st_size > 0:
        print(f"Using cached mock GeoTIFF template: {output_path}")
        return
    ensure_directory(output_path.parent)
    script = f"""
from osgeo import gdal, osr
from pathlib import Path
path = Path(r"{output_path}")
driver = gdal.GetDriverByName("GTiff")
if path.exists():
    driver.Delete(str(path))
dataset = driver.Create(str(path), {WIDTH}, {HEIGHT}, 1, gdal.GDT_Byte, options=["COMPRESS=DEFLATE"])
dataset.SetGeoTransform(({MIN_X}, {SCALE_X}, 0.0, {MAX_Y}, 0.0, {SCALE_Y}))
srs = osr.SpatialReference()
srs.ImportFromEPSG(4326)
dataset.SetProjection(srs.ExportToWkt())
band = dataset.GetRasterBand(1)
band.Fill(1)
band.SetNoDataValue(0)
dataset = None
"""
    run_process([str(qgis_python), "-"], input_text=script)
    print(f"Created mock GeoTIFF template: {output_path}")


def read_raster_style_id(data_dir: Path) -> str:
    style_path = data_dir / "styles" / "raster.xml"
    tree = ElementTree.parse(style_path)
    style_id = tree.findtext("id")
    if not style_id:
        raise RuntimeError(f"Raster style id not found in {style_path}")
    return style_id.strip()


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
        content_type: str = "application/json",
        expected: tuple[int, ...] = (200, 201, 202),
    ) -> bytes:
        url = urljoin(self.base_url, rest_path.lstrip("/"))
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "Authorization": self.auth_header,
                "Accept": "application/json",
                "Content-Type": content_type,
            },
        )
        try:
            with urlopen(request, timeout=300) as response:
                status = response.getcode()
                body = response.read()
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}") from exc
        except URLError as exc:
            raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc
        if status not in expected:
            raise RuntimeError(f"{method} {url} returned unexpected HTTP {status}")
        return body

    def wait_until_ready(self, timeout_seconds: int = 1800) -> None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                self.request("GET", "rest/about/version.json", expected=(200,))
                print("GeoServer REST is ready.")
                return
            except Exception:
                time.sleep(5)
        raise RuntimeError("Timed out waiting for GeoServer REST to become ready.")

    def reload(self) -> None:
        self.request("POST", "rest/reload", data=b"", content_type="text/plain", expected=(200, 201))
        print("Triggered GeoServer catalog reload.")


def workspace_xml(workspace_id: str, name: str, created: str) -> str:
    return (
        "<workspace>\n"
        f"  <id>{workspace_id}</id>\n"
        f"  <name>{escape(name)}</name>\n"
        "  <isolated>false</isolated>\n"
        f"  <dateCreated>{created}</dateCreated>\n"
        "</workspace>\n"
    )


def namespace_xml(namespace_id: str, name: str) -> str:
    return (
        "<namespace>\n"
        f"  <id>{namespace_id}</id>\n"
        f"  <prefix>{escape(name)}</prefix>\n"
        f"  <uri>http://{escape(name)}</uri>\n"
        "  <isolated>false</isolated>\n"
        "</namespace>\n"
    )


def coveragestore_xml(store_id: str, workspace_id: str, store_name: str, url: str, created: str) -> str:
    return (
        "<coverageStore>\n"
        f"  <id>{store_id}</id>\n"
        f"  <name>{escape(store_name)}</name>\n"
        "  <type>GeoTIFF</type>\n"
        "  <enabled>true</enabled>\n"
        "  <workspace>\n"
        f"    <id>{workspace_id}</id>\n"
        "  </workspace>\n"
        "  <__default>false</__default>\n"
        f"  <dateCreated>{created}</dateCreated>\n"
        "  <disableOnConnFailure>false</disableOnConnFailure>\n"
        f"  <url>{escape(url)}</url>\n"
        "</coverageStore>\n"
    )


def coverage_xml(
    coverage_id: str,
    store_id: str,
    namespace_id: str,
    store_name: str,
    native_name: str,
    created: str,
) -> str:
    return (
        "<coverage>\n"
        f"  <id>{coverage_id}</id>\n"
        f"  <name>{escape(store_name)}</name>\n"
        f"  <nativeName>{escape(native_name)}</nativeName>\n"
        "  <namespace>\n"
        f"    <id>{namespace_id}</id>\n"
        "  </namespace>\n"
        f"  <title>{escape(store_name)}</title>\n"
        f"  <nativeCRS>{escape(NATIVE_CRS_WKT)}</nativeCRS>\n"
        "  <srs>EPSG:4326</srs>\n"
        "  <nativeBoundingBox>\n"
        f"    <minx>{MIN_X}</minx>\n"
        f"    <maxx>{MAX_X}</maxx>\n"
        f"    <miny>{MIN_Y}</miny>\n"
        f"    <maxy>{MAX_Y}</maxy>\n"
        "    <crs>EPSG:4326</crs>\n"
        "  </nativeBoundingBox>\n"
        "  <latLonBoundingBox>\n"
        f"    <minx>{MIN_X}</minx>\n"
        f"    <maxx>{MAX_X}</maxx>\n"
        f"    <miny>{MIN_Y}</miny>\n"
        f"    <maxy>{MAX_Y}</maxy>\n"
        "    <crs>EPSG:4326</crs>\n"
        "  </latLonBoundingBox>\n"
        "  <enabled>true</enabled>\n"
        '  <store class="coverageStore">\n'
        f"    <id>{store_id}</id>\n"
        "  </store>\n"
        "  <serviceConfiguration>false</serviceConfiguration>\n"
        '  <grid dimension="2">\n'
        "    <range>\n"
        "      <low>0 0</low>\n"
        f"      <high>{WIDTH} {HEIGHT}</high>\n"
        "    </range>\n"
        "    <transform>\n"
        f"      <scaleX>{SCALE_X}</scaleX>\n"
        f"      <scaleY>{SCALE_Y}</scaleY>\n"
        "      <shearX>0.0</shearX>\n"
        "      <shearY>0.0</shearY>\n"
        f"      <translateX>{TRANSLATE_X}</translateX>\n"
        f"      <translateY>{TRANSLATE_Y}</translateY>\n"
        "    </transform>\n"
        "    <crs>EPSG:4326</crs>\n"
        "  </grid>\n"
        f"  <dateCreated>{created}</dateCreated>\n"
        "</coverage>\n"
    )


def layer_xml(layer_id: str, coverage_id: str, style_id: str, store_name: str, created: str) -> str:
    return (
        "<layer>\n"
        f"  <name>{escape(store_name)}</name>\n"
        f"  <id>{layer_id}</id>\n"
        "  <type>RASTER</type>\n"
        "  <defaultStyle>\n"
        f"    <id>{style_id}</id>\n"
        "  </defaultStyle>\n"
        '  <resource class="coverage">\n'
        f"    <id>{coverage_id}</id>\n"
        "  </resource>\n"
        "  <attribution>\n"
        "    <logoWidth>0</logoWidth>\n"
        "    <logoHeight>0</logoHeight>\n"
        "  </attribution>\n"
        f"  <dateCreated>{created}</dateCreated>\n"
        "</layer>\n"
    )


def populate_workspace(
    workspace_name: str,
    stores_per_workspace: int,
    data_root: Path,
    workspaces_root: Path,
    base_tif: Path,
    raster_style_id: str,
) -> None:
    print(f"Preparing workspace {workspace_name} with {stores_per_workspace} stores")
    remove_path(data_root / workspace_name)
    remove_path(workspaces_root / workspace_name)

    raster_dir = data_root / workspace_name / "raster"
    workspace_dir = workspaces_root / workspace_name
    ensure_directory(raster_dir)
    ensure_directory(workspace_dir)

    workspace_id = make_id("WorkspaceInfoImpl")
    namespace_id = make_id("NamespaceInfoImpl")
    created = utc_timestamp()
    write_text(workspace_dir / "workspace.xml", workspace_xml(workspace_id, workspace_name, created))
    write_text(workspace_dir / "namespace.xml", namespace_xml(namespace_id, workspace_name))

    for index in range(1, stores_per_workspace + 1):
        store_name = f"{workspace_name}_store_{index:05d}"
        tif_path = raster_dir / f"{store_name}.tif"
        shutil.copyfile(base_tif, tif_path)

        store_id = make_id("CoverageStoreInfoImpl")
        coverage_id = make_id("CoverageInfoImpl")
        layer_id = make_id("LayerInfoImpl")
        created = utc_timestamp()
        store_dir = workspace_dir / store_name
        coverage_dir = store_dir / store_name
        ensure_directory(coverage_dir)
        store_url = f"file:data/{workspace_name}/raster/{store_name}.tif"

        write_text(
            store_dir / "coveragestore.xml",
            coveragestore_xml(store_id, workspace_id, store_name, store_url, created),
        )
        write_text(
            coverage_dir / "coverage.xml",
            coverage_xml(coverage_id, store_id, namespace_id, store_name, store_name, created),
        )
        write_text(
            coverage_dir / "layer.xml",
            layer_xml(layer_id, coverage_id, raster_style_id, store_name, created),
        )

        if index % 1000 == 0 or index == stores_per_workspace:
            print(f"  {workspace_name}: created {index}/{stores_per_workspace} stores")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create large mock GeoTIFF workspaces directly in the GeoServer data directory.",
    )
    parser.add_argument("--base-dir", default=".", help="Project base directory")
    parser.add_argument("--geoserver-url", default="http://localhost:8081/geoserver", help="GeoServer base URL")
    parser.add_argument("--username", default="admin", help="GeoServer admin username")
    parser.add_argument("--password", default="geoserver", help="GeoServer admin password")
    parser.add_argument("--qgis-python", default=DEFAULT_QGIS_PYTHON, help="Path to python-qgis.bat")
    parser.add_argument("--workspace-prefix", default="mock_ws_", help="Workspace prefix")
    parser.add_argument("--workspace-count", type=int, default=3, help="Number of mock workspaces")
    parser.add_argument("--stores-per-workspace", type=int, default=10000, help="Stores per workspace")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    base_dir = Path(args.base_dir).resolve()
    qgis_python = Path(args.qgis_python)
    if not qgis_python.exists():
        raise SystemExit(f"QGIS Python executable not found: {qgis_python}")

    data_dir = base_dir / "docker" / "geoserver_data"
    data_root = data_dir / "data"
    workspaces_root = data_dir / "workspaces"
    template_dir = base_dir / "docker" / "downloads" / "mock_templates"
    base_tif = template_dir / "mock_template_4326.tif"
    ensure_directory(template_dir)
    create_base_geotiff(qgis_python, base_tif)
    raster_style_id = read_raster_style_id(data_dir)

    workspace_names = [
        "{}{:02d}".format(args.workspace_prefix, index)
        for index in range(1, args.workspace_count + 1)
    ]

    for workspace_name in workspace_names:
        populate_workspace(
            workspace_name=workspace_name,
            stores_per_workspace=args.stores_per_workspace,
            data_root=data_root,
            workspaces_root=workspaces_root,
            base_tif=base_tif,
            raster_style_id=raster_style_id,
        )

    client = GeoServerRest(args.geoserver_url, args.username, args.password)
    client.reload()
    client.wait_until_ready()
    print(
        "Created {} workspaces with {} stores each.".format(
            len(workspace_names),
            args.stores_per_workspace,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
