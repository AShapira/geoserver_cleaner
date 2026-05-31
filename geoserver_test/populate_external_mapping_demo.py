#!/usr/bin/env python
"""Create a small GeoServer catalog fixture for external path mapping demos."""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from xml.sax.saxutils import escape


WORKSPACE = "external_mapping_demo"
WINDOWS_SOURCE_ROOT = r"C:\demo_geodata\windows"
WINDOWS_MISSING_SOURCE_ROOT = r"C:\demo_geodata\missing"
POSIX_SOURCE_ROOT = "/srv/geodata/posix"
DOCKER_WINDOWS_ROOT = "/external_windows"
DOCKER_POSIX_ROOT = "/external_posix"
DOCKER_MISSING_ROOT = "/external_missing"
INTERNAL_STORE = "internal_raster"
WINDOWS_STORE = "windows_external_raster"
POSIX_STORE = "posix_external_raster"
MISSING_STORE = "missing_external_raster"


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink()


def write_text(path: Path, text: str) -> None:
    ensure_directory(path.parent)
    path.write_text(text, encoding="utf-8")


def write_demo_raster(path: Path, label: str) -> None:
    ensure_directory(path.parent)
    path.write_bytes(("GeoServer Cleaner external mapping demo: {}\n".format(label)).encode("utf-8"))


def make_id(prefix: str) -> str:
    return "{}-{}".format(prefix, uuid.uuid4())


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " UTC"


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


def coveragestore_xml(store_id: str, workspace_id: str, store_name: str, configured_path: str, created: str) -> str:
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
        f"  <url>{escape(configured_path)}</url>\n"
        "</coverageStore>\n"
    )


def coverage_xml(coverage_id: str, store_id: str, namespace_id: str, store_name: str, created: str) -> str:
    return (
        "<coverage>\n"
        f"  <id>{coverage_id}</id>\n"
        f"  <name>{escape(store_name)}</name>\n"
        f"  <nativeName>{escape(store_name)}</nativeName>\n"
        "  <namespace>\n"
        f"    <id>{namespace_id}</id>\n"
        "  </namespace>\n"
        f"  <title>{escape(store_name)}</title>\n"
        "  <srs>EPSG:4326</srs>\n"
        "  <enabled>true</enabled>\n"
        '  <store class="coverageStore">\n'
        f"    <id>{store_id}</id>\n"
        "  </store>\n"
        "  <serviceConfiguration>false</serviceConfiguration>\n"
        f"  <dateCreated>{created}</dateCreated>\n"
        "</coverage>\n"
    )


def layer_xml(layer_id: str, coverage_id: str, store_name: str, created: str) -> str:
    return (
        "<layer>\n"
        f"  <name>{escape(store_name)}</name>\n"
        f"  <id>{layer_id}</id>\n"
        "  <type>RASTER</type>\n"
        '  <resource class="coverage">\n'
        f"    <id>{coverage_id}</id>\n"
        "  </resource>\n"
        f"  <dateCreated>{created}</dateCreated>\n"
        "</layer>\n"
    )


def write_coverage_store(
    workspace_dir: Path,
    workspace_id: str,
    namespace_id: str,
    store_name: str,
    configured_path: str,
) -> None:
    created = utc_timestamp()
    store_id = make_id("CoverageStoreInfoImpl")
    coverage_id = make_id("CoverageInfoImpl")
    layer_id = make_id("LayerInfoImpl")
    store_dir = workspace_dir / store_name
    coverage_dir = store_dir / store_name
    write_text(
        store_dir / "coveragestore.xml",
        coveragestore_xml(store_id, workspace_id, store_name, configured_path, created),
    )
    write_text(
        coverage_dir / "coverage.xml",
        coverage_xml(coverage_id, store_id, namespace_id, store_name, created),
    )
    write_text(
        coverage_dir / "layer.xml",
        layer_xml(layer_id, coverage_id, store_name, created),
    )


def build_local_external_path_mappings(base_dir: Path) -> dict[str, str]:
    external_data = base_dir / "geoserver_test" / "external_data"
    return {
        WINDOWS_SOURCE_ROOT: str((external_data / "windows").resolve()),
        POSIX_SOURCE_ROOT: str((external_data / "posix").resolve()),
        WINDOWS_MISSING_SOURCE_ROOT: str((external_data / "missing").resolve()),
    }


def build_docker_external_path_mappings() -> dict[str, str]:
    return {
        WINDOWS_SOURCE_ROOT: DOCKER_WINDOWS_ROOT,
        POSIX_SOURCE_ROOT: DOCKER_POSIX_ROOT,
        WINDOWS_MISSING_SOURCE_ROOT: DOCKER_MISSING_ROOT,
    }


def prepare_fixture(base_dir: Path, clean: bool = True) -> dict[str, object]:
    geoserver_test_dir = base_dir / "geoserver_test"
    data_dir = geoserver_test_dir / "geoserver_data"
    data_root = data_dir / "data"
    workspaces_root = data_dir / "workspaces"
    external_data = geoserver_test_dir / "external_data"
    workspace_dir = workspaces_root / WORKSPACE

    if clean:
        remove_path(data_root / WORKSPACE)
        remove_path(workspace_dir)
        remove_path(external_data)

    internal_raster = data_root / WORKSPACE / "internal" / "internal.tif"
    windows_raster = external_data / "windows" / "mapped_windows.tif"
    posix_raster = external_data / "posix" / "mapped_posix.tif"
    write_demo_raster(internal_raster, "internal data_dir coverage")
    write_demo_raster(windows_raster, "windows-style external coverage")
    write_demo_raster(posix_raster, "posix-style external coverage")

    workspace_id = make_id("WorkspaceInfoImpl")
    namespace_id = make_id("NamespaceInfoImpl")
    created = utc_timestamp()
    write_text(workspace_dir / "workspace.xml", workspace_xml(workspace_id, WORKSPACE, created))
    write_text(workspace_dir / "namespace.xml", namespace_xml(namespace_id, WORKSPACE))
    write_coverage_store(
        workspace_dir,
        workspace_id,
        namespace_id,
        INTERNAL_STORE,
        "file:data/{}/internal/internal.tif".format(WORKSPACE),
    )
    write_coverage_store(
        workspace_dir,
        workspace_id,
        namespace_id,
        WINDOWS_STORE,
        WINDOWS_SOURCE_ROOT + r"\mapped_windows.tif",
    )
    write_coverage_store(
        workspace_dir,
        workspace_id,
        namespace_id,
        POSIX_STORE,
        POSIX_SOURCE_ROOT + "/mapped_posix.tif",
    )
    write_coverage_store(
        workspace_dir,
        workspace_id,
        namespace_id,
        MISSING_STORE,
        WINDOWS_MISSING_SOURCE_ROOT + r"\missing.tif",
    )

    return {
        "workspace": WORKSPACE,
        "data_dir": str(data_dir.resolve()),
        "external_data": str(external_data.resolve()),
        "local_external_path_mappings": build_local_external_path_mappings(base_dir),
        "docker_external_path_mappings": build_docker_external_path_mappings(),
        "stores": [INTERNAL_STORE, WINDOWS_STORE, POSIX_STORE, MISSING_STORE],
    }


class GeoServerRest:
    def __init__(self, base_url: str, username: str, password: str) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        self.auth_header = f"Basic {token}"

    def request(self, method: str, rest_path: str, expected: tuple[int, ...]) -> bytes:
        url = urljoin(self.base_url, rest_path.lstrip("/"))
        request = Request(
            url,
            method=method,
            headers={
                "Authorization": self.auth_header,
                "Accept": "application/json",
                "Content-Type": "text/plain",
            },
        )
        try:
            with urlopen(request, timeout=60) as response:
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

    def reload(self) -> None:
        self.request("POST", "rest/reload", expected=(200, 201))


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create the external mapping demo GeoServer catalog fixture.")
    parser.add_argument(
        "--base-dir",
        default=os.getcwd(),
        help="Project base directory that contains the geoserver_test fixture directory",
    )
    parser.add_argument("--no-clean", action="store_true", help="Keep existing external mapping demo files")
    parser.add_argument("--reload-geoserver", action="store_true", help="POST /rest/reload after writing catalog XML")
    parser.add_argument("--geoserver-url", default="http://localhost:8081/geoserver", help="GeoServer base URL")
    parser.add_argument("--username", default="admin", help="GeoServer admin username")
    parser.add_argument("--password", default="geoserver", help="GeoServer admin password")
    parser.add_argument("--print-docker-mapping", action="store_true", help="Print Docker JSON mapping for compose use")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)
    base_dir = Path(args.base_dir).resolve()
    result = prepare_fixture(base_dir, clean=not args.no_clean)
    print("Created external mapping demo fixture.")
    print("GeoServer data_dir: {}".format(result["data_dir"]))
    print("External data: {}".format(result["external_data"]))
    print("Stores: {}".format(", ".join(result["stores"])))
    if args.print_docker_mapping:
        print(json.dumps(result["docker_external_path_mappings"], sort_keys=True))
    if args.reload_geoserver:
        GeoServerRest(args.geoserver_url, args.username, args.password).reload()
        print("Triggered GeoServer catalog reload.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
