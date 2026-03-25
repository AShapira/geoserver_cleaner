#!/usr/bin/env python
"""
Generate a CSV inventory of GeoServer stores and orphaned data files.

Designed to run in the QGIS Python shell or with a regular Python interpreter.
It uses only the standard library so it does not depend on third-party modules.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import ssl
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urljoin
from urllib.request import (
    HTTPBasicAuthHandler,
    HTTPPasswordMgrWithDefaultRealm,
    HTTPSHandler,
    Request,
    build_opener,
)


SHAPEFILE_EXTENSIONS = {
    ".shp",
    ".shx",
    ".dbf",
    ".prj",
    ".cpg",
    ".qix",
    ".fix",
    ".sbn",
    ".sbx",
    ".aih",
    ".ain",
    ".atx",
    ".ixs",
    ".mxs",
    ".qpj",
    ".xml",
}

GEOPACKAGE_SIDE_SUFFIXES = {
    ".gpkg-wal",
    ".gpkg-shm",
    ".gpkg-journal",
}

RASTER_EXACT_SUFFIXES = {
    "",
    ".ovr",
    ".aux",
    ".aux.xml",
    ".xml",
}

RASTER_STEM_SUFFIXES = {
    ".ovr",
    ".aux",
    ".aux.xml",
    ".xml",
    ".prj",
    ".wld",
    ".tfw",
    ".tifw",
    ".tab",
}


@dataclass
class ScanResult:
    size_bytes: int
    file_count: int
    referenced_files: Set[str]


def normalize_path(path: str) -> str:
    return os.path.normcase(os.path.normpath(path))


def bytes_to_gb(size_bytes: int) -> float:
    return round(size_bytes / (1024.0 ** 3), 6)


def as_list(value) -> List[dict]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def entries_to_dict(entries) -> Dict[str, str]:
    params: Dict[str, str] = {}
    for entry in as_list(entries):
        if isinstance(entry, dict):
            key = entry.get("@key")
            if not key:
                continue
            value = entry.get("$")
            if value is None and "#text" in entry:
                value = entry["#text"]
            if value is None and "value" in entry:
                value = entry["value"]
            params[str(key)] = "" if value is None else str(value)
    return params


class GeoServerClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 60,
        insecure: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        password_mgr = HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(
            realm=None,
            uri=self.base_url,
            user=username,
            passwd=password,
        )

        handlers = [HTTPBasicAuthHandler(password_mgr)]
        if insecure:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            handlers.append(HTTPSHandler(context=context))

        self.opener = build_opener(*handlers)

    def get_json(self, rest_path: str) -> dict:
        url = urljoin(self.base_url, rest_path.lstrip("/"))
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            raise RuntimeError(
                "GeoServer request failed with HTTP {} for {}".format(exc.code, url)
            ) from exc
        except URLError as exc:
            raise RuntimeError(
                "GeoServer request failed for {}: {}".format(url, exc.reason)
            ) from exc

        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Invalid JSON from {}: {}".format(url, exc)) from exc


def list_workspaces(client: GeoServerClient) -> List[str]:
    payload = client.get_json("rest/workspaces.json")
    workspaces = payload.get("workspaces", {}).get("workspace")
    names = []
    for item in as_list(workspaces):
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return names


def list_store_refs(client: GeoServerClient, workspace: str, store_kind: str) -> List[str]:
    workspace_q = quote(workspace, safe="")
    endpoint = "rest/workspaces/{}/{}.json".format(workspace_q, store_kind)
    payload = client.get_json(endpoint)
    root_key = "dataStores" if store_kind == "datastores" else "coverageStores"
    item_key = "dataStore" if store_kind == "datastores" else "coverageStore"
    items = payload.get(root_key, {}).get(item_key)
    names = []
    for item in as_list(items):
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return names


def get_store_detail(
    client: GeoServerClient,
    workspace: str,
    store_kind: str,
    store_name: str,
) -> dict:
    workspace_q = quote(workspace, safe="")
    store_q = quote(store_name, safe="")
    endpoint = "rest/workspaces/{}/{}/{}.json".format(workspace_q, store_kind, store_q)
    payload = client.get_json(endpoint)
    root_key = "dataStore" if store_kind == "datastores" else "coverageStore"
    return payload.get(root_key, {})


def list_store_layers(
    client: GeoServerClient,
    workspace: str,
    store_kind: str,
    store_name: str,
) -> List[str]:
    workspace_q = quote(workspace, safe="")
    store_q = quote(store_name, safe="")
    if store_kind == "datastores":
        endpoint = "rest/workspaces/{}/datastores/{}/featuretypes.json".format(
            workspace_q,
            store_q,
        )
        root_key = "featureTypes"
        item_key = "featureType"
    else:
        endpoint = "rest/workspaces/{}/coveragestores/{}/coverages.json".format(
            workspace_q,
            store_q,
        )
        root_key = "coverages"
        item_key = "coverage"

    try:
        payload = client.get_json(endpoint)
    except RuntimeError:
        return []

    items = payload.get(root_key, {}).get(item_key)
    names = []
    for item in as_list(items):
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return sorted(set(names))


def extract_store_path(store_detail: dict, store_kind: str) -> str:
    if store_kind == "coveragestores":
        value = store_detail.get("url", "")
        return str(value).strip()

    params = entries_to_dict(store_detail.get("connectionParameters", {}).get("entry"))
    for key in ("url", "database", "file", "path"):
        value = params.get(key)
        if value:
            return value.strip()

    candidates = []
    for key, value in params.items():
        value = (value or "").strip()
        if not value:
            continue
        lower = value.lower()
        if lower.startswith("file:") or re.match(r"^[a-z]:[\\/]", value, re.I) or value.startswith("\\\\"):
            candidates.append(value)
    return candidates[0] if candidates else ""


def resolve_store_path(configured_path: str, data_dir: str) -> str:
    if not configured_path:
        return ""

    data_dir = os.path.abspath(data_dir)
    value = unquote(configured_path.strip()).replace("/", os.sep).replace("\\", os.sep)
    lower = value.lower()

    if lower.startswith("file:"):
        suffix = value[5:]
        suffix = suffix.replace("/", os.sep).replace("\\", os.sep)
        if re.match(r"^[/\\]+[a-zA-Z]:", suffix):
            suffix = suffix.lstrip("/\\")
            return os.path.abspath(suffix)
        if re.match(r"^[a-zA-Z]:[\\/]", suffix) or suffix.startswith("\\\\"):
            return os.path.abspath(suffix)
        if lower.startswith("file:data" + os.sep) or lower == "file:data":
            relative = suffix[len("data") :].lstrip("/\\")
            return os.path.abspath(os.path.join(data_dir, "data", relative))
        return os.path.abspath(os.path.join(data_dir, suffix))

    if re.match(r"^[a-zA-Z]:[\\/]", value) or value.startswith("\\\\"):
        return os.path.abspath(value)

    if lower == "data" or lower.startswith("data" + os.sep):
        relative = value[len("data") :].lstrip("/\\")
        return os.path.abspath(os.path.join(data_dir, "data", relative))

    return os.path.abspath(os.path.join(data_dir, value))


def scan_directory(path: str) -> ScanResult:
    total_size = 0
    total_count = 0
    referenced_files: Set[str] = set()

    for root, _, files in os.walk(path):
        for filename in files:
            file_path = os.path.join(root, filename)
            try:
                stat = os.stat(file_path)
            except OSError:
                continue
            total_size += stat.st_size
            total_count += 1
            referenced_files.add(normalize_path(file_path))

    return ScanResult(total_size, total_count, referenced_files)


def scan_file_bundle(path: str, store_type: str) -> ScanResult:
    directory = os.path.dirname(path) or "."
    filename = os.path.basename(path)
    stem, extension = os.path.splitext(filename)
    lower_type = (store_type or "").lower()
    stem_prefix = stem.lower() + "."
    filename_prefix = filename.lower() + "."

    selected: Set[str] = set()
    try:
        names = os.listdir(directory)
    except OSError:
        names = []

    if "shape" in lower_type or extension.lower() == ".shp":
        for name in names:
            lower_name = name.lower()
            full = os.path.join(directory, name)
            if lower_name == filename.lower() or lower_name.startswith(stem_prefix):
                selected.add(full)
    elif "geopackage" in lower_type or extension.lower() == ".gpkg":
        for name in names:
            full = os.path.join(directory, name)
            lower_name = name.lower()
            if lower_name == filename.lower() or lower_name.startswith(stem_prefix):
                selected.add(full)
            for suffix in GEOPACKAGE_SIDE_SUFFIXES:
                if lower_name == stem.lower() + suffix:
                    selected.add(full)
    else:
        for name in names:
            full = os.path.join(directory, name)
            lower_name = name.lower()
            if lower_name == filename.lower() or lower_name.startswith(stem_prefix):
                selected.add(full)
                continue
            for suffix in RASTER_EXACT_SUFFIXES:
                if suffix and lower_name == filename.lower() + suffix:
                    selected.add(full)
                    break
            if lower_name.startswith(filename_prefix):
                selected.add(full)
                continue
            for suffix in RASTER_STEM_SUFFIXES:
                if lower_name == stem.lower() + suffix:
                    selected.add(full)
                    break

    if not selected and os.path.exists(path):
        selected.add(path)

    total_size = 0
    total_count = 0
    referenced_files: Set[str] = set()
    for item in selected:
        try:
            stat = os.stat(item)
        except OSError:
            continue
        if not os.path.isfile(item):
            continue
        total_size += stat.st_size
        total_count += 1
        referenced_files.add(normalize_path(item))

    return ScanResult(total_size, total_count, referenced_files)


def path_under_any_root(path: str, roots: Sequence[str]) -> bool:
    normalized = normalize_path(path)
    for root in roots:
        if normalized == root or normalized.startswith(root + os.sep):
            return True
    return False


def scan_any_path(path: str, store_type: str) -> ScanResult:
    if os.path.isdir(path):
        return scan_directory(path)
    return scan_file_bundle(path, store_type)


def collect_orphans(
    data_root: str,
    referenced_roots: Sequence[str],
    referenced_files: Set[str],
) -> List[dict]:
    orphan_rows: List[dict] = []
    referenced_file_set = {normalize_path(item) for item in referenced_files}
    normalized_roots = [normalize_path(item) for item in referenced_roots]

    def visit_dir(path: str) -> Tuple[bool, int, int]:
        normalized = normalize_path(path)
        if path_under_any_root(normalized, normalized_roots):
            return True, 0, 0

        has_referenced = False
        total_size = 0
        total_count = 0
        orphan_children: List[dict] = []

        try:
            entries = list(os.scandir(path))
        except OSError as exc:
            orphan_children.append(
                build_row(
                    row_kind="orphaned",
                    workspace="",
                    store_name="",
                    store_type="",
                    layer_names="",
                    configured_path="",
                    resolved_path=path,
                    path_kind="directory",
                    size_bytes=0,
                    file_count=0,
                    status="error",
                    notes=str(exc),
                )
            )
            return False, 0, 0

        for entry in entries:
            child_path = entry.path
            if entry.is_dir(follow_symlinks=False):
                child_has_ref, child_size, child_count = visit_dir(child_path)
                total_size += child_size
                total_count += child_count
                if child_has_ref:
                    has_referenced = True
                else:
                    orphan_children.append(
                        build_row(
                            row_kind="orphaned",
                            workspace="",
                            store_name="",
                            store_type="",
                            layer_names="",
                            configured_path="",
                            resolved_path=child_path,
                            path_kind="directory",
                            size_bytes=child_size,
                            file_count=child_count,
                            status="orphaned",
                            notes="Directory is not referenced by any GeoServer store.",
                        )
                    )
            elif entry.is_file(follow_symlinks=False):
                try:
                    stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                total_size += stat.st_size
                total_count += 1
                if normalize_path(child_path) in referenced_file_set:
                    has_referenced = True
                else:
                    orphan_children.append(
                        build_row(
                            row_kind="orphaned",
                            workspace="",
                            store_name="",
                            store_type="",
                            layer_names="",
                            configured_path="",
                            resolved_path=child_path,
                            path_kind="file",
                            size_bytes=stat.st_size,
                            file_count=1,
                            status="orphaned",
                            notes="File is not referenced by any GeoServer store.",
                        )
                    )

        if has_referenced:
            orphan_rows.extend(orphan_children)
            return True, total_size, total_count

        return False, total_size, total_count

    try:
        root_entries = list(os.scandir(data_root))
    except OSError as exc:
        orphan_rows.append(
            build_row(
                row_kind="orphaned",
                workspace="",
                store_name="",
                store_type="",
                layer_names="",
                configured_path="",
                resolved_path=data_root,
                path_kind="directory",
                size_bytes=0,
                file_count=0,
                status="error",
                notes=str(exc),
            )
        )
        return orphan_rows

    for entry in root_entries:
        path = entry.path
        if entry.is_dir(follow_symlinks=False):
            if path_under_any_root(path, normalized_roots):
                continue
            has_referenced, total_size, total_count = visit_dir(path)
            if not has_referenced:
                orphan_rows.append(
                    build_row(
                        row_kind="orphaned",
                        workspace="",
                        store_name="",
                        store_type="",
                        layer_names="",
                        configured_path="",
                        resolved_path=path,
                        path_kind="directory",
                        size_bytes=total_size,
                        file_count=total_count,
                        status="orphaned",
                        notes="Directory is not referenced by any GeoServer store.",
                    )
                )
        elif entry.is_file(follow_symlinks=False):
            normalized = normalize_path(path)
            if normalized not in referenced_file_set:
                try:
                    stat = entry.stat(follow_symlinks=False)
                except OSError:
                    continue
                orphan_rows.append(
                    build_row(
                        row_kind="orphaned",
                        workspace="",
                        store_name="",
                        store_type="",
                        layer_names="",
                        configured_path="",
                        resolved_path=path,
                        path_kind="file",
                        size_bytes=stat.st_size,
                        file_count=1,
                        status="orphaned",
                        notes="File is not referenced by any GeoServer store.",
                    )
                )

    orphan_rows.sort(key=lambda item: normalize_path(item["resolved_path"]))
    return orphan_rows


def build_row(
    row_kind: str,
    workspace: str,
    store_name: str,
    store_type: str,
    layer_names: str,
    configured_path: str,
    resolved_path: str,
    path_kind: str,
    size_bytes: int,
    file_count: int,
    status: str,
    notes: str,
) -> dict:
    return {
        "row_kind": row_kind,
        "workspace": workspace,
        "store_name": store_name,
        "store_type": store_type,
        "layer_names": layer_names,
        "configured_path": configured_path,
        "resolved_path": resolved_path,
        "path_kind": path_kind,
        "size_bytes": size_bytes,
        "size_gb": bytes_to_gb(size_bytes),
        "file_count": file_count,
        "status": status,
        "notes": notes,
    }


def inventory_stores(client: GeoServerClient, data_dir: str) -> Tuple[List[dict], List[str], Set[str]]:
    rows: List[dict] = []
    referenced_roots: List[str] = []
    referenced_files: Set[str] = set()

    for workspace in list_workspaces(client):
        for store_kind in ("coveragestores", "datastores"):
            for store_name in list_store_refs(client, workspace, store_kind):
                try:
                    detail = get_store_detail(client, workspace, store_kind, store_name)
                    store_type = str(detail.get("type", "")).strip()
                    configured_path = extract_store_path(detail, store_kind)
                    resolved_path = resolve_store_path(configured_path, data_dir)
                    layer_names = ", ".join(list_store_layers(client, workspace, store_kind, store_name))

                    if not configured_path:
                        rows.append(
                            build_row(
                                row_kind="store",
                                workspace=workspace,
                                store_name=store_name,
                                store_type=store_type,
                                layer_names=layer_names,
                                configured_path="",
                                resolved_path="",
                                path_kind="",
                                size_bytes=0,
                                file_count=0,
                                status="unresolved",
                                notes="Could not find a usable filesystem path in store configuration.",
                            )
                        )
                        continue

                    if not os.path.exists(resolved_path):
                        rows.append(
                            build_row(
                                row_kind="store",
                                workspace=workspace,
                                store_name=store_name,
                                store_type=store_type,
                                layer_names=layer_names,
                                configured_path=configured_path,
                                resolved_path=resolved_path,
                                path_kind="missing",
                                size_bytes=0,
                                file_count=0,
                                status="missing",
                                notes="Resolved path does not exist on disk.",
                            )
                        )
                        continue

                    scan = scan_any_path(resolved_path, store_type)
                    path_kind = "directory" if os.path.isdir(resolved_path) else "file"
                    if path_kind == "directory":
                        referenced_roots.append(normalize_path(resolved_path))
                    else:
                        referenced_files.update(scan.referenced_files)

                    rows.append(
                        build_row(
                            row_kind="store",
                            workspace=workspace,
                            store_name=store_name,
                            store_type=store_type,
                            layer_names=layer_names,
                            configured_path=configured_path,
                            resolved_path=resolved_path,
                            path_kind=path_kind,
                            size_bytes=scan.size_bytes,
                            file_count=scan.file_count,
                            status="ok",
                            notes="",
                        )
                    )
                except Exception as exc:
                    rows.append(
                        build_row(
                            row_kind="store",
                            workspace=workspace,
                            store_name=store_name,
                            store_type="",
                            layer_names="",
                            configured_path="",
                            resolved_path="",
                            path_kind="",
                            size_bytes=0,
                            file_count=0,
                            status="error",
                            notes=str(exc),
                        )
                    )

    return rows, referenced_roots, referenced_files


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inventory GeoServer stores and orphaned data files.",
    )
    parser.add_argument(
        "--geoserver-url",
        default=os.environ.get("GEOSERVER_URL", ""),
        help="GeoServer base URL, for example http://localhost:8080/geoserver",
    )
    parser.add_argument(
        "--username",
        default=os.environ.get("GEOSERVER_USER", "admin"),
        help="GeoServer username",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("GEOSERVER_PASSWORD", ""),
        help="GeoServer password",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("GEOSERVER_DATA_DIR", ""),
        help="GeoServer data directory path",
    )
    parser.add_argument(
        "--output-csv",
        default=os.path.abspath("geoserver_store_report.csv"),
        help="CSV output path",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL certificate verification for HTTPS GeoServer URLs.",
    )
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    missing = []
    if not args.geoserver_url:
        missing.append("--geoserver-url")
    if not args.password:
        missing.append("--password")
    if not args.data_dir:
        missing.append("--data-dir")
    if missing:
        raise SystemExit("Missing required arguments: {}".format(", ".join(missing)))


def write_csv(path: str, rows: Sequence[dict]) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)

    fieldnames = [
        "row_kind",
        "workspace",
        "store_name",
        "store_type",
        "layer_names",
        "configured_path",
        "resolved_path",
        "path_kind",
        "size_bytes",
        "size_gb",
        "file_count",
        "status",
        "notes",
    ]

    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    validate_args(args)

    data_dir = os.path.abspath(args.data_dir)
    data_root = os.path.join(data_dir, "data")
    if not os.path.isdir(data_root):
        raise SystemExit("GeoServer data path does not exist: {}".format(data_root))

    client = GeoServerClient(
        base_url=args.geoserver_url,
        username=args.username,
        password=args.password,
        timeout=args.timeout,
        insecure=args.insecure,
    )

    store_rows, referenced_roots, referenced_files = inventory_stores(client, data_dir)
    orphan_rows = collect_orphans(data_root, referenced_roots, referenced_files)
    rows = sorted(
        store_rows,
        key=lambda item: (
            item["row_kind"],
            item["workspace"].lower(),
            item["store_name"].lower(),
            normalize_path(item["resolved_path"] or item["configured_path"] or ""),
        ),
    )
    rows.extend(orphan_rows)
    write_csv(args.output_csv, rows)

    store_count = sum(1 for row in store_rows if row["row_kind"] == "store")
    orphan_count = sum(1 for row in orphan_rows if row["row_kind"] == "orphaned")
    print("Wrote {} store rows and {} orphan rows to {}".format(store_count, orphan_count, args.output_csv))
    return 0


if __name__ == "__main__":
    sys.exit(main())
