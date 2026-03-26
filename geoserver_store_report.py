#!/usr/bin/env python
"""
Generate a CSV inventory of GeoServer stores and orphaned data files.

Designed to run in the QGIS Python shell or with a regular Python interpreter.
It uses only the standard library so it does not depend on third-party modules.
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import logging
import os
import re
import ssl
import sys
from datetime import datetime
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

LOGGER = logging.getLogger("geoserver_store_report")

HTML_COLUMNS = [
    ("row_kind", "Row Type", "text"),
    ("workspace", "Workspace", "text"),
    ("store_name", "Store", "text"),
    ("store_type", "Store Type", "text"),
    ("layer_names", "Layer Names", "text"),
    ("configured_path", "Configured Path", "text"),
    ("resolved_path", "Resolved Path", "text"),
    ("path_kind", "Path Kind", "text"),
    ("size_bytes", "Size (Bytes)", "number"),
    ("size_gb", "Size (GB)", "number"),
    ("file_count", "Files", "number"),
    ("status", "Status", "text"),
    ("notes", "Notes", "text"),
]


@dataclass
class ScanResult:
    size_bytes: int
    file_count: int
    referenced_files: Set[str]


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def normalize_path(path: str) -> str:
    return os.path.normcase(os.path.normpath(path))


def bytes_to_gb(size_bytes: int) -> str:
    return "{:.2f}".format(size_bytes / (1024.0 ** 3))


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


def parse_excluded_workspaces(raw: str) -> Set[str]:
    return {item.strip().lower() for item in raw.split(",") if item.strip()}


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
                raw = response.read().decode("utf-8", errors="replace")
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
            snippet = raw[:200].replace("\n", " ").replace("\r", " ")
            raise RuntimeError(
                "Invalid JSON from {}: {} (body starts with {!r})".format(url, exc, snippet)
            ) from exc


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
    detail = payload.get(root_key, {})
    if not isinstance(detail, dict):
        raise RuntimeError("Unexpected store detail payload type for {}".format(store_name))
    return detail


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
    except RuntimeError as exc:
        LOGGER.warning(
            "Failed to list layers for workspace=%s store=%s kind=%s: %s",
            workspace,
            store_name,
            store_kind,
            exc,
        )
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
                            notes="Directory is not referenced by any included GeoServer store.",
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
                            notes="File is not referenced by any included GeoServer store.",
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
                        notes="Directory is not referenced by any included GeoServer store.",
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
                        notes="File is not referenced by any included GeoServer store.",
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


def build_error_row(
    workspace: str,
    store_name: str,
    status: str,
    notes: str,
    store_type: str = "",
) -> dict:
    return build_row(
        row_kind="store",
        workspace=workspace,
        store_name=store_name,
        store_type=store_type,
        layer_names="",
        configured_path="",
        resolved_path="",
        path_kind="",
        size_bytes=0,
        file_count=0,
        status=status,
        notes=notes,
    )


def inventory_stores(
    client: GeoServerClient,
    data_dir: str,
    excluded_workspaces: Set[str],
) -> Tuple[List[dict], List[str], Set[str]]:
    rows: List[dict] = []
    referenced_roots: List[str] = []
    referenced_files: Set[str] = set()

    workspaces = list_workspaces(client)
    LOGGER.info("Discovered %d workspace(s)", len(workspaces))

    for workspace in workspaces:
        workspace_excluded = workspace.lower() in excluded_workspaces
        if workspace_excluded:
            LOGGER.info("Workspace %s is excluded from report rows", workspace)
            fallback_root = os.path.join(data_dir, "data", workspace)
            if os.path.isdir(fallback_root):
                referenced_roots.append(normalize_path(fallback_root))
                LOGGER.info(
                    "Marked excluded workspace root as referenced: %s",
                    fallback_root,
                )

        for store_kind in ("coveragestores", "datastores"):
            try:
                store_names = list_store_refs(client, workspace, store_kind)
                LOGGER.info(
                    "Workspace %s: discovered %d %s",
                    workspace,
                    len(store_names),
                    store_kind,
                )
            except Exception as exc:
                LOGGER.warning(
                    "Failed to list %s for workspace %s: %s",
                    store_kind,
                    workspace,
                    exc,
                )
                if not workspace_excluded:
                    rows.append(
                        build_error_row(
                            workspace=workspace,
                            store_name="",
                            status="error",
                            notes="Failed to list {}: {}".format(store_kind, exc),
                        )
                    )
                continue

            for store_name in store_names:
                LOGGER.info(
                    "Processing workspace=%s kind=%s store=%s",
                    workspace,
                    store_kind,
                    store_name,
                )
                try:
                    detail = get_store_detail(client, workspace, store_kind, store_name)
                    store_type = str(detail.get("type", "")).strip()
                    configured_path = extract_store_path(detail, store_kind)
                    resolved_path = resolve_store_path(configured_path, data_dir)
                    layer_names = ", ".join(list_store_layers(client, workspace, store_kind, store_name))

                    if not configured_path:
                        LOGGER.warning(
                            "Store %s/%s has no usable filesystem path",
                            workspace,
                            store_name,
                        )
                        if not workspace_excluded:
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
                        LOGGER.warning(
                            "Resolved path missing for store %s/%s: %s",
                            workspace,
                            store_name,
                            resolved_path,
                        )
                        if not workspace_excluded:
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

                    LOGGER.info(
                        "Scanned store %s/%s: %s file(s), %s GB",
                        workspace,
                        store_name,
                        scan.file_count,
                        bytes_to_gb(scan.size_bytes),
                    )
                    if not workspace_excluded:
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
                    LOGGER.warning(
                        "Failed to process workspace=%s kind=%s store=%s: %s",
                        workspace,
                        store_kind,
                        store_name,
                        exc,
                    )
                    if not workspace_excluded:
                        rows.append(
                            build_error_row(
                                workspace=workspace,
                                store_name=store_name,
                                store_type="",
                                status="error",
                                notes=str(exc),
                            )
                        )

    return rows, referenced_roots, referenced_files


def derive_output_html_path(output_csv: str, output_html: str) -> str:
    if output_html:
        return os.path.abspath(output_html)
    base, _ = os.path.splitext(os.path.abspath(output_csv))
    return base + ".html"


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

    fieldnames = [column[0] for column in HTML_COLUMNS]
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_html_summary(rows: Sequence[dict], excluded_workspaces: Sequence[str]) -> dict:
    store_rows = [row for row in rows if row["row_kind"] == "store"]
    orphan_rows = [row for row in rows if row["row_kind"] == "orphaned"]
    ok_rows = [row for row in store_rows if row["status"] == "ok"]
    issue_rows = [row for row in store_rows if row["status"] != "ok"]
    tracked_size_bytes = sum(int(row["size_bytes"]) for row in ok_rows)
    return {
        "store_count": len(store_rows),
        "orphan_count": len(orphan_rows),
        "issue_count": len(issue_rows),
        "tracked_size_gb": bytes_to_gb(tracked_size_bytes),
        "excluded_workspaces": ", ".join(excluded_workspaces) if excluded_workspaces else "None",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def render_html_table_rows(rows: Sequence[dict]) -> str:
    rendered_rows = []
    for row in rows:
        status_class = "status-" + str(row["status"]).lower()
        cells = []
        for key, _, sort_type in HTML_COLUMNS:
            value = row.get(key, "")
            display = html.escape(str(value))
            sort_value = str(value if sort_type == "number" else str(value).lower())
            cells.append(
                '<td data-key="{key}" data-value="{sort_value}">{display}</td>'.format(
                    key=html.escape(key),
                    sort_value=html.escape(sort_value),
                    display=display,
                )
            )
        rendered_rows.append('<tr class="report-row {}">{}</tr>'.format(status_class, "".join(cells)))
    return "\n".join(rendered_rows)


def write_html_report(
    path: str,
    rows: Sequence[dict],
    excluded_workspaces: Sequence[str],
    geoserver_url: str,
    data_dir: str,
) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent and not os.path.isdir(parent):
        os.makedirs(parent, exist_ok=True)

    summary = build_html_summary(rows, excluded_workspaces)
    header_cells = []
    for key, label, sort_type in HTML_COLUMNS:
        header_cells.append(
            '<th class="sortable" data-key="{key}" data-type="{sort_type}">{label}<span class="sort-indicator"></span></th>'.format(
                key=html.escape(key),
                sort_type=html.escape(sort_type),
                label=html.escape(label),
            )
        )

    html_text = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GeoServer Store Report</title>
  <style>
    :root {{
      --paper: #f6f1e8;
      --ink: #1e2a32;
      --muted: #5d6b75;
      --line: #dccfbe;
      --panel: #fffdf8;
      --accent: #a2471b;
      --ok: #e2f1e8;
      --warn: #fff1d6;
      --error: #fde3e1;
      --orphan: #f3e7ff;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", "Trebuchet MS", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(162, 71, 27, 0.14), transparent 24rem),
        linear-gradient(180deg, #f9f5ee 0%, var(--paper) 100%);
    }}
    .page {{ max-width: 1480px; margin: 0 auto; padding: 2rem 1.5rem 3rem; }}
    .hero {{
      display: grid;
      gap: 1rem;
      padding: 1.5rem;
      border: 1px solid var(--line);
      border-radius: 1.25rem;
      background: linear-gradient(135deg, rgba(255,255,255,0.95), rgba(245,236,222,0.92));
      box-shadow: 0 18px 45px rgba(40, 37, 30, 0.08);
    }}
    .hero h1 {{ margin: 0; font-size: clamp(1.8rem, 2.8vw, 3rem); letter-spacing: 0.02em; }}
    .hero p {{ margin: 0; color: var(--muted); max-width: 72rem; line-height: 1.55; }}
    .summary-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.9rem;
      margin-top: 1rem;
    }}
    .summary-card {{
      padding: 1rem;
      border-radius: 1rem;
      border: 1px solid var(--line);
      background: var(--panel);
    }}
    .summary-card .label {{
      font-size: 0.82rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}
    .summary-card .value {{ margin-top: 0.35rem; font-size: 1.45rem; font-weight: 700; }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 0.9rem;
      margin-top: 1.1rem;
    }}
    .meta-card {{
      padding: 1rem;
      border-radius: 1rem;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.78);
    }}
    .meta-card strong {{ display: block; margin-bottom: 0.35rem; }}
    .toolbar {{
      display: flex;
      gap: 1rem;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      margin: 1.5rem 0 1rem;
    }}
    .toolbar .hint {{ color: var(--muted); font-size: 0.95rem; }}
    .search {{
      min-width: min(28rem, 100%);
      padding: 0.85rem 1rem;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.85);
      font: inherit;
    }}
    .table-shell {{
      overflow: auto;
      border-radius: 1rem;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.88);
      box-shadow: 0 14px 32px rgba(30, 42, 50, 0.08);
    }}
    table {{ width: 100%; border-collapse: collapse; min-width: 1200px; }}
    thead th {{
      position: sticky;
      top: 0;
      z-index: 1;
      padding: 0.95rem 0.9rem;
      background: #eadfce;
      text-align: left;
      font-size: 0.88rem;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
      user-select: none;
      white-space: nowrap;
    }}
    tbody td {{
      padding: 0.82rem 0.9rem;
      vertical-align: top;
      border-bottom: 1px solid rgba(220, 207, 190, 0.65);
      font-size: 0.92rem;
      line-height: 1.45;
      word-break: break-word;
    }}
    tbody tr:nth-child(even) {{ background: rgba(250, 246, 239, 0.6); }}
    tbody tr.status-ok {{ background: var(--ok); }}
    tbody tr.status-missing,
    tbody tr.status-unresolved {{ background: var(--warn); }}
    tbody tr.status-error {{ background: var(--error); }}
    tbody tr.status-orphaned {{ background: var(--orphan); }}
    .sort-indicator {{ display: inline-block; width: 0.9rem; margin-left: 0.35rem; color: var(--accent); }}
    th[data-direction="asc"] .sort-indicator::after {{ content: "↑"; }}
    th[data-direction="desc"] .sort-indicator::after {{ content: "↓"; }}
    .empty-state {{ padding: 1rem 0; color: var(--muted); }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <div>
        <h1>GeoServer Store Report</h1>
        <p>Sortable inventory of included GeoServer stores and orphaned disk usage. Click any column header to sort, and use the filter box to narrow the table during review.</p>
      </div>
      <div class="summary-grid">
        <div class="summary-card"><div class="label">Store Rows</div><div class="value">{store_count}</div></div>
        <div class="summary-card"><div class="label">Orphan Rows</div><div class="value">{orphan_count}</div></div>
        <div class="summary-card"><div class="label">Store Issues</div><div class="value">{issue_count}</div></div>
        <div class="summary-card"><div class="label">Tracked Size</div><div class="value">{tracked_size_gb} GB</div></div>
      </div>
      <div class="meta">
        <div class="meta-card"><strong>GeoServer URL</strong><span>{geoserver_url}</span></div>
        <div class="meta-card"><strong>Data Directory</strong><span>{data_dir}</span></div>
        <div class="meta-card"><strong>Excluded Workspaces</strong><span>{excluded_workspaces}</span></div>
        <div class="meta-card"><strong>Generated</strong><span>{generated_at}</span></div>
      </div>
    </section>
    <div class="toolbar">
      <div class="hint">Statuses are color coded: green for scanned stores, amber for unresolved or missing paths, red for failures, and violet for orphaned data.</div>
      <input id="rowFilter" class="search" type="search" placeholder="Filter rows by workspace, store, path, status, or notes">
    </div>
    <div class="table-shell">
      <table id="reportTable">
        <thead><tr>{header_cells}</tr></thead>
        <tbody>{table_rows}</tbody>
      </table>
    </div>
    <p id="emptyState" class="empty-state" hidden>No rows match the current filter.</p>
  </div>
  <script>
    const table = document.getElementById("reportTable");
    const tbody = table.querySelector("tbody");
    const headers = Array.from(table.querySelectorAll("th.sortable"));
    const filterInput = document.getElementById("rowFilter");
    const emptyState = document.getElementById("emptyState");
    function normalizeValue(value, type) {{
      if (type === "number") {{
        const parsed = Number(value);
        return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
      }}
      return String(value).toLowerCase();
    }}
    function updateEmptyState() {{
      const visibleRows = Array.from(tbody.querySelectorAll("tr")).filter(row => !row.hidden);
      emptyState.hidden = visibleRows.length !== 0;
    }}
    function sortBy(header) {{
      const key = header.dataset.key;
      const type = header.dataset.type;
      const current = header.dataset.direction === "asc" ? "desc" : "asc";
      headers.forEach(item => delete item.dataset.direction);
      header.dataset.direction = current;
      const rows = Array.from(tbody.querySelectorAll("tr"));
      rows.sort((left, right) => {{
        const leftCell = left.querySelector(`[data-key="${{key}}"]`);
        const rightCell = right.querySelector(`[data-key="${{key}}"]`);
        const leftValue = normalizeValue(leftCell ? leftCell.dataset.value : "", type);
        const rightValue = normalizeValue(rightCell ? rightCell.dataset.value : "", type);
        if (leftValue < rightValue) return current === "asc" ? -1 : 1;
        if (leftValue > rightValue) return current === "asc" ? 1 : -1;
        return 0;
      }});
      for (const row of rows) tbody.appendChild(row);
    }}
    function applyFilter() {{
      const query = filterInput.value.trim().toLowerCase();
      for (const row of tbody.querySelectorAll("tr")) {{
        row.hidden = query && !row.innerText.toLowerCase().includes(query);
      }}
      updateEmptyState();
    }}
    headers.forEach(header => header.addEventListener("click", () => sortBy(header)));
    filterInput.addEventListener("input", applyFilter);
    updateEmptyState();
  </script>
</body>
</html>
""".format(
        store_count=summary["store_count"],
        orphan_count=summary["orphan_count"],
        issue_count=summary["issue_count"],
        tracked_size_gb=summary["tracked_size_gb"],
        geoserver_url=html.escape(geoserver_url),
        data_dir=html.escape(os.path.abspath(data_dir)),
        excluded_workspaces=html.escape(summary["excluded_workspaces"]),
        generated_at=html.escape(summary["generated_at"]),
        header_cells="".join(header_cells),
        table_rows=render_html_table_rows(rows),
    )

    with open(path, "w", encoding="utf-8") as handle:
        handle.write(html_text)


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
        "--output-html",
        default="",
        help="Optional HTML output path. Defaults to the CSV path with .html extension.",
    )
    parser.add_argument(
        "--exclude-workspaces",
        default="",
        help="Optional comma-separated list of workspaces to exclude from report rows and orphan detection.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level, for example DEBUG, INFO, WARNING, or ERROR.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable SSL certificate verification for HTTPS GeoServer URLs.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    validate_args(args)

    data_dir = os.path.abspath(args.data_dir)
    data_root = os.path.join(data_dir, "data")
    if not os.path.isdir(data_root):
        raise SystemExit("GeoServer data path does not exist: {}".format(data_root))

    excluded_workspaces = sorted(parse_excluded_workspaces(args.exclude_workspaces))
    output_html = derive_output_html_path(args.output_csv, args.output_html)

    LOGGER.info("Starting GeoServer report generation")
    LOGGER.info("GeoServer URL: %s", args.geoserver_url)
    LOGGER.info("Data directory: %s", data_dir)
    LOGGER.info("CSV output: %s", os.path.abspath(args.output_csv))
    LOGGER.info("HTML output: %s", output_html)
    LOGGER.info(
        "Excluded workspaces: %s",
        ", ".join(excluded_workspaces) if excluded_workspaces else "None",
    )

    client = GeoServerClient(
        base_url=args.geoserver_url,
        username=args.username,
        password=args.password,
        timeout=args.timeout,
        insecure=args.insecure,
    )

    store_rows, referenced_roots, referenced_files = inventory_stores(
        client,
        data_dir,
        set(excluded_workspaces),
    )
    LOGGER.info("Collected %d store row(s)", len(store_rows))

    orphan_rows = collect_orphans(data_root, referenced_roots, referenced_files)
    LOGGER.info("Collected %d orphan row(s)", len(orphan_rows))

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

    LOGGER.info("Writing CSV report")
    write_csv(args.output_csv, rows)
    LOGGER.info("Writing HTML report")
    write_html_report(
        output_html,
        rows,
        excluded_workspaces,
        args.geoserver_url,
        data_dir,
    )

    store_count = sum(1 for row in store_rows if row["row_kind"] == "store")
    orphan_count = sum(1 for row in orphan_rows if row["row_kind"] == "orphaned")
    LOGGER.info("Report generation complete")
    print(
        "Wrote {} store rows and {} orphan rows to {} and {}".format(
            store_count,
            orphan_count,
            args.output_csv,
            output_html,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
