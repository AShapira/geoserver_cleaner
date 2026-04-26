"""Core GeoServer inventory and orphan-detection utilities."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
import html
import io
import json
import logging
import ntpath
import os
import posixpath
import re
import ssl
import sys
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urljoin, urlparse
from urllib.request import (
    HTTPBasicAuthHandler,
    HTTPPasswordMgrWithDefaultRealm,
    HTTPSHandler,
    Request,
    build_opener,
)
from xml.etree import ElementTree


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

DEFAULT_ORPHAN_SMALL_FILE_THRESHOLD_BYTES = 100 * 1024

LOGGER = logging.getLogger("geoserver_cleaner.reporting")

WINDOWS_ABSOLUTE_RE = re.compile(r"^[a-zA-Z]:[\\/]")
WINDOWS_DRIVE_WITH_SLASHES_RE = re.compile(r"^[/\\]+[a-zA-Z]:")

@dataclass
class ScanResult:
    size_bytes: int
    file_count: int
    referenced_files: Set[str]


@dataclass
class CatalogStore:
    workspace: str
    store_name: str
    store_kind: str
    store_type: str
    configured_path: str
    layer_names: str
    status: str = "ok"
    notes: str = ""


@dataclass(frozen=True)
class ExternalPathMapping:
    geoserver_root: str
    cleaner_root: str
    geoserver_style: str
    geoserver_root_normalized: str
    cleaner_root_normalized: str


@dataclass(frozen=True)
class ResolvedStorePath:
    resolved_path: str
    mapping: Optional[ExternalPathMapping] = None


@dataclass
class ProcessedStore:
    row: dict
    referenced_root: str = ""
    referenced_files: Set[str] = field(default_factory=set)


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def normalize_path(path: str) -> str:
    return os.path.normcase(os.path.normpath(path))


def parse_external_path_mappings(raw: str) -> Tuple[ExternalPathMapping, ...]:
    if raw is None or not raw.strip():
        return ()

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("GEOSERVER_EXTERNAL_PATH_MAPPINGS must be valid JSON.") from exc
    if not isinstance(payload, dict):
        raise ValueError("GEOSERVER_EXTERNAL_PATH_MAPPINGS must be a JSON object.")

    mappings: List[ExternalPathMapping] = []
    for geoserver_root, cleaner_root in payload.items():
        source = str(geoserver_root or "").strip()
        target = str(cleaner_root or "").strip()
        if not source or not target:
            raise ValueError("GEOSERVER_EXTERNAL_PATH_MAPPINGS entries must use non-empty roots.")
        source_style = detect_absolute_path_style(source)
        if source_style is None:
            raise ValueError(
                "GEOSERVER_EXTERNAL_PATH_MAPPINGS source roots must be absolute paths: {!r}".format(source)
            )
        if not os.path.isabs(target):
            raise ValueError(
                "GEOSERVER_EXTERNAL_PATH_MAPPINGS cleaner roots must be absolute paths for this runtime: {!r}".format(
                    target
                )
            )
        cleaner_root = os.path.abspath(target)
        mappings.append(
            ExternalPathMapping(
                geoserver_root=source,
                cleaner_root=cleaner_root,
                geoserver_style=source_style,
                geoserver_root_normalized=normalize_style_path(source, source_style),
                cleaner_root_normalized=normalize_path(cleaner_root),
            )
        )

    mappings.sort(key=lambda item: len(item.geoserver_root_normalized), reverse=True)
    return tuple(mappings)


def detect_absolute_path_style(path: str) -> Optional[str]:
    value = str(path or "").strip()
    if not value:
        return None
    if WINDOWS_DRIVE_WITH_SLASHES_RE.match(value):
        return "windows"
    if WINDOWS_ABSOLUTE_RE.match(value):
        return "windows"
    if value.startswith("\\\\") or value.startswith("//"):
        return "windows"
    if value.startswith("/"):
        return "posix"
    return None


def normalize_style_path(path: str, style: str) -> str:
    if style == "windows":
        value = path.replace("/", "\\")
        if WINDOWS_DRIVE_WITH_SLASHES_RE.match(value):
            value = value.lstrip("/\\")
        return ntpath.normcase(ntpath.normpath(value))
    if style == "posix":
        value = path.replace("\\", "/")
        if value.startswith("//"):
            value = "/" + value.lstrip("/")
        return posixpath.normpath(value)
    raise ValueError("Unsupported path style: {}".format(style))


def _path_starts_with_root(path: str, root: str, style: str) -> bool:
    separator = "\\" if style == "windows" else "/"
    return path == root or path.startswith(root + separator)


def _relative_from_root(path: str, root: str, style: str) -> str:
    if path == root:
        return ""
    separator_chars = "\\/" if style == "windows" else "/"
    return path[len(root) :].lstrip(separator_chars)


def _split_local_segments(value: str) -> List[str]:
    return [segment for segment in re.split(r"[\\/]+", value or "") if segment]


def _join_local_root(root: str, relative_path: str) -> str:
    result = root
    for segment in _split_local_segments(relative_path):
        result = os.path.join(result, segment)
    return os.path.abspath(result)


def inspect_external_root_accessibility(
    external_path_mappings: Sequence[ExternalPathMapping],
) -> Dict[str, bool]:
    accessibility: Dict[str, bool] = {}
    for mapping in external_path_mappings:
        if mapping.cleaner_root_normalized in accessibility:
            continue
        try:
            is_accessible = os.path.isdir(mapping.cleaner_root) and os.access(
                mapping.cleaner_root,
                os.R_OK | os.X_OK,
            )
        except OSError:
            is_accessible = False
        accessibility[mapping.cleaner_root_normalized] = is_accessible
    return accessibility


def _decode_configured_path(configured_path: str) -> Tuple[str, bool]:
    raw = unquote(str(configured_path or "").strip())
    if not raw:
        return "", False
    if not raw.lower().startswith("file:"):
        return raw, False

    parsed = urlparse(raw)
    netloc = unquote(parsed.netloc or "")
    path = unquote(parsed.path or "")
    if netloc:
        if re.match(r"^[a-zA-Z]:$", netloc):
            return netloc + path, True
        return "//{}{}".format(netloc, path), True
    if not path:
        path = raw[5:]
    if WINDOWS_DRIVE_WITH_SLASHES_RE.match(path):
        path = path.lstrip("/\\")
    return path, True


def _is_data_relative_path(path: str) -> bool:
    normalized = str(path or "").strip().replace("\\", "/").strip("/")
    lowered = normalized.lower()
    return lowered == "data" or lowered.startswith("data/")


def _apply_external_path_mapping(
    external_path: str,
    external_path_mappings: Sequence[ExternalPathMapping],
) -> Optional[ResolvedStorePath]:
    source_style = detect_absolute_path_style(external_path)
    if source_style is None:
        return None
    normalized_source = normalize_style_path(external_path, source_style)
    for mapping in external_path_mappings:
        if mapping.geoserver_style != source_style:
            continue
        if not _path_starts_with_root(normalized_source, mapping.geoserver_root_normalized, source_style):
            continue
        relative_path = _relative_from_root(
            normalized_source,
            mapping.geoserver_root_normalized,
            source_style,
        )
        return ResolvedStorePath(
            resolved_path=_join_local_root(mapping.cleaner_root, relative_path),
            mapping=mapping,
        )
    return None


def bytes_to_gb(size_bytes: int) -> str:
    return "{:.2f}".format(size_bytes / (1024.0 ** 3))


def as_list(value) -> List[dict]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def join_notes(*parts: str) -> str:
    return " ".join(str(part).strip() for part in parts if str(part or "").strip())


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


def worker_default() -> int:
    cpu_count = os.cpu_count() or 4
    return max(4, min(16, cpu_count * 2))


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
    container = payload.get(root_key)
    if not isinstance(container, dict):
        return []
    items = container.get(item_key)
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


def list_layer_group_refs(client: GeoServerClient, workspace: str = "") -> List[str]:
    if workspace:
        endpoint = "rest/workspaces/{}/layergroups.json".format(quote(workspace, safe=""))
    else:
        endpoint = "rest/layergroups.json"
    payload = client.get_json(endpoint)
    container = payload.get("layerGroups")
    if not isinstance(container, dict):
        return []
    items = container.get("layerGroup")
    names = []
    for item in as_list(items):
        if isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return sorted(set(names))


def get_layer_group_detail(client: GeoServerClient, group_name: str, workspace: str = "") -> dict:
    group_q = quote(group_name, safe="")
    if workspace:
        endpoint = "rest/workspaces/{}/layergroups/{}.json".format(
            quote(workspace, safe=""),
            group_q,
        )
    else:
        endpoint = "rest/layergroups/{}.json".format(group_q)
    payload = client.get_json(endpoint)
    detail = payload.get("layerGroup", {})
    if not isinstance(detail, dict):
        raise RuntimeError("Unexpected layer group payload type for {}".format(group_name))
    return detail


def _layer_group_layer_names(detail: dict) -> List[str]:
    layers = detail.get("layers")
    if not isinstance(layers, dict):
        return []
    names: List[str] = []
    for item in as_list(layers.get("layer")):
        if isinstance(item, str):
            if item.strip():
                names.append(item.strip())
        elif isinstance(item, dict):
            value = item.get("name") or item.get("$") or item.get("#text")
            if value:
                names.append(str(value).strip())
    return sorted(set(name for name in names if name))


def collect_layer_group_memberships(
    client: Optional[GeoServerClient],
    workspaces: Sequence[str],
) -> Dict[str, List[str]]:
    if client is None:
        return {}

    memberships: Dict[str, Set[str]] = {}

    def add_group(group_label: str, detail: dict) -> None:
        for layer_name in _layer_group_layer_names(detail):
            memberships.setdefault(layer_name, set()).add(group_label)
            if ":" in layer_name:
                memberships.setdefault(layer_name.split(":", 1)[1], set()).add(group_label)

    try:
        for group_name in list_layer_group_refs(client):
            add_group(group_name, get_layer_group_detail(client, group_name))
        for workspace in sorted(set(workspaces), key=lambda value: value.lower()):
            for group_name in list_layer_group_refs(client, workspace):
                add_group(
                    "{}/{}".format(workspace, group_name),
                    get_layer_group_detail(client, group_name, workspace),
                )
    except Exception as exc:
        LOGGER.warning("Failed to collect layer group memberships: %s", exc)
        return {}

    return {layer_name: sorted(groups) for layer_name, groups in memberships.items()}


def append_store_layer_group_warnings(
    catalog_stores: Sequence[CatalogStore],
    layer_group_memberships: Dict[str, List[str]],
) -> None:
    if not layer_group_memberships:
        return
    for catalog_store in catalog_stores:
        layer_names = [
            item.strip()
            for item in (catalog_store.layer_names or "").split(",")
            if item.strip()
        ]
        group_names: Set[str] = set()
        for layer_name in layer_names:
            group_names.update(layer_group_memberships.get(layer_name, []))
            group_names.update(layer_group_memberships.get("{}:{}".format(catalog_store.workspace, layer_name), []))
        if not group_names:
            continue
        warning = "Warning: layer(s) are used by layer group(s): {}.".format(
            ", ".join(sorted(group_names, key=lambda value: value.lower()))
        )
        catalog_store.notes = join_notes(catalog_store.notes, warning)


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

    container = payload.get(root_key)
    if not isinstance(container, dict):
        return []
    items = container.get(item_key)
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


def resolve_store_path(
    configured_path: str,
    data_dir: str,
    external_path_mappings: Sequence[ExternalPathMapping] = (),
) -> ResolvedStorePath:
    if not configured_path:
        return ResolvedStorePath("")

    data_dir = os.path.abspath(data_dir)
    value, _used_file_scheme = _decode_configured_path(configured_path)
    absolute_style = detect_absolute_path_style(value)
    if absolute_style is not None:
        mapped = _apply_external_path_mapping(value, external_path_mappings)
        if mapped is not None:
            return mapped
        return ResolvedStorePath(value)

    if _is_data_relative_path(value):
        relative = value[4:].lstrip("/\\")
        return ResolvedStorePath(_join_local_root(os.path.join(data_dir, "data"), relative))

    return ResolvedStorePath(_join_local_root(data_dir, value))


def parse_xml_file(path: str) -> ElementTree.Element:
    try:
        tree = ElementTree.parse(path)
    except (OSError, ElementTree.ParseError) as exc:
        raise RuntimeError("Failed to parse XML {}: {}".format(path, exc)) from exc
    return tree.getroot()


def xml_text(element: Optional[ElementTree.Element], tag_name: str, default: str = "") -> str:
    if element is None:
        return default
    value = element.findtext(tag_name)
    if value is None:
        return default
    return str(value).strip()


def xml_connection_parameters(root: ElementTree.Element) -> Dict[str, str]:
    params: Dict[str, str] = {}
    connection_parameters = root.find("connectionParameters")
    if connection_parameters is None:
        return params
    for entry in connection_parameters.findall("entry"):
        key = (entry.get("key") or "").strip()
        if not key:
            continue
        value = "".join(entry.itertext()).strip()
        params[key] = value
    return params


def extract_datastore_path_from_params(params: Dict[str, str]) -> str:
    for key in ("url", "database", "file", "path"):
        value = (params.get(key) or "").strip()
        if value:
            return value

    candidates = []
    for value in params.values():
        candidate = (value or "").strip()
        if not candidate:
            continue
        lower = candidate.lower()
        if lower.startswith("file:") or re.match(r"^[a-z]:[\\/]", candidate, re.I) or candidate.startswith("\\\\"):
            candidates.append(candidate)
    return candidates[0] if candidates else ""


def collect_layer_names_from_store_dir(store_dir: str, layer_file_name: str, fallback_name: str) -> str:
    layer_names: List[str] = []
    try:
        entries = sorted(os.scandir(store_dir), key=lambda entry: entry.name.lower())
    except OSError as exc:
        raise RuntimeError("Failed to inspect store directory {}: {}".format(store_dir, exc)) from exc

    for entry in entries:
        if not entry.is_dir():
            continue
        layer_xml_path = os.path.join(entry.path, layer_file_name)
        if not os.path.isfile(layer_xml_path):
            continue
        try:
            layer_root = parse_xml_file(layer_xml_path)
            layer_name = xml_text(layer_root, "name", entry.name)
        except RuntimeError:
            layer_name = entry.name
        if layer_name:
            layer_names.append(layer_name)

    if not layer_names and fallback_name:
        layer_names.append(fallback_name)
    return ", ".join(sorted(set(layer_names)))


def read_catalog_store(store_dir: str, workspace: str) -> CatalogStore:
    datastore_xml = os.path.join(store_dir, "datastore.xml")
    coveragestore_xml = os.path.join(store_dir, "coveragestore.xml")
    store_name = os.path.basename(store_dir)

    if os.path.isfile(datastore_xml):
        root = parse_xml_file(datastore_xml)
        parsed_store_name = xml_text(root, "name", store_name) or store_name
        store_type = xml_text(root, "type")
        configured_path = extract_datastore_path_from_params(xml_connection_parameters(root))
        layer_names = collect_layer_names_from_store_dir(store_dir, "featuretype.xml", parsed_store_name)
        return CatalogStore(
            workspace=workspace,
            store_name=parsed_store_name,
            store_kind="datastores",
            store_type=store_type,
            configured_path=configured_path,
            layer_names=layer_names,
        )

    if os.path.isfile(coveragestore_xml):
        root = parse_xml_file(coveragestore_xml)
        parsed_store_name = xml_text(root, "name", store_name) or store_name
        store_type = xml_text(root, "type")
        configured_path = xml_text(root, "url")
        layer_names = collect_layer_names_from_store_dir(store_dir, "coverage.xml", parsed_store_name)
        if not layer_names and store_type.lower() == "geotiff":
            layer_names = parsed_store_name
        return CatalogStore(
            workspace=workspace,
            store_name=parsed_store_name,
            store_kind="coveragestores",
            store_type=store_type,
            configured_path=configured_path,
            layer_names=layer_names,
        )

    raise RuntimeError("No datastore.xml or coveragestore.xml found in {}".format(store_dir))


def list_catalog_workspaces(
    data_dir: str,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> Tuple[List[str], List[CatalogStore]]:
    workspaces_root = os.path.join(data_dir, "workspaces")
    if not os.path.isdir(workspaces_root):
        raise RuntimeError("GeoServer workspaces directory does not exist: {}".format(workspaces_root))

    workspace_names: List[str] = []
    catalog_stores: List[CatalogStore] = []
    discovered_count = 0

    for entry in sorted(os.scandir(workspaces_root), key=lambda item: item.name.lower()):
        if not entry.is_dir():
            continue
        workspace_dir = entry.path
        workspace_xml = os.path.join(workspace_dir, "workspace.xml")
        workspace = entry.name
        if os.path.isfile(workspace_xml):
            try:
                workspace_root = parse_xml_file(workspace_xml)
                workspace = xml_text(workspace_root, "name", entry.name) or entry.name
            except RuntimeError as exc:
                LOGGER.warning("Failed to parse workspace XML %s: %s", workspace_xml, exc)
        workspace_names.append(workspace)

        for store_entry in sorted(os.scandir(workspace_dir), key=lambda item: item.name.lower()):
            if not store_entry.is_dir():
                continue
            store_dir = store_entry.path
            if not (
                os.path.isfile(os.path.join(store_dir, "datastore.xml"))
                or os.path.isfile(os.path.join(store_dir, "coveragestore.xml"))
            ):
                continue
            try:
                catalog_stores.append(read_catalog_store(store_dir, workspace))
            except Exception as exc:
                catalog_stores.append(
                    CatalogStore(
                        workspace=workspace,
                        store_name=store_entry.name,
                        store_kind="",
                        store_type="",
                        configured_path="",
                        layer_names="",
                        status="error",
                        notes=str(exc),
                    )
                )
            discovered_count += 1
            if progress_callback is not None:
                progress_callback(discovered_count, workspace)

    return workspace_names, catalog_stores


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
    small_file_threshold_bytes: int = DEFAULT_ORPHAN_SMALL_FILE_THRESHOLD_BYTES,
) -> List[dict]:
    orphan_rows: List[dict] = []
    referenced_file_set = {normalize_path(item) for item in referenced_files}
    normalized_roots = [normalize_path(item) for item in referenced_roots]
    small_file_threshold_bytes = max(int(small_file_threshold_bytes or 0), 0)

    def should_emit_directory(size_bytes: int, file_count: int) -> bool:
        return size_bytes > 0 and file_count > 0

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
                elif should_emit_directory(child_size, child_count):
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
                if stat.st_size < small_file_threshold_bytes:
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
            if not has_referenced and should_emit_directory(total_size, total_count):
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
                if stat.st_size < small_file_threshold_bytes:
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


def collect_rest_catalog(
    client: Optional[GeoServerClient],
    data_dir: str,
    progress_callback: Optional[Callable[[int, str], None]] = None,
) -> Tuple[List[str], List[CatalogStore], List[dict]]:
    workspace_names = list_workspaces(client)
    LOGGER.info("Discovered %d workspace(s) via REST", len(workspace_names))
    catalog_stores: List[CatalogStore] = []
    error_rows: List[dict] = []
    discovered_count = 0

    for workspace in workspace_names:
        for store_kind in ("coveragestores", "datastores"):
            try:
                store_names = list_store_refs(client, workspace, store_kind)
                LOGGER.info(
                    "Workspace %s: discovered %d %s via REST",
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
                error_rows.append(
                    build_error_row(
                        workspace=workspace,
                        store_name="",
                        status="error",
                        notes="Failed to list {}: {}".format(store_kind, exc),
                    )
                )
                continue

            for store_name in store_names:
                try:
                    detail = get_store_detail(client, workspace, store_kind, store_name)
                    store_type = str(detail.get("type", "")).strip()
                    configured_path = extract_store_path(detail, store_kind)
                    if store_kind == "coveragestores" and store_type.lower() == "geotiff":
                        layer_names = store_name
                    else:
                        layer_names = ", ".join(list_store_layers(client, workspace, store_kind, store_name))
                    catalog_stores.append(
                        CatalogStore(
                            workspace=workspace,
                            store_name=store_name,
                            store_kind=store_kind,
                            store_type=store_type,
                            configured_path=configured_path,
                            layer_names=layer_names,
                        )
                    )
                    discovered_count += 1
                    if progress_callback is not None:
                        progress_callback(discovered_count, workspace)
                except Exception as exc:
                    LOGGER.warning(
                        "Failed to collect REST metadata for workspace=%s kind=%s store=%s: %s",
                        workspace,
                        store_kind,
                        store_name,
                        exc,
                    )
                    error_rows.append(
                        build_error_row(
                            workspace=workspace,
                            store_name=store_name,
                            store_type="",
                            status="error",
                            notes=str(exc),
                        )
                    )

    return workspace_names, catalog_stores, error_rows


def process_catalog_store(
    catalog_store: CatalogStore,
    data_dir: str,
    external_path_mappings: Sequence[ExternalPathMapping] = (),
    external_root_accessibility: Optional[Dict[str, bool]] = None,
) -> ProcessedStore:
    if catalog_store.status != "ok":
        return ProcessedStore(
            row=build_error_row(
                workspace=catalog_store.workspace,
                store_name=catalog_store.store_name,
                store_type=catalog_store.store_type,
                status=catalog_store.status,
                notes=catalog_store.notes,
            )
        )

    configured_path = catalog_store.configured_path
    resolved = resolve_store_path(configured_path, data_dir, external_path_mappings=external_path_mappings)
    resolved_path = resolved.resolved_path

    if not configured_path:
        return ProcessedStore(
            row=build_row(
                row_kind="store",
                workspace=catalog_store.workspace,
                store_name=catalog_store.store_name,
                store_type=catalog_store.store_type,
                layer_names=catalog_store.layer_names,
                configured_path="",
                resolved_path="",
                path_kind="",
                size_bytes=0,
                file_count=0,
                status="unresolved",
                notes=join_notes(
                    "Could not find a usable filesystem path in store configuration.",
                    catalog_store.notes,
                ),
            )
        )

    if (
        resolved.mapping is not None
        and external_root_accessibility is not None
        and not external_root_accessibility.get(resolved.mapping.cleaner_root_normalized, False)
    ):
        return ProcessedStore(
            row=build_row(
                row_kind="store",
                workspace=catalog_store.workspace,
                store_name=catalog_store.store_name,
                store_type=catalog_store.store_type,
                layer_names=catalog_store.layer_names,
                configured_path=configured_path,
                resolved_path=resolved_path,
                path_kind="missing",
                size_bytes=0,
                file_count=0,
                status="missing",
                notes=join_notes(
                    "Mapped external root '{}' is not accessible from the current runtime.".format(
                        resolved.mapping.cleaner_root
                    ),
                    catalog_store.notes,
                ),
            )
        )

    if not os.path.exists(resolved_path):
        return ProcessedStore(
            row=build_row(
                row_kind="store",
                workspace=catalog_store.workspace,
                store_name=catalog_store.store_name,
                store_type=catalog_store.store_type,
                layer_names=catalog_store.layer_names,
                configured_path=configured_path,
                resolved_path=resolved_path,
                path_kind="missing",
                size_bytes=0,
                file_count=0,
                status="missing",
                notes=join_notes("Resolved path does not exist on disk.", catalog_store.notes),
            )
        )

    scan = scan_any_path(resolved_path, catalog_store.store_type)
    path_kind = "directory" if os.path.isdir(resolved_path) else "file"
    return ProcessedStore(
        row=build_row(
            row_kind="store",
            workspace=catalog_store.workspace,
            store_name=catalog_store.store_name,
            store_type=catalog_store.store_type,
            layer_names=catalog_store.layer_names,
            configured_path=configured_path,
            resolved_path=resolved_path,
            path_kind=path_kind,
            size_bytes=scan.size_bytes,
            file_count=scan.file_count,
            status="ok",
            notes=catalog_store.notes,
        ),
        referenced_root=normalize_path(resolved_path) if path_kind == "directory" else "",
        referenced_files=scan.referenced_files if path_kind == "file" else set(),
    )


def inventory_stores(
    client: Optional[GeoServerClient],
    data_dir: str,
    excluded_workspaces: Set[str],
    catalog_source: str = "auto",
    workers: Optional[int] = None,
    external_path_mappings: Sequence[ExternalPathMapping] = (),
) -> Tuple[List[dict], List[str], Set[str]]:
    rows: List[dict] = []
    referenced_roots: List[str] = []
    referenced_files: Set[str] = set()
    external_root_accessibility = inspect_external_root_accessibility(external_path_mappings)

    catalog_source_normalized = (catalog_source or "auto").lower()
    if catalog_source_normalized not in {"auto", "filesystem", "rest"}:
        raise RuntimeError("Unsupported catalog source: {}".format(catalog_source))

    workspace_names: List[str]
    catalog_stores: List[CatalogStore]
    if catalog_source_normalized in {"auto", "filesystem"}:
        try:
            workspace_names, catalog_stores = list_catalog_workspaces(data_dir)
            LOGGER.info(
                "Discovered %d workspace(s) and %d store(s) via filesystem catalog",
                len(workspace_names),
                len(catalog_stores),
            )
        except Exception as exc:
            if catalog_source_normalized == "filesystem":
                raise
            LOGGER.warning("Filesystem catalog discovery failed, falling back to REST: %s", exc)
            workspace_names, catalog_stores, rows = collect_rest_catalog(client, data_dir)
        else:
            rows = []
    else:
        workspace_names, catalog_stores, rows = collect_rest_catalog(client, data_dir)

    append_store_layer_group_warnings(
        catalog_stores,
        collect_layer_group_memberships(client, workspace_names),
    )

    for workspace in workspace_names:
        if workspace.lower() in excluded_workspaces:
            LOGGER.info("Workspace %s is excluded from report rows", workspace)
            fallback_root = os.path.join(data_dir, "data", workspace)
            if os.path.isdir(fallback_root):
                referenced_roots.append(normalize_path(fallback_root))

    included_stores = [
        catalog_store
        for catalog_store in catalog_stores
        if catalog_store.workspace.lower() not in excluded_workspaces
    ]
    skipped_stores = len(catalog_stores) - len(included_stores)
    if skipped_stores:
        LOGGER.info("Skipped %d store(s) because their workspace is excluded", skipped_stores)

    max_workers = workers or worker_default()
    completed = 0
    log_interval = 500 if len(included_stores) >= 1000 else 100
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                process_catalog_store,
                catalog_store,
                data_dir,
                external_path_mappings,
                external_root_accessibility,
            ): catalog_store
            for catalog_store in included_stores
        }
        for future in as_completed(future_map):
            catalog_store = future_map[future]
            try:
                processed = future.result()
            except Exception as exc:
                LOGGER.warning(
                    "Failed to process workspace=%s kind=%s store=%s: %s",
                    catalog_store.workspace,
                    catalog_store.store_kind,
                    catalog_store.store_name,
                    exc,
                )
                rows.append(
                    build_error_row(
                        workspace=catalog_store.workspace,
                        store_name=catalog_store.store_name,
                        store_type=catalog_store.store_type,
                        status="error",
                        notes=str(exc),
                    )
                )
            else:
                rows.append(processed.row)
                if processed.referenced_root:
                    referenced_roots.append(processed.referenced_root)
                if processed.referenced_files:
                    referenced_files.update(processed.referenced_files)
            completed += 1
            if completed == 1 or completed % log_interval == 0 or completed == len(included_stores):
                LOGGER.info("Processed %d/%d store(s)", completed, len(included_stores))

    return rows, referenced_roots, referenced_files
