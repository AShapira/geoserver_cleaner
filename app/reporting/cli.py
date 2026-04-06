"""Standalone report-generation CLI for GeoServer Cleaner."""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional, Sequence

from app.reporting.core import (
    GeoServerClient,
    collect_orphans,
    configure_logging,
    inventory_stores,
    normalize_path,
    parse_excluded_workspaces,
    worker_default,
)
from app.reporting.render import write_html_report, write_csv


def derive_output_html_path(output_csv: str, output_html: str) -> str:
    if output_html:
        return os.path.abspath(output_html)
    base, _ = os.path.splitext(os.path.abspath(output_csv))
    return base + ".html"


def validate_args(args: argparse.Namespace, catalog_source: str) -> None:
    missing = []
    if catalog_source == "rest" and not args.geoserver_url:
        missing.append("--geoserver-url")
    if catalog_source == "rest" and not args.password:
        missing.append("--password")
    if not args.data_dir:
        missing.append("--data-dir")
    if missing:
        raise SystemExit("Missing required arguments: {}".format(", ".join(missing)))


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
        default=os.path.abspath("geoserver_cleaner_report.csv"),
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
        "--catalog-source",
        choices=("auto", "filesystem", "rest"),
        default="auto",
        help="Catalog discovery source. 'auto' prefers local data_dir/workspaces and falls back to REST.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=worker_default(),
        help="Worker thread count for per-store filesystem processing.",
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

    data_dir = os.path.abspath(args.data_dir)
    data_root = os.path.join(data_dir, "data")
    if not os.path.isdir(data_root):
        raise SystemExit("GeoServer data path does not exist: {}".format(data_root))

    requested_catalog_source = (args.catalog_source or "auto").lower()
    if requested_catalog_source == "auto":
        catalog_source = (
            "filesystem" if os.path.isdir(os.path.join(data_dir, "workspaces")) else "rest"
        )
    else:
        catalog_source = requested_catalog_source
    validate_args(args, catalog_source)

    excluded_workspaces = sorted(parse_excluded_workspaces(args.exclude_workspaces))
    output_html = derive_output_html_path(args.output_csv, args.output_html)

    client = None
    if catalog_source == "rest" or (requested_catalog_source == "auto" and args.geoserver_url):
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
        catalog_source=catalog_source,
        workers=args.workers,
    )
    orphan_rows = collect_orphans(data_root, referenced_roots, referenced_files)

    rows = sorted(
        store_rows,
        key=lambda item: (
            item["row_kind"],
            item["workspace"].lower(),
            item["store_name"].lower(),
            normalize_path(item.get("resolved_path") or item.get("configured_path") or ""),
        ),
    )
    rows.extend(orphan_rows)

    write_csv(args.output_csv, rows)
    write_html_report(
        output_html,
        rows,
        excluded_workspaces,
        args.geoserver_url,
        data_dir,
    )

    store_count = sum(1 for row in store_rows if row["row_kind"] == "store")
    orphan_count = sum(1 for row in orphan_rows if row["row_kind"] == "orphaned")
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
