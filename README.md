# GeoServer Cleaner

## Overview

GeoServer Cleaner is a server-side cleanup tool for large GeoServer installations.

It provides two interfaces over the same backend:

- a web application for operators
- an MCP server for LLM and agent workflows

Both interfaces use the same inventory snapshot data, job model, GeoServer REST delete flow, and export logic.

The project is designed for installations where GeoServer contains many file-based stores under `data_dir`, including:

- `GeoTIFF`
- `ImageMosaic`
- `Shapefile`
- `GeoPackage`

## Main Capabilities

- scan GeoServer and build inventory snapshots in SQLite
- calculate size and file counts per store
- detect orphaned files and directories under `data_dir/data`
- filter, sort, and review stores in the web UI
- preview store deletion before execution
- delete stores through GeoServer REST with `recurse=true&purge=all`
- distinguish between:
  - stores with data inside `data_dir`
  - stores with data outside `data_dir`
- export the latest snapshot as CSV or HTML
- expose the same operational capabilities to agents through MCP

Important behavior:

- store deletion is always performed through GeoServer REST
- orphan rows are report-only and cannot be deleted by the app or MCP server
- if store data is outside `data_dir`, deleting the store is treated as a configuration-only operation

## Architecture

Core components:

- `FastAPI` backend
- `Jinja2` templates
- `HTMX` for partial page refresh and job polling
- `SQLite` for inventory snapshots and jobs
- `MCP` server over `stdio`

The shared backend lives under [app](c:/Alex/work/geoserver_cleaner/app).

Main modules:

- web app entry: [app/main.py](c:/Alex/work/geoserver_cleaner/app/main.py)
- MCP server: [app/mcp/server.py](c:/Alex/work/geoserver_cleaner/app/mcp/server.py)
- runtime launcher: [app/run.py](c:/Alex/work/geoserver_cleaner/app/run.py)
- snapshot queries and exports: [app/services/snapshots.py](c:/Alex/work/geoserver_cleaner/app/services/snapshots.py)
- deletion logic: [app/services/deletion.py](c:/Alex/work/geoserver_cleaner/app/services/deletion.py)
- inventory scan logic: [app/services/inventory.py](c:/Alex/work/geoserver_cleaner/app/services/inventory.py)

## Web Application

The web UI is intended for manual cleanup operations.

Current workflow:

1. run an inventory scan
2. review the latest snapshot on `/stores`
3. filter and select stores
4. inspect the delete preview
5. execute a delete job
6. review the refreshed snapshot

Implemented UI capabilities:

- server-side paging, filtering, and sorting
- background scan and delete jobs
- live job progress pages
- delete preview with internal/external data explanation
- CSV and HTML snapshot download buttons

Run locally:

```powershell
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open:

```text
http://localhost:8000/stores
```

## MCP Server

The MCP server is intended for LLM and agent usage and runs against the same database and GeoServer configuration as the web app.

Transport:

- `stdio`

Current MCP tools:

- `get_latest_snapshot`
- `start_inventory_scan`
- `get_job_status`
- `list_heaviest_stores`
- `summarize_workspace_usage`
- `list_orphans`
- `find_stores`
- `delete_stores`
- `export_snapshot_csv`
- `export_snapshot_html`

Examples of supported agent use:

- `list 5 heaviest stores`
- `summarize disk usage by workspaces`
- `find unresolved stores`
- `delete these store ids`
- `export the latest snapshot as HTML`

Run locally:

```powershell
python -m app.mcp.server
```

## Configuration

Both the web app and MCP server use the same environment variables.

Required:

- `GEOSERVER_URL`
- `GEOSERVER_USER`
- `GEOSERVER_PASSWORD`
- `GEOSERVER_DATA_DIR`

Optional:

- `GEOSERVER_CATALOG_SOURCE`
  `auto`, `filesystem`, or `rest`
- `GEOSERVER_EXCLUDE_WORKSPACES`
- `GEOSERVER_TIMEOUT`
- `GEOSERVER_WORKERS`
- `GEOSERVER_INSECURE`
- `APP_DATABASE_PATH`
- `APP_EXPORT_DIR`
- `APP_PAGE_SIZE_DEFAULT`
- `APP_PAGE_SIZE_MAX`
- `APP_TITLE`

## Docker

The same image can run either runtime.

Build:

```powershell
docker build -f docker/Dockerfile.app -t geoserver-cleaner .
```

Run the web app:

```powershell
docker compose -f docker-compose.geoserver-cleaner.yml up --build
```

The compose file is:

- [docker-compose.geoserver-cleaner.yml](c:/Alex/work/geoserver_cleaner/docker-compose.geoserver-cleaner.yml)

Runtime switch:

- `APP_RUNTIME=web` for the FastAPI UI
- `APP_RUNTIME=mcp` for the MCP server

The image intentionally excludes the local GeoServer test fixture through [.dockerignore](c:/Alex/work/geoserver_cleaner/.dockerignore).

## Snapshot Model

The system stores inventory snapshots in SQLite and uses the latest completed snapshot as the default source for:

- the `/stores` page
- report downloads
- MCP query tools

Each snapshot contains:

- store rows
- orphan rows
- size and file counts
- status and notes
- excluded workspace context
- GeoServer URL and data directory metadata

Orphan analysis is limited to:

- `data_dir/data`

## Deletion Model

Store deletion is GeoServer-managed, not filesystem-managed by this application.

The delete preview and MCP delete tool distinguish between:

- internal data
  GeoServer can remove store configuration and internal data
- external or unresolved data
  GeoServer removes store configuration only

Not allowed:

- orphan deletion
- direct file deletion by the app

## Test Fixture

A local GeoServer fixture for development and validation is kept under [geoserver_test](c:/Alex/work/geoserver_cleaner/geoserver_test).

Important files:

- [geoserver_test/docker-compose.geoserver-test.yml](c:/Alex/work/geoserver_cleaner/geoserver_test/docker-compose.geoserver-test.yml)
- [geoserver_test/populate_geoserver_natural_earth.py](c:/Alex/work/geoserver_cleaner/geoserver_test/populate_geoserver_natural_earth.py)
- [geoserver_test/populate_geoserver_bulk_mock.py](c:/Alex/work/geoserver_cleaner/geoserver_test/populate_geoserver_bulk_mock.py)

This fixture is for local testing only and is not part of the cleanup-app image build context.

## Limitations

- the product is focused on file-based GeoServer stores
- database-backed stores such as PostGIS are out of scope
- external data locations are not scanned for orphan detection
- deleting a store with external data does not delete that external data
- authentication and RBAC are not implemented yet

## Internal Note

The repository still contains the standalone inventory/report generator in [geoserver_store_report.py](c:/Alex/work/geoserver_cleaner/geoserver_store_report.py), but the primary product interfaces are the web application and the MCP server.
