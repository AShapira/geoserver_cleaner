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
- delete stores through GeoServer REST with `recurse=true`
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
- `MCP` server over `stdio` and optional `streamable-http`

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
6. review the updated snapshot rows, then run a full scan when orphan data must be refreshed

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
- optional `streamable-http` at `/mcp` when enabled in the web app

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

Standalone report CLI:

```powershell
python -m app.reporting.cli --data-dir <path-to-geoserver-data-dir>
```

Expose MCP over HTTP from the existing web app:

```powershell
$env:APP_ENABLE_MCP_HTTP="true"
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

The HTTP MCP endpoint is then:

```text
http://localhost:8000/mcp
```

This endpoint is intended for trusted internal networks or a reverse proxy. v1 does not add app-level authentication.

VS Code workspace `mcp.json` example:

```json
{
  "servers": {
    "geoServerCleaner": {
      "type": "http",
      "url": "http://localhost:8000/mcp"
    }
  }
}
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
- `GEOSERVER_EXTERNAL_PATH_MAPPINGS`
- `GEOSERVER_EXCLUDE_WORKSPACES`
- `GEOSERVER_TIMEOUT`
- `GEOSERVER_WORKERS`
- `GEOSERVER_INSECURE`
- `APP_DATABASE_PATH`
- `APP_EXPORT_DIR`
- `APP_PAGE_SIZE_DEFAULT`
- `APP_PAGE_SIZE_MAX`
- `APP_TITLE`
- `APP_ENABLE_MCP_HTTP`
- `APP_MCP_HTTP_PATH`
- `APP_LOG_LEVEL`
- `APP_LOG_PATH`
- `APP_LOG_MAX_BYTES`
- `APP_LOG_BACKUP_COUNT`
- `APP_ORPHAN_SMALL_FILE_THRESHOLD_BYTES`

External path mappings:

- use `GEOSERVER_EXTERNAL_PATH_MAPPINGS` when GeoServer and GeoServer Cleaner see the same external share through different absolute paths
- the value must be a JSON object whose keys are GeoServer-visible absolute roots and whose values are cleaner-visible absolute roots
- example:

```json
{
  "C:\\data\\osm": "/ext_data_path",
  "\\\\fileserver\\gis": "/mnt/gis"
}
```

- mappings are applied only to absolute external store paths
- `data/...` and other relative paths still resolve from `GEOSERVER_DATA_DIR`
- mapped external roots are used for inventory visibility and sizing only
- mapped external roots are not included in orphan detection
- if the mapped cleaner-side root is missing or inaccessible, the store remains `missing` and the row notes explain that the mapped external root is not accessible from the current runtime

Logging:

- the web app and MCP server write structured JSON logs to a rotating file
- supported `APP_LOG_LEVEL` values are `DEBUG`, `INFO`, `WARN`, `ERROR`, and `FATAL`
- `INFOW` is accepted as an alias for `INFO`
- default level is `INFO`
- if `APP_LOG_PATH` is not set, the app writes to `logs/geoserver_cleaner.log` beside the SQLite database path
- `APP_LOG_MAX_BYTES` controls rotation size per file
- `APP_LOG_BACKUP_COUNT` controls how many rotated files are retained

Orphan filtering:

- empty directories are not shown as orphan rows
- orphan files smaller than `APP_ORPHAN_SMALL_FILE_THRESHOLD_BYTES` are hidden
- default threshold is `102400` bytes (`100 KB`)

## Docker

The same image can run either runtime.

Build:

```powershell
docker build -f docker/Dockerfile.app -t geoserver-cleaner .
```

Local security scan:

```powershell
.\scripts\test-security.ps1
```

The script:

- builds the app image
- shows a Docker Scout quick overview
- fails if fixable `critical` or `high` vulnerabilities are found

If the image is already built:

```powershell
.\scripts\test-security.ps1 -SkipBuild -ImageTag geoserver-cleaner:security
```

Run the web app:

```powershell
docker compose -f docker-compose.geoserver-cleaner.yml up --build
```

The provided compose file enables HTTP MCP on the same port as the web app:

```text
http://localhost:8000/mcp
```

The compose file is:

- [docker-compose.geoserver-cleaner.yml](c:/Alex/work/geoserver_cleaner/docker-compose.geoserver-cleaner.yml)

Example external-store mapping in Docker:

```yaml
environment:
  GEOSERVER_EXTERNAL_PATH_MAPPINGS: '{"C:\\data\\osm":"/ext_data_path"}'
volumes:
  - /host/path/to/osm:/ext_data_path
```

This example means GeoServer is configured with `C:\data\osm`, while the cleaner container sees the same data at `/ext_data_path`.

Production compose:

- [docker-compose.production.yml](c:/Alex/work/geoserver_cleaner/docker-compose.production.yml)

This file pulls the published GitHub Container Registry image instead of building locally.

Run production compose:

```powershell
$env:GEOSERVER_URL="http://your-geoserver-host/geoserver"
$env:GEOSERVER_USER="admin"
$env:GEOSERVER_PASSWORD="secret"
$env:GEOSERVER_DATA_DIR_HOST="C:\path\to\geoserver_data"
docker compose -f docker-compose.production.yml up -d
```

Notes:

- the default image tag is the latest tagged release currently in this repo: `2.7.0`
- override it with `GEOSERVER_CLEANER_TAG` when a newer release is published
- if the package is private, run `docker login ghcr.io` before `docker compose up`

Local production flow against the repo test fixture:

- use [`.env.production.local`](c:/Alex/work/geoserver_cleaner/.env.production.local) for a local `2.7.0` image run wired to [geoserver_test/geoserver_data](c:/Alex/work/geoserver_cleaner/geoserver_test/geoserver_data)
- start both the GeoServer fixture and the production app with [`scripts/run-local-production.ps1`](c:/Alex/work/geoserver_cleaner/scripts/run-local-production.ps1)

```powershell
.\scripts\run-local-production.ps1
```

Stop it with:

```powershell
.\scripts\run-local-production.ps1 -Down
```

This starts:

- GeoServer test fixture on `http://127.0.0.1:8081/geoserver`
- GeoServer Cleaner UI on `http://127.0.0.1:8000/stores`
- HTTP MCP endpoint on `http://127.0.0.1:8000/mcp/`

Published-image validation:

- use [`scripts/validate-ghcr-image.ps1`](c:/Alex/work/geoserver_cleaner/scripts/validate-ghcr-image.ps1) to pull a published GHCR image tag, run it with the production compose file, exercise the app over HTTP, and write a Markdown validation report to `TASK_EXECUTION_REPORT.md`

```powershell
.\scripts\validate-ghcr-image.ps1 -ImageTag 2.7.0
```

VS Code workspace MCP config for that flow lives in [`.vscode/mcp.json`](c:/Alex/work/geoserver_cleaner/.vscode/mcp.json).

Runtime switch:

- `APP_RUNTIME=web` for the FastAPI UI
- `APP_RUNTIME=mcp` for the stdio MCP server

The image intentionally excludes the local GeoServer test fixture through [.dockerignore](c:/Alex/work/geoserver_cleaner/.dockerignore).

## Security Scanning

Recommended local checks:

```powershell
python -m pip install pip-audit
pip-audit -r requirements.txt
.\scripts\test-security.ps1
```

Recommended CI checks:

- Python dependency audit with `pip-audit`
- Docker image scan with Docker Scout

The repository workflow for image scanning is:

- [.github/workflows/security.yml](c:/Alex/work/geoserver_cleaner/.github/workflows/security.yml)

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
  GeoServer can remove store configuration and, for coverage stores, internal data when its reader supports purge
- external or unresolved data
  GeoServer removes store configuration only

Datastore deletion uses GeoServer `recurse=true` to remove configured feature types and layers. For GeoPackage datastores, this means configured layers in that datastore are removed from GeoServer; unrelated tables in the same `.gpkg` are not directly deleted by this application.

External path mappings do not change delete semantics:

- if mapped data still resolves outside `data_dir`, delete preview remains configuration-only

Not allowed:

- orphan deletion
- direct file deletion by the app

## Test Fixture

A local GeoServer fixture for development and validation is kept under [geoserver_test](c:/Alex/work/geoserver_cleaner/geoserver_test).

Important files:

- [geoserver_test/docker-compose.geoserver-test.yml](c:/Alex/work/geoserver_cleaner/geoserver_test/docker-compose.geoserver-test.yml)
- [geoserver_test/populate_geoserver_natural_earth.py](c:/Alex/work/geoserver_cleaner/geoserver_test/populate_geoserver_natural_earth.py)
- [geoserver_test/populate_geoserver_bulk_mock.py](c:/Alex/work/geoserver_cleaner/geoserver_test/populate_geoserver_bulk_mock.py)
- [geoserver_test/populate_external_mapping_demo.py](c:/Alex/work/geoserver_cleaner/geoserver_test/populate_external_mapping_demo.py)

This fixture is for local testing only and is not part of the cleanup-app image build context.

### External mapping demo

The external mapping demo validates two separate behaviors:

- inventory can resolve GeoServer catalog paths that point outside `GEOSERVER_DATA_DIR`
- deletion always uses GeoServer REST, with `purge=all` only for internal coverage-store data and `purge=none` for mapped external coverage-store data

Run it from PowerShell:

```powershell
.\scripts\run-external-mapping-demo.ps1
```

Stop the demo containers:

```powershell
.\scripts\run-external-mapping-demo.ps1 -Down
```

The demo creates local ignored fixture data under:

- `geoserver_test/geoserver_data`
- `geoserver_test/external_data`
- `app_data/external_mapping_demo`

The cleaner container maps:

```json
{
  "C:\\demo_geodata\\windows": "/external_windows",
  "/srv/geodata/posix": "/external_posix",
  "C:\\demo_geodata\\missing": "/external_missing"
}
```

Expected validation:

- `internal_raster` previews as `Delete Data = Yes`
- `windows_external_raster` and `posix_external_raster` preview as configuration-only
- external files still exist after delete execution
- cleaner logs show both `purge=all` for internal data and `purge=none` for external data

## Limitations

- the product is focused on file-based GeoServer stores
- database-backed stores such as PostGIS are out of scope
- external data locations are not scanned for orphan detection
- deleting a store with external data does not delete that external data
- authentication and RBAC are not implemented yet

## Internal Note

The standalone inventory/report generator now lives under [app/reporting/cli.py](c:/Alex/work/geoserver_cleaner/app/reporting/cli.py). The primary product interfaces remain the web application and the MCP server.
