# GeoServer Cleaner

GeoServer Cleaner is a FastAPI web application and reporting CLI for inspecting file-backed GeoServer data stores, finding oversized or orphaned data, exporting inventory snapshots, and safely deleting selected GeoServer stores through the GeoServer REST API.

The application is designed for GeoServer installations with many file-based stores under `data_dir`, including coverage stores, datastores, external file mappings, and mixed healthy/problem rows.

## Capabilities

- Scan a GeoServer catalog from the filesystem or REST fallback.
- Store inventory runs, rows, job status, and delete audit events in SQLite.
- Review stores and orphan rows in the web UI.
- Filter, sort, and page inventory snapshots.
- Export the latest completed snapshot as CSV or HTML.
- Preview delete operations before execution.
- Delete selected GeoServer store configuration through GeoServer REST.
- Use GeoServer-managed `purge=all` only for internal coverage-store data that is safe for GeoServer to purge.
- Treat external, unresolved, datastore, and orphan rows conservatively.

Important deletion behavior:

- Store deletion is always performed through GeoServer REST.
- The application does not directly delete files from disk.
- Orphan rows are report-only and cannot be deleted by the application.
- If store data is outside `data_dir`, deletion is configuration-only.
- GeoPackage datastores are configuration-only because unrelated tables may exist in the same `.gpkg`.

## Architecture

Core runtime components:

- `FastAPI` web application in `app/main.py`
- `Jinja2` templates in `app/templates/`
- `HTMX` partial updates and job polling
- `SQLite` persistence in `app/db.py`
- Shared runtime construction in `app/runtime.py`
- Inventory/reporting logic in `app/reporting/`
- Snapshot query/export helpers in `app/services/snapshots.py`
- Delete preview and execution logic in `app/services/deletion.py`
- GeoServer REST helpers in `app/services/geoserver.py`
- Standalone reporting CLI in `app/reporting/cli.py`

The web UI and reporting CLI share the same inventory and rendering logic. Delete execution remains part of the web application workflow because it requires explicit operator review.

## Configuration

Required production environment variables:

- `GEOSERVER_URL`
- `GEOSERVER_USER`
- `GEOSERVER_PASSWORD`
- `GEOSERVER_DATA_DIR_HOST`

Common optional variables:

- `GEOSERVER_CLEANER_TAG`
- `APP_PORT`
- `GEOSERVER_CATALOG_SOURCE`
- `GEOSERVER_EXCLUDE_WORKSPACES`
- `GEOSERVER_TIMEOUT`
- `GEOSERVER_WORKERS`
- `GEOSERVER_INSECURE`
- `GEOSERVER_EXTERNAL_PATH_MAPPINGS`
- `APP_PAGE_SIZE_DEFAULT`
- `APP_PAGE_SIZE_MAX`
- `APP_TITLE`
- `APP_LOG_LEVEL`
- `APP_LOG_PATH`
- `APP_LOG_MAX_BYTES`
- `APP_LOG_BACKUP_COUNT`
- `APP_ORPHAN_SMALL_FILE_THRESHOLD_BYTES`

Logging uses structured JSON records written to a rotating file. Supported `APP_LOG_LEVEL` values are `DEBUG`, `INFO`, `WARN`, `ERROR`, and `FATAL`; `INFOW` is accepted as an alias for `INFO`.

## Local Web Run

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Run the web application:

```powershell
python -m app.run
```

Open:

```text
http://localhost:8000/stores
```

## Reporting CLI

Generate a standalone report from a GeoServer data directory:

```powershell
python -m app.reporting.cli --data-dir <path-to-geoserver-data-dir>
```

The CLI is intended for inventory/report generation. It does not execute delete operations.

## Docker

Build and run the local application image:

```powershell
docker compose -f docker-compose.geoserver-cleaner.yml up --build
```

Run the local production-style flow against the bundled GeoServer fixture:

```powershell
.\scripts\run-local-production.ps1
```

This starts:

- GeoServer test fixture on `http://127.0.0.1:8081/geoserver`
- GeoServer Cleaner UI on `http://127.0.0.1:8000/stores`

Stop the local production-style flow:

```powershell
.\scripts\run-local-production.ps1 -Down
```

Validate a published GHCR image:

```powershell
.\scripts\validate-ghcr-image.ps1 -ImageTag 2.7.0
```

The validation script pulls the published image, starts it with `docker-compose.production.yml`, exercises the web UI, triggers an inventory scan, downloads the latest CSV export, and writes a Markdown report to `TASK_EXECUTION_REPORT.md`.

## Security

The production compose file runs the application container with:

- `read_only: true`
- `cap_drop: [ALL]`
- `no-new-privileges:true`
- writable tmpfs for `/tmp`
- named volumes for SQLite data and exports

Local image security checks:

```powershell
.\scripts\test-security.ps1
```

## Data Model

Each completed snapshot contains:

- store rows
- orphan rows
- workspace, store name, type, layer names, configured path, resolved path, path kind, status, and notes
- size and file-count summaries

Snapshot exports are generated from persisted runs instead of live rescans.

## Delete Workflow

Operators select rows in the web UI and request a preview. The preview classifies each selected row as deletable or blocked, explains whether GeoServer may purge data, and shows the exact operation semantics before execution.

When execution starts, the job manager calls GeoServer REST delete endpoints and then verifies whether stores still exist. Snapshot rows are removed only when deletion verification succeeds.
