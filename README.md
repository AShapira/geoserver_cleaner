# GeoServer Store Report

## Purpose

`geoserver_store_report.py` generates a CSV inventory of data used by a GeoServer instance.

The repository now also includes a web application that builds on the same inventory logic:

- `FastAPI` backend
- `Jinja2` templates
- `HTMX` for partial page refresh and job polling
- `SQLite` snapshot cache and job tracking

GeoServer fixture assets for local testing are organized under [geoserver_test](c:\Alex\work\geoserver_cleaner\geoserver_test).

It is intended for cases where a GeoServer installation has accumulated a large amount of data on disk and the user needs to understand:

- which stores consume the most space
- which layers belong to each store
- where the data is stored on disk
- how many files belong to each store
- which folders or files under `data_dir/data` are no longer referenced by GeoServer
- a user-friendly sortable HTML version of the same report

The script is designed to run in the QGIS Python shell or any regular Python environment available on the GeoServer workstation.

## What The Script Does

The script combines two sources of information:

1. GeoServer REST API metadata
   - workspaces
   - store names
   - store types
   - published layer names
   - configured data paths

2. Filesystem scanning
   - resolves each store to a local Windows path
   - calculates total size in bytes and GB
   - counts files
   - includes common sidecar files for single-file datasets such as GeoTIFF, Shapefile, and GeoPackage
   - scans `data_dir/data` for orphaned folders and files not referenced by any discovered store

The script is hardened to continue when individual stores return bad REST responses. Those failures are logged and written as `error` rows instead of aborting the full report.

The report contains one row per store, plus additional rows for orphaned data, and also writes a sortable HTML view.

## Supported Store Scenarios

The script is built for the storage patterns described in this project:

- `GeoTIFF`
- `ImageMosaic`
- `Shapefile`
- `GeoPackage`

It should also work for some other file-based stores if GeoServer exposes a usable file path through REST, but the main logic is optimized for the store types above.

## Requirements

- Windows machine with access to the GeoServer data directory
- GeoServer REST API enabled
- GeoServer URL, username, and password
- Python available through QGIS shell or system Python

The script uses only Python standard library modules. No external packages are required.

## Configuration

The script accepts command-line arguments. Most parameters also have environment variable defaults.

### Required Inputs

- `--geoserver-url`
  GeoServer base URL, for example:
  `http://server:8080/geoserver`

- `--password`
  GeoServer password

- `--data-dir`
  Path to the GeoServer data directory, not the `data` subfolder.
  Example:
  `D:\GeoServer\data_dir`

### Optional Inputs

- `--username`
  GeoServer username
  Default: `admin`

- `--output-csv`
  Output CSV file path
  Default: `.\geoserver_store_report.csv`

- `--output-html`
  Optional HTML output file path
  Default: same as CSV output path but with `.html` extension

- `--exclude-workspaces`
  Optional comma-separated list of workspaces to exclude from the report
  Stores from excluded workspaces are omitted from report rows, and data belonging to those workspaces is not marked as orphaned

- `--log-level`
  Logging level
  Default: `INFO`

- `--timeout`
  REST request timeout in seconds
  Default: `60`

- `--insecure`
  Disables HTTPS certificate validation
  Use only if GeoServer is exposed through HTTPS with an untrusted certificate

### Environment Variables

Instead of passing some values every time, the following environment variables can be used:

- `GEOSERVER_URL`
- `GEOSERVER_USER`
- `GEOSERVER_PASSWORD`
- `GEOSERVER_DATA_DIR`

Command-line arguments override environment variable values.

## Usage

Example command:

```powershell
python geoserver_store_report.py `
  --geoserver-url "http://server:8080/geoserver" `
  --username "admin" `
  --password "secret" `
  --data-dir "D:\GeoServer\data_dir" `
  --output-csv "D:\reports\geoserver_store_report.csv"
```

Example excluding workspaces and writing both outputs explicitly:

```powershell
python geoserver_store_report.py `
  --geoserver-url "http://server:8080/geoserver" `
  --username "admin" `
  --password "secret" `
  --data-dir "D:\GeoServer\data_dir" `
  --output-csv "D:\reports\geoserver_store_report.csv" `
  --output-html "D:\reports\geoserver_store_report.html" `
  --exclude-workspaces "workspace_a,workspace_b" `
  --log-level INFO
```

Example with insecure HTTPS:

```powershell
python geoserver_store_report.py `
  --geoserver-url "https://server/geoserver" `
  --username "admin" `
  --password "secret" `
  --data-dir "D:\GeoServer\data_dir" `
  --output-csv "D:\reports\geoserver_store_report.csv" `
  --insecure
```

## Output

The script writes:

- a CSV file with one row per store and additional rows for orphaned data
- a sortable HTML file with summary cards, filtering, and clickable column sorting

### CSV Columns

- `row_kind`
  `store` or `orphaned`

- `workspace`
  GeoServer workspace name for store rows

- `store_name`
  GeoServer store name

- `store_type`
  Store type as reported by GeoServer

- `layer_names`
  Comma-separated list of published layer names for the store

- `configured_path`
  Path or URL value extracted from GeoServer store configuration

- `resolved_path`
  Local Windows path used for filesystem scanning

- `path_kind`
  `directory`, `file`, `missing`, or empty if unresolved

- `size_bytes`
  Total size of the scanned store or orphaned item in bytes

- `size_gb`
  Total size in GB, rounded to 2 decimal places

- `file_count`
  Number of files counted for that row

- `status`
  Processing result for that row

- `notes`
  Additional explanation, especially for missing, unresolved, error, or orphaned rows

### HTML Report

The HTML report is intended for manual review and cleanup planning.

It includes:

- summary cards for store count, orphan count, issue count, and tracked size
- a searchable table
- clickable column headers for sorting
- status color coding for quick scanning
- metadata showing the GeoServer URL, data directory, excluded workspaces, and generation time

## Status Values

Typical `status` values:

- `ok`
  Store was resolved and scanned successfully

- `missing`
  Store path was resolved but does not exist on disk

- `unresolved`
  The script could not extract a usable filesystem path from the store configuration

- `error`
  The script encountered an exception while processing the store or scanning orphaned data

- `orphaned`
  File or directory under `data_dir/data` is not referenced by any discovered store

## How Store Size Is Calculated

### Directory-Based Stores

For directory-based stores such as `ImageMosaic`, the script scans the entire directory recursively and sums all files under that directory.

### File-Based Stores

For file-based stores such as `GeoTIFF`, `Shapefile`, and `GeoPackage`, the script includes the main dataset and common sidecar files in the same folder.

Examples:

- GeoTIFF:
  `.tif`, `.ovr`, `.aux.xml`, `.prj`, `.tfw`, and similar sidecar files

- Shapefile:
  `.shp`, `.shx`, `.dbf`, `.prj`, `.cpg`, `.qix`, and common related files with the same base name

- GeoPackage:
  `.gpkg` and common journal side files such as `-wal` and `-shm`

## Orphaned Data Detection

The script scans `data_dir/data` and compares its contents against all store paths that were successfully resolved.

## Cleanup Web App

The application lives under [app](c:\Alex\work\geoserver_cleaner\app) and implements the first full cleanup workflow:

- inventory page with server-side paging, filtering, and sorting
- multi-select store selection
- delete preview page with path safety checks
- background scan and delete jobs
- automatic inventory refresh after delete jobs

The application reuses the filesystem-first scan logic from [geoserver_store_report.py](c:\Alex\work\geoserver_cleaner\geoserver_store_report.py).

### Web App Requirements

- Python 3.13
- packages from [requirements.txt](c:\Alex\work\geoserver_cleaner\requirements.txt)
- local access to the GeoServer data directory
- GeoServer REST credentials for delete operations and REST fallback

### Web App Configuration

Environment variables:

- `GEOSERVER_URL`
- `GEOSERVER_USER`
- `GEOSERVER_PASSWORD`
- `GEOSERVER_DATA_DIR`
- `GEOSERVER_CATALOG_SOURCE`
  `auto`, `filesystem`, or `rest`
- `GEOSERVER_EXCLUDE_WORKSPACES`
- `APP_DATABASE_PATH`
- `ALLOW_PHYSICAL_DELETE`
- `ALLOWED_DATA_ROOTS`
- `APP_PAGE_SIZE_DEFAULT`
- `APP_PAGE_SIZE_MAX`

Important note:

- physical delete is disabled by default
- when enabled, the application deletes only under `ALLOWED_DATA_ROOTS`
- the scan form in the web UI also accepts a comma-separated exclude list and persists that choice into the generated snapshot

### Run The Web App

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Run locally:

```powershell
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open:

```text
http://localhost:8000/stores
```

### Docker Run

Build and run with Docker Compose:

```powershell
docker compose -f docker-compose.cleanup-app.yml up --build
```

The compose example is in [docker-compose.cleanup-app.yml](c:\Alex\work\geoserver_cleaner\docker-compose.cleanup-app.yml). It mounts:

- GeoServer data directory at `/geoserver_data`
- application database at `/app_data`

By default the example mounts the local fixture directory:

- [geoserver_test/geoserver_data](c:\Alex\work\geoserver_cleaner\geoserver_test\geoserver_data)

### Current Scope

The web app is an MVP implementation of the design review in [FASTAPI_APP_DESIGN_REVIEW.md](c:\Alex\work\geoserver_cleaner\FASTAPI_APP_DESIGN_REVIEW.md).

Implemented now:

- inventory snapshots persisted in SQLite
- `/stores` page with server-side table queries
- `/delete/preview` safety preview
- `/delete/execute` background job
- `/scan` background job
- `/jobs/{id}` status page

Not implemented yet:

- user authentication and RBAC
- partial workspace-only refresh after delete
- richer audit browsing UI
- batch progress itemization per store in the job page

If workspaces are excluded with `--exclude-workspaces`, or with the web app scan exclude field, stores from those workspaces are omitted from report rows and their data is not treated as orphaned.

## GeoServer Test Fixture

The local Docker GeoServer fixture and its population scripts now live in [geoserver_test](c:\Alex\work\geoserver_cleaner\geoserver_test):

- [geoserver_test/docker-compose.geoserver-test.yml](c:\Alex\work\geoserver_cleaner\geoserver_test\docker-compose.geoserver-test.yml)
- [geoserver_test/populate_geoserver_natural_earth.py](c:\Alex\work\geoserver_cleaner\geoserver_test\populate_geoserver_natural_earth.py)
- [geoserver_test/populate_geoserver_bulk_mock.py](c:\Alex\work\geoserver_cleaner\geoserver_test\populate_geoserver_bulk_mock.py)

The cleanup-app Docker image ignores that directory through [.dockerignore](c:\Alex\work\geoserver_cleaner\.dockerignore), so fixture data and downloads are not copied into the application image build context.

It reports:

- orphaned directories not claimed by any store
- orphaned files not claimed by any store
- orphaned files located inside partially referenced directories

This is useful for identifying abandoned GeoTIFFs, unused mosaic folders, or residual data left behind after old stores were removed from GeoServer.

## Limitations

- The script relies on GeoServer REST responses. If a store does not expose a usable path, it will be marked as `unresolved`.
- If an individual store returns a bad or invalid REST response, the script logs the failure, records an `error` row when the workspace is included, and continues.
- The logic is focused on file-based storage under the GeoServer data directory. It is not intended for database-backed stores such as PostGIS.
- Sidecar matching is based on common naming conventions. If a deployment uses unusual file structures, counts may need review.
- Orphan detection is based on resolved store paths. If GeoServer references data outside the expected conventions, some results may need manual validation.

## Recommended Workflow

1. Run the script and sort the CSV by `size_bytes` descending.
2. Review the largest `store` rows first.
3. Review all `missing`, `unresolved`, and `error` rows.
4. Review `orphaned` rows before deleting anything.
5. Validate suspicious results manually in GeoServer and on disk before cleanup.

## Files

- [geoserver_store_report.py](c:\Alex\work\geoserver_cleaner\geoserver_store_report.py)
- [README.md](c:\Alex\work\geoserver_cleaner\README.md)
