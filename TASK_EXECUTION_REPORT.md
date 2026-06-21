# GeoServer Cleaner Current Validation Report

## Scope

This report tracks the current application shape. GeoServer Cleaner supports the FastAPI web UI, reporting CLI, Docker packaging, scan jobs, delete preview/execution, and snapshot exports.

## Current Runtime Surfaces

- Web UI: `http://127.0.0.1:8000/stores`
- CSV export: `http://127.0.0.1:8000/reports/latest.csv`
- HTML export: `http://127.0.0.1:8000/reports/latest.html`
- Reporting CLI: `python -m app.reporting.cli --data-dir <path-to-geoserver-data-dir>`

## Validation Expectations

The standard validation flow is:

1. Run unit tests with `python -m unittest discover -s tests -v`.
2. Confirm the FastAPI application imports.
3. Validate local and production Docker Compose configuration.
4. Run the published-image validation script when a release image is available.

## Validation Results

Executed on 2026-06-21:

- `python -m unittest discover -s tests -v`: passed, 51 tests.
- `python -c "import app.main; print(app.main.app.title)"`: passed, printed `GeoServer Cleaner`.
- `docker compose -f docker-compose.geoserver-cleaner.yml config`: passed.
- `docker compose --env-file .env.production.local -f docker-compose.production.yml config`: passed.
- `docker compose -f docker-compose.external-mapping-demo.yml config`: passed.
- Removed-server content scan: no matches.
- Removed-server filename scan: no matches.
- `git diff --check`: passed; Git reported line-ending normalization warnings only.

## Docker Validation Flow

Use:

```powershell
.\scripts\validate-ghcr-image.ps1 -ImageTag 2.7.0
```

The script pulls `ghcr.io/ashapira/geoserver-cleaner:<tag>`, starts it with the production compose file, checks `/stores`, triggers an inventory scan, waits for completion, downloads the latest CSV report, and records the resulting command log in this file.

## Notes

- Delete execution is performed through GeoServer REST.
- The application does not directly delete files from disk.
- Orphan rows are report-only.
- External and unresolved paths are configuration-only during delete execution.
