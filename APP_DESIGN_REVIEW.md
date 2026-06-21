# GeoServer Cleaner Design Review

## Current Shape

GeoServer Cleaner is now centered on two supported surfaces:

- A `FastAPI` web UI for operator-driven inventory, export, preview, and delete workflows.
- A standalone reporting CLI for inventory/report generation.

The shared backend remains under `app/reporting` and `app/services`, with persistence in `SQLite` and background scan/delete execution through the in-process job manager.

Main entry points:

- Web UI: `app/main.py`
- Runtime launcher: `app/run.py`
- Reporting CLI: `app/reporting/cli.py`
- Runtime construction: `app/runtime.py`

## Architecture Assessment

The codebase has a clean separation between user interfaces and domain behavior:

- `app/reporting/core.py` handles catalog discovery, path resolution, size aggregation, and orphan detection.
- `app/services/snapshots.py` handles persisted snapshot queries and exports.
- `app/services/deletion.py` owns delete preview and execution semantics.
- `app/services/geoserver.py` owns GeoServer REST operations.
- `app/jobs.py` runs scan and delete work while recording status and metadata.

This structure keeps the web UI thin and preserves a reusable reporting path for non-interactive inventory generation.

## Implemented Web Routes

Current web routes include:

- `/`
- `/stores`
- `/stores/table`
- `/scan`
- `/jobs/{job_id}`
- `/jobs/{job_id}/header`
- `/jobs/{job_id}/status`
- `/delete/preview`
- `/delete/execute`
- `/reports/latest.csv`
- `/reports/latest.html`

The web UI remains the only supported interactive delete surface.

## Deletion Semantics

The delete flow is conservative and appropriate for GeoServer data stores:

- Store deletion is GeoServer-managed, not filesystem-managed by this application.
- Coverage stores inside `data_dir` may use GeoServer `purge=all`.
- External, mapped, unresolved, and datastore rows are configuration-only.
- Orphan rows are report-only and blocked from deletion.
- Verification controls whether snapshot rows are removed after delete execution.

The wording in the UI should continue to emphasize that `Delete Data = Yes` means GeoServer may purge internal coverage-store data. It does not mean this application directly removes files.

## Deployment Assessment

The Docker packaging is suitable for the current application:

- `python:3.13-alpine` base image
- non-root application user
- read-only production container filesystem
- dropped Linux capabilities
- `no-new-privileges:true`
- writable volumes for database and exports
- structured rotating JSON logs

The local production script and published-image validation script should continue to exercise the GeoServer fixture, `/stores`, scan execution, and report download.

## Recommendations

- Keep delete behavior REST-only and verification-driven.
- Keep the reporting CLI read-only.
- Add browser-level smoke coverage for the main web workflow if UI changes accelerate.
- Keep Docker validation focused on startup, scan, export, and container hardening.
- Avoid reintroducing secondary agent/server surfaces unless they have a separate security and support plan.

## Source Layout

```text
app/
  main.py
  run.py
  runtime.py
  db.py
  jobs.py
  reporting/
    cli.py
    core.py
    render.py
  services/
    deletion.py
    geoserver.py
    inventory.py
    job_status.py
    snapshots.py
  templates/
  static/
docker/
scripts/
tests/
```
