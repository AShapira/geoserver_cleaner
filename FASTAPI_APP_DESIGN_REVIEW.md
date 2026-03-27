# FastAPI App Design Review

## Scope

This document reviews the target architecture for turning the current reporting script into an operational cleanup application with this flow:

1. present GeoServer contents
2. allow multi-select of stores
3. delete selected stores from GeoServer and disk
4. recalculate the inventory

The recommended stack is:

- backend: `FastAPI`
- HTML templates: `Jinja2`
- light interactivity: `HTMX` plus a small amount of local JavaScript
- persistence: `SQLite` first, optionally `PostgreSQL` later
- packaging: Docker image with all dependencies bundled locally

## Why This Stack Fits

This is an internal admin tool with large tables, destructive actions, and an isolated deployment environment.

`FastAPI + Jinja/HTMX` fits because it gives:

- server-side paging, filtering, and sorting for large datasets
- simple deployment with one backend service
- no separate frontend build required for the first version
- low operational complexity
- a clear place to put deletion safety checks and audit logging

This is a better long-term fit than a giant generated HTML report and more robust than a Streamlit app for bulk delete workflows.

## Architectural Decision

Use a backend-driven application.

That means:

- the browser does not load all stores at once
- the server owns filtering, paging, and sorting
- the server owns destructive actions
- the browser receives rendered HTML pages or partial HTML fragments

The current script should be split into reusable services instead of being called only as a CLI.

## Recommended High-Level Architecture

### 1. Web Application

Responsibilities:

- render pages
- accept filter and paging parameters
- handle bulk selection and deletion requests
- display scan and deletion job progress

Implementation:

- `FastAPI`
- `Jinja2Templates`
- `HTMX` for partial table refresh and job polling

### 2. Inventory Service

Responsibilities:

- discover GeoServer stores
- resolve data paths
- scan size and file counts
- detect orphaned data
- persist inventory snapshots

Implementation notes:

- reuse the catalog parsing and filesystem scanning logic from [geoserver_store_report.py](c:\Alex\work\geoserver_cleaner\geoserver_store_report.py)
- keep `filesystem` catalog parsing as the default path for scale
- use REST only where runtime operations require it

### 3. GeoServer Service

Responsibilities:

- call GeoServer REST API
- delete store objects safely
- reload or reset catalog if required

Implementation notes:

- isolate GeoServer REST logic in one service module
- never mix REST deletion and filesystem deletion in template handlers directly

### 4. Deletion Planner

Responsibilities:

- build a deletion plan before execution
- determine which store objects will be removed
- determine which paths are safe to delete
- block dangerous or ambiguous deletions

### 5. Job Runner

Responsibilities:

- run long scans in background
- run delete jobs in background
- update progress in the database

First version:

- in-process worker with a job table is sufficient

Possible later upgrade:

- external worker process if concurrency or uptime requirements grow

### 6. Database

Responsibilities:

- current inventory cache
- scan history
- deletion jobs and job items
- audit events

First version recommendation:

- `SQLite`

Use `PostgreSQL` later only if you need heavier concurrent use.

## Deployment Model

Use Docker.

Recommended runtime layout:

- `app` container
- mounted GeoServer `data_dir` volume
- mounted app data volume for database and logs
- network access to GeoServer URL inside the isolated network

Example responsibilities:

- app container reads `data_dir/workspaces`
- app container calls GeoServer REST
- app container deletes only under explicitly allowed roots

No CDN dependencies should be used. Bundle all Python packages, JS, and CSS inside the image.

## Page Model

### `/stores`

Purpose:

- main inventory page

Features:

- filters by workspace, type, status, path text, orphan flag
- server-side sort
- server-side paging
- multi-select checkboxes
- summary cards
- action buttons: `Preview Delete`, `Recalculate`

HTMX usage:

- filters update only the table area
- paging updates only the table area
- sort updates only the table area

### `/delete/preview`

Purpose:

- review selected stores before deletion

Features:

- selected stores list
- GeoServer objects to delete
- filesystem paths to delete
- blocked items and warnings
- final confirmation button

This page must be server-rendered and explicit. Do not perform delete directly from the list page without preview.

### `/jobs/{job_id}`

Purpose:

- view scan or delete progress

Features:

- queued/running/completed/failed status
- item counts
- failure list
- link back to `/stores`

HTMX usage:

- poll a small status fragment every few seconds

## Backend Endpoints

Suggested initial routes:

- `GET /stores`
- `GET /stores/table`
- `POST /scan`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/status`
- `POST /delete/preview`
- `POST /delete/execute`

Notes:

- `GET /stores` returns the full page
- `GET /stores/table` returns only the table fragment for HTMX updates
- destructive operations stay as `POST`

## Data Model

Suggested initial tables:

### `inventory_runs`

- `id`
- `started_at`
- `finished_at`
- `status`
- `catalog_source`
- `excluded_workspaces`
- `store_count`
- `orphan_count`
- `issue_count`
- `notes`

### `stores`

- `id`
- `run_id`
- `row_kind`
- `workspace`
- `store_name`
- `store_type`
- `layer_names`
- `configured_path`
- `resolved_path`
- `path_kind`
- `size_bytes`
- `file_count`
- `status`
- `notes`
- `path_hash` or normalized path key for dedup/safety checks

### `jobs`

- `id`
- `job_type` (`scan`, `delete`)
- `status`
- `created_at`
- `started_at`
- `finished_at`
- `requested_by`
- `summary_json`
- `error_text`

### `job_items`

- `id`
- `job_id`
- `store_id`
- `workspace`
- `store_name`
- `action`
- `status`
- `message`

### `audit_events`

- `id`
- `created_at`
- `event_type`
- `actor`
- `payload_json`

## Deletion Workflow

### Step 1. Select stores

The user selects rows on `/stores`.

The browser submits selected store ids to `POST /delete/preview`.

### Step 2. Build deletion plan

The backend computes a deletion plan.

Checks should include:

- does the store still exist in the latest inventory snapshot
- is the resolved path under an approved root
- is the path missing or unresolved
- is the same path referenced by another store
- is the store part of a multi-layer GeoPackage
- is the store an ImageMosaic directory

### Step 3. Show preview

The user sees:

- stores that can be deleted
- stores that are blocked
- exact GeoServer objects to remove
- exact filesystem paths to remove
- warnings

### Step 4. Execute deletion

The backend creates a delete job.

Recommended order:

1. delete GeoServer store object
2. confirm REST result
3. delete filesystem data if allowed
4. record audit event

### Step 5. Refresh inventory

Recommended first version:

- run a new scan job after delete completion

Possible later optimization:

- partial refresh only for affected workspaces

## Deletion Safety Rules

This is the most important part of the application.

Required safeguards:

- only delete paths under configured allowed roots
- normalize all paths before comparison
- block deletion if path ownership is ambiguous
- block deletion if a path is referenced by more than one active store
- require explicit confirmation before physical delete
- keep audit logs for every delete request and result
- support a dry-run preview that does not mutate anything

Recommended application settings:

- `ALLOW_PHYSICAL_DELETE=true|false`
- `ALLOWED_DATA_ROOTS=/mounted/geoserver_data/data`
- `PROTECTED_WORKSPACES=...`

## Reuse From Current Script

The current script already contains the hard parts of inventory logic.

Good candidates to extract into reusable modules:

- path normalization
- catalog parsing
- store path resolution
- filesystem scan logic
- orphan detection
- row/status modeling

The CLI should become a thin wrapper around those shared services instead of being the only entry point.

## Suggested Repository Layout

```text
app/
  main.py
  config.py
  db.py
  models.py
  schemas.py
  services/
    inventory.py
    geoserver.py
    deletion.py
    jobs.py
  routes/
    stores.py
    jobs.py
    delete.py
  templates/
    base.html
    stores.html
    _stores_table.html
    delete_preview.html
    job_detail.html
  static/
    app.css
    app.js
docker/
  Dockerfile.app
```

## HTMX Usage Boundaries

Use HTMX for:

- table refresh
- paging
- sorting
- filter submission
- job status polling

Do not use HTMX as a substitute for backend design. Business logic, delete planning, and safety checks stay on the server.

## Performance Notes

To support tens of thousands of stores:

- inventory generation should remain filesystem-first
- scans should run in background jobs
- the UI should read from the database, not rescan live on every page load
- table queries must be paged
- filters and sorting must be server-side

Do not embed all report rows into one HTML document.

## Authentication

If this runs in an isolated trusted admin network, the first version can be simple.

Reasonable first options:

- reverse proxy auth
- HTTP basic auth at ingress
- application login backed by local config

If multiple admins will use the tool, add per-user audit identity early.

## Logging And Audit

Keep two separate concepts:

- operational logs for debugging
- audit events for user actions

Audit should capture:

- who requested scan/delete
- what was selected
- what was blocked
- what was deleted in GeoServer
- what was deleted on disk
- failures and partial failures

## Risks

Main technical risks:

- deleting shared data used by multiple stores
- invalid or stale catalog entries
- partial failure between REST delete and filesystem delete
- very large scans blocking the app if not pushed into jobs
- path traversal or bad path normalization

These risks are manageable if deletion planning and job execution are explicit services rather than template-side logic.

## Recommended Delivery Plan

### Phase 1

- extract shared inventory logic from the script
- add database and scan snapshot persistence
- build `/stores` page with server-side paging and filters
- add `POST /scan` and job tracking

### Phase 2

- add row selection
- add deletion preview page
- add safe GeoServer store deletion
- add audit log

### Phase 3

- add physical data deletion
- add protected workspace and allowed-root configuration
- add partial refresh optimization

## Conclusion

The recommended architecture is a Dockerized `FastAPI + Jinja/HTMX` application with:

- filesystem-first inventory generation
- server-side paging/filtering/sorting
- background scan and delete jobs
- explicit deletion preview and safety checks
- local bundled dependencies only

This keeps the system simple enough to deliver, but structured enough to safely replace the current manual cleanup flow.
