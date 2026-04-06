# App Design Review

## Summary

This document now reviews the implemented GeoServer Cleaner architecture rather than the earlier migration target.

The application is already a backend-driven operational tool with:

- a `FastAPI` web UI for operators
- an MCP server for agent workflows
- a shared reporting/inventory package under `app/reporting`
- snapshot and job persistence in `SQLite`
- background scan and delete execution through an in-process job manager

The stack choice remains correct. `FastAPI + Jinja2 + HTMX + SQLite` is a strong fit for an internal admin application that needs large server-side tables, explicit destructive workflows, and low deployment complexity.

## Current State

### Implemented runtime model

Current product interfaces:

- web UI: [app/main.py](c:/Alex/work/geoserver_cleaner/app/main.py)
- MCP server: [app/mcp/server.py](c:/Alex/work/geoserver_cleaner/app/mcp/server.py)
- standalone report CLI: [app/reporting/cli.py](c:/Alex/work/geoserver_cleaner/app/reporting/cli.py)

Current shared backend modules:

- reporting core: [app/reporting/core.py](c:/Alex/work/geoserver_cleaner/app/reporting/core.py)
- report rendering: [app/reporting/render.py](c:/Alex/work/geoserver_cleaner/app/reporting/render.py)
- scan orchestration: [app/services/inventory.py](c:/Alex/work/geoserver_cleaner/app/services/inventory.py)
- delete planning and execution: [app/services/deletion.py](c:/Alex/work/geoserver_cleaner/app/services/deletion.py)
- GeoServer REST operations: [app/services/geoserver.py](c:/Alex/work/geoserver_cleaner/app/services/geoserver.py)
- snapshot query/export helpers: [app/services/snapshots.py](c:/Alex/work/geoserver_cleaner/app/services/snapshots.py)
- persistence layer: [app/db.py](c:/Alex/work/geoserver_cleaner/app/db.py)
- job runner: [app/jobs.py](c:/Alex/work/geoserver_cleaner/app/jobs.py)

### What the application does today

Implemented workflow:

1. queue an inventory scan
2. persist a completed snapshot in SQLite
3. review the latest snapshot on `/stores`
4. filter, sort, and page server-side
5. preview store deletion
6. execute GeoServer REST store deletion in a background job
7. refresh the inventory automatically after delete completion

Implemented behavior:

- filesystem-first inventory discovery with REST fallback
- orphan detection under `data_dir/data`
- CSV and HTML exports from stored snapshots
- MCP access to the same operational model
- audit event recording for delete execution

## Architecture Review

### 1. Backend-driven UI

This decision is correct and is implemented consistently.

The browser does not own business logic. The server owns:

- filtering, sorting, and paging
- deletion planning
- job orchestration
- snapshot export generation

HTMX is used in the right places:

- table refresh
- paging and sort updates
- job fragment polling

This keeps the UI simple and avoids shipping a large client-side app for a primarily operational workflow.

### 2. Inventory and reporting logic

The earlier recommendation to extract reusable logic from the old script has now been completed.

That logic lives under `app/reporting`:

- `core.py` contains GeoServer client access, catalog parsing, path handling, filesystem scanning, orphan detection, and row modeling
- `render.py` contains CSV and HTML generation
- `cli.py` is now a thin standalone wrapper over those shared services

This is the right shape. It gives the web app, MCP server, and CLI one common implementation instead of duplicating scan/report logic.

### 3. Persistence and jobs

`SQLite` remains a good first datastore for the current deployment model.

Current persisted tables:

- `inventory_runs`
- `stores`
- `jobs`
- `audit_events`

This is simpler than the earlier proposed schema and matches what the product actually needs today. In particular:

- there is no separate `job_items` table yet
- job progress is stored in `jobs.metadata_json`
- snapshots are immutable enough for the current read model

The in-process threaded `JobManager` is also a reasonable v1 choice for an internal tool. It is easy to operate and already gives scan/delete progress with low infrastructure overhead.

### 4. Deletion model

This is the most important area where the old review was out of date.

Current deletion behavior:

- store deletion is always performed through GeoServer REST
- orphan rows are report-only and cannot be deleted
- the app does not directly delete filesystem data
- the preview still determines whether data is internal, external, unresolved, or shared

This is safer than the earlier design draft that assumed direct disk deletion by the app.

The current preview model is appropriate because it:

- blocks non-store rows
- blocks incomplete metadata
- distinguishes internal vs external/unresolved/shared paths
- uses normalized path ownership checks against the current snapshot

The wording `Delete Data = Yes` currently means GeoServer may purge data for an internal store. It does not mean the application itself deletes files.

### 5. MCP integration

The old review did not cover MCP, but it is now part of the actual architecture and should be treated as first-class.

This is a good design choice because:

- it reuses the same snapshot and job model as the web app
- it avoids duplicating operational logic
- it exposes safe, bounded administrative workflows to agents

The optional streamable HTTP mount under `/mcp` is also cleanly integrated through the app lifespan.

## Implemented Endpoints

Current web routes:

- `GET /`
- `GET /stores`
- `GET /stores/table`
- `POST /scan`
- `GET /jobs/{job_id}`
- `GET /jobs/{job_id}/header`
- `GET /jobs/{job_id}/status`
- `POST /delete/preview`
- `POST /delete/execute`
- `GET /reports/latest.csv`
- `GET /reports/latest.html`

This is a coherent surface area for the current operator workflow.

## Strengths

- The backend is intentionally simple and operationally realistic.
- Shared logic is now correctly centralized under `app/reporting`.
- Server-side table rendering avoids loading full inventories into the browser.
- The delete workflow is explicit and conservative.
- Snapshot exports are derived from persisted runs instead of live rescans.
- MCP uses the same backend model instead of becoming a forked interface.
- Docker packaging and local security scanning are already integrated into the repo workflow.

## Gaps And Recommendations

### 1. Authentication and actor identity

This remains the biggest product gap.

Current state:

- no app-level authentication
- audit events do not capture a concrete user identity

Recommended next step:

- add reverse-proxy auth or ingress auth first
- thread actor identity into delete and scan audit events

### 2. Job execution robustness

The current in-process thread model is acceptable, but it has known limits:

- jobs are tied to the app process lifetime
- there is no recovery for in-flight work after restart
- only one queued/running job is allowed

Recommended next step:

- keep the current model for now
- move to an external worker only if uptime/concurrency requirements justify it

### 3. Route/module organization

The current code is still compact enough, but [app/main.py](c:/Alex/work/geoserver_cleaner/app/main.py) owns many routes directly.

Recommended next step:

- split routes into feature modules only when the file becomes a maintenance burden
- do not introduce route fragmentation prematurely

### 4. Database evolution

`SQLite` is still appropriate, but future growth points are predictable:

- more concurrent users
- larger audit requirements
- more complex job history queries

Recommended next step:

- keep the schema as-is for now
- introduce `PostgreSQL` only when operational concurrency, not aesthetics, requires it

### 5. Delete semantics communication

The product is safer now than the original draft, but the terminology can still confuse users.

Recommended next step:

- keep UI and docs explicit that deletion is GeoServer-managed
- avoid implying that the app performs raw filesystem deletion

## Updated Repository Layout

```text
app/
  main.py
  config.py
  db.py
  jobs.py
  runtime.py
  reporting/
    core.py
    render.py
    cli.py
  services/
    inventory.py
    geoserver.py
    deletion.py
    snapshots.py
    job_status.py
  mcp/
    server.py
  templates/
  static/
docker/
  Dockerfile.app
tests/
```

## Conclusion

The architecture is in good shape.

The original design recommendation was directionally correct, and the current implementation now matches that direction with two important clarifications:

- the old script has been successfully converted into reusable app modules under `app/reporting`
- deletion is intentionally GeoServer-managed and not a direct filesystem-delete tool

The next meaningful improvements are not framework changes. They are product-hardening changes:

- authentication and actor identity
- stronger job durability if needed
- continued clarity around deletion safety and auditability

No architectural rewrite is warranted at this stage.
