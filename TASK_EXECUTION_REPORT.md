# GeoServer Cleaner 2.5.0 GHCR Validation Report

## Release Summary

- Repository: `AShapira/geoserver_cleaner`
- Release tag: `2.5.0`
- Release commit SHA: `03021d20227427022d7fa2fe1f5352a54389cff4`
- Tag commit SHA: `03021d20227427022d7fa2fe1f5352a54389cff4`
- Validated image: `ghcr.io/ashapira/geoserver-cleaner:2.5.0`
- Pulled image digest: `ghcr.io/ashapira/geoserver-cleaner@sha256:07b800ddb78889d6e60350e7bba409b8b19248f93eab65d4a917ecc35870a5db`
- Running container image id: `sha256:5e8db265ffc35c90ac785c1ccadd6181e37016d858d9ccb7337a5930f6fb5727`

## Publish Workflow

- Workflow: `Publish GeoServer Cleaner Image`
- Run URL: https://github.com/AShapira/geoserver_cleaner/actions/runs/24323632542
- Run status: completed
- Run conclusion: success

## Validation Environment

- Validation host directory: `C:\Alex\work\geoserver_cleaner`
- GeoServer fixture compose: `geoserver_test/docker-compose.geoserver-test.yml`
- App compose: `docker-compose.production.yml`
- Validation env source: `.env.production.local` with `GEOSERVER_CLEANER_TAG=2.5.0` forced in a temporary env file
- GeoServer base URL during validation: `http://127.0.0.1:8081/geoserver`
- Web UI endpoint: `http://127.0.0.1:8000/stores`
- MCP endpoint: `http://127.0.0.1:8000/mcp/`

## Executed Checks

- GeoServer fixture status: HTTP 200
- UI status: HTTP 200
- MCP status: HTTP 406
- Inventory scan request: HTTP 200 redirect to `/jobs/20`
- Inventory scan job id: `20`
- CSV export status: HTTP 200
- App container id: `6ba3daa7e382a9111d44c5de5e18d257deb640c3420e2d9b2b6564ed8e2566be`
- GeoServer fixture container id: `f87fc291ef0b77173f9d9ca579380a7a02e2ac759a915b6cee7683cfc014679c`

## Scan and Endpoint Results

- `/stores` responded successfully and served the web UI.
- `/mcp/` was reachable with HTTP MCP enabled.
- A fresh inventory scan was triggered over HTTP and reached the completed state.
- `/reports/latest.csv` responded successfully after the scan completed.
- The running app used the published GHCR image rather than a local build.

## Log Summary

```text
03:04:18 INFO geoserver_cleaner.run Starting runtime entrypoint
03:04:19 INFO geoserver_cleaner.runtime Database initialized
03:04:19 INFO geoserver_cleaner.runtime Application runtime initialized
03:04:19 INFO geoserver_cleaner Creating FastAPI application
03:04:19 INFO geoserver_cleaner.mcp Configured MCP streamable HTTP app
03:04:19 INFO geoserver_cleaner MCP HTTP enabled
03:04:19 INFO uvicorn.error Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
03:04:58 INFO geoserver_cleaner.inventory Inventory scan completed
03:04:59 INFO geoserver_cleaner CSV export requested
```

## Isolated-Network Readiness Conclusion

The published image `ghcr.io/ashapira/geoserver-cleaner:2.5.0` started successfully with the production compose file, served `/stores` and `/mcp/`, completed an inventory scan against the configured GeoServer fixture, and served a snapshot export. The runtime validation and codebase behavior indicate that steady-state network dependency is the configured GeoServer endpoint rather than external internet services.

This validation did not add a host-level firewall block; the conclusion is based on successful execution of the published image, the captured container logs, and the repo code paths that only initiate outbound HTTP toward `GEOSERVER_URL` during normal operation.

## Commands

## git rev-parse HEAD

```text
03021d20227427022d7fa2fe1f5352a54389cff4
```

## git rev-list -n 1 2.5.0

```text
03021d20227427022d7fa2fe1f5352a54389cff4
```

## python -m unittest discover -s tests -v

```text
06:04:09 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:09 INFO geoserver_cleaner.runtime Database initialized
06:04:09 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:09 INFO geoserver_cleaner Creating FastAPI application
06:04:09 INFO geoserver_cleaner FastAPI lifespan starting
06:04:09 INFO geoserver_cleaner MCP HTTP disabled
06:04:09 INFO geoserver_cleaner HTTP request started
06:04:09 INFO geoserver_cleaner HTTP request completed
06:04:09 INFO httpx HTTP Request: GET http://testserver/jobs/1 "HTTP/1.1 200 OK"
06:04:09 INFO geoserver_cleaner FastAPI lifespan stopping
test_completed_job_status_shows_progress_once (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_completed_job_status_shows_progress_once) ... ok
06:04:09 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:09 INFO geoserver_cleaner.runtime Database initialized
06:04:09 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:09 INFO geoserver_cleaner Creating FastAPI application
06:04:09 INFO geoserver_cleaner FastAPI lifespan starting
06:04:09 INFO geoserver_cleaner MCP HTTP disabled
06:04:09 INFO geoserver_cleaner HTTP request started
06:04:09 INFO geoserver_cleaner Delete execution requested
06:04:09 INFO geoserver_cleaner.deletion Building delete preview
06:04:09 INFO geoserver_cleaner.deletion Delete preview built
06:04:09 INFO geoserver_cleaner Delete job queued
06:04:09 INFO geoserver_cleaner HTTP request completed
06:04:09 INFO httpx HTTP Request: POST http://testserver/delete/execute "HTTP/1.1 303 See Other"
06:04:09 INFO geoserver_cleaner FastAPI lifespan stopping
test_delete_execute_filters_to_valid_store_rows (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_delete_execute_filters_to_valid_store_rows) ... ok
06:04:09 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:09 INFO geoserver_cleaner.runtime Database initialized
06:04:09 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:09 INFO geoserver_cleaner Creating FastAPI application
06:04:10 INFO geoserver_cleaner FastAPI lifespan starting
06:04:10 INFO geoserver_cleaner MCP HTTP disabled
06:04:10 INFO geoserver_cleaner HTTP request started
06:04:10 INFO geoserver_cleaner HTTP request completed
06:04:10 INFO httpx HTTP Request: GET http://testserver/jobs/1/status "HTTP/1.1 200 OK"
06:04:10 INFO geoserver_cleaner FastAPI lifespan stopping
test_delete_job_status_fragment_shows_delete_counts (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_delete_job_status_fragment_shows_delete_counts) ... ok
06:04:10 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:10 INFO geoserver_cleaner.runtime Database initialized
06:04:10 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:10 INFO geoserver_cleaner Creating FastAPI application
06:04:10 INFO geoserver_cleaner FastAPI lifespan starting
06:04:10 INFO geoserver_cleaner MCP HTTP disabled
06:04:10 INFO geoserver_cleaner HTTP request started
06:04:10 INFO geoserver_cleaner Delete preview requested
06:04:10 INFO geoserver_cleaner.deletion Building delete preview
06:04:10 INFO geoserver_cleaner.deletion Delete preview built
06:04:10 INFO geoserver_cleaner Delete preview built
06:04:10 INFO geoserver_cleaner HTTP request completed
06:04:10 INFO httpx HTTP Request: POST http://testserver/delete/preview "HTTP/1.1 200 OK"
06:04:10 INFO geoserver_cleaner FastAPI lifespan stopping
test_delete_preview_marks_external_store_as_configuration_only (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_delete_preview_marks_external_store_as_configuration_only) ... ok
06:04:10 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:10 INFO geoserver_cleaner.runtime Database initialized
06:04:10 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:10 INFO geoserver_cleaner Creating FastAPI application
06:04:10 INFO geoserver_cleaner FastAPI lifespan starting
06:04:10 INFO geoserver_cleaner MCP HTTP disabled
06:04:10 INFO geoserver_cleaner HTTP request started
06:04:10 INFO geoserver_cleaner Delete preview requested
06:04:10 INFO geoserver_cleaner.deletion Building delete preview
06:04:10 INFO geoserver_cleaner.deletion Delete preview built
06:04:10 INFO geoserver_cleaner Delete preview built
06:04:10 INFO geoserver_cleaner HTTP request completed
06:04:10 INFO httpx HTTP Request: POST http://testserver/delete/preview "HTTP/1.1 200 OK"
06:04:10 INFO geoserver_cleaner FastAPI lifespan stopping
test_delete_preview_marks_internal_store_as_data_deletable (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_delete_preview_marks_internal_store_as_data_deletable) ... ok
06:04:10 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:10 INFO geoserver_cleaner.runtime Database initialized
06:04:10 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:10 INFO geoserver_cleaner Creating FastAPI application
06:04:10 INFO geoserver_cleaner FastAPI lifespan starting
06:04:10 INFO geoserver_cleaner MCP HTTP disabled
06:04:10 INFO geoserver_cleaner HTTP request started
06:04:10 INFO geoserver_cleaner Delete preview requested
06:04:10 INFO geoserver_cleaner.deletion Building delete preview
06:04:10 INFO geoserver_cleaner.deletion Delete preview built
06:04:10 INFO geoserver_cleaner Delete preview built
06:04:10 INFO geoserver_cleaner HTTP request completed
06:04:10 INFO httpx HTTP Request: POST http://testserver/delete/preview "HTTP/1.1 200 OK"
06:04:10 INFO geoserver_cleaner FastAPI lifespan stopping
test_delete_preview_marks_unresolved_store_as_configuration_only (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_delete_preview_marks_unresolved_store_as_configuration_only) ... ok
06:04:10 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:10 INFO geoserver_cleaner.runtime Database initialized
06:04:10 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:10 INFO geoserver_cleaner Creating FastAPI application
06:04:10 INFO geoserver_cleaner.deletion Building delete preview
06:04:10 INFO geoserver_cleaner.deletion Delete preview built
06:04:10 INFO geoserver_cleaner.deletion Executing delete job
06:04:10 INFO geoserver_cleaner.deletion Delete job execution finished
test_execute_delete_job_does_not_remove_files (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_execute_delete_job_does_not_remove_files) ... ok
06:04:10 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:10 INFO geoserver_cleaner.runtime Database initialized
06:04:10 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:10 INFO geoserver_cleaner Creating FastAPI application
06:04:10 INFO geoserver_cleaner.geoserver Sending GeoServer delete request
06:04:10 INFO geoserver_cleaner.geoserver GeoServer delete request completed
test_geoserver_delete_uses_recurse_and_purge_all (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_geoserver_delete_uses_recurse_and_purge_all) ... ok
06:04:10 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:11 INFO geoserver_cleaner.runtime Database initialized
06:04:11 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:11 INFO geoserver_cleaner Creating FastAPI application
06:04:11 DEBUG asyncio Using proactor: IocpProactor
06:04:11 INFO geoserver_cleaner FastAPI lifespan starting
06:04:11 INFO geoserver_cleaner MCP HTTP disabled
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:11 INFO geoserver_cleaner HTTP request completed
06:04:11 INFO httpx HTTP Request: GET http://testserver/stores "HTTP/1.1 200 OK"
06:04:11 INFO geoserver_cleaner FastAPI lifespan stopping
test_http_requests_are_logged_to_json_file (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_http_requests_are_logged_to_json_file) ... ok
06:04:11 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:11 INFO geoserver_cleaner.runtime Database initialized
06:04:11 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:11 INFO geoserver_cleaner Creating FastAPI application
06:04:11 INFO geoserver_cleaner FastAPI lifespan starting
06:04:11 INFO geoserver_cleaner MCP HTTP disabled
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:11 INFO geoserver_cleaner HTTP request completed
06:04:11 INFO httpx HTTP Request: GET http://testserver/jobs/1/header "HTTP/1.1 200 OK"
06:04:11 INFO geoserver_cleaner FastAPI lifespan stopping
test_job_header_fragment_shows_live_progress_summary (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_job_header_fragment_shows_live_progress_summary) ... ok
06:04:11 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:11 INFO geoserver_cleaner.runtime Database initialized
06:04:11 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:11 INFO geoserver_cleaner Creating FastAPI application
06:04:11 INFO geoserver_cleaner FastAPI lifespan starting
06:04:11 INFO geoserver_cleaner MCP HTTP disabled
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:11 INFO geoserver_cleaner HTTP request completed
06:04:11 INFO httpx HTTP Request: GET http://testserver/jobs/1/status "HTTP/1.1 200 OK"
06:04:11 INFO geoserver_cleaner FastAPI lifespan stopping
test_job_status_fragment_shows_progress_and_eta (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_job_status_fragment_shows_progress_and_eta) ... ok
06:04:11 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:11 INFO geoserver_cleaner.runtime Database initialized
06:04:11 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:11 INFO geoserver_cleaner Creating FastAPI application
06:04:11 INFO geoserver_cleaner FastAPI lifespan starting
06:04:11 INFO geoserver_cleaner MCP HTTP disabled
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:11 INFO geoserver_cleaner CSV export requested
06:04:11 INFO geoserver_cleaner HTTP request completed
06:04:11 INFO httpx HTTP Request: GET http://testserver/reports/latest.csv "HTTP/1.1 200 OK"
06:04:11 INFO geoserver_cleaner FastAPI lifespan stopping
test_latest_csv_download_uses_completed_snapshot (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_latest_csv_download_uses_completed_snapshot) ... ok
06:04:11 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:11 INFO geoserver_cleaner.runtime Database initialized
06:04:11 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:11 INFO geoserver_cleaner Creating FastAPI application
06:04:11 INFO geoserver_cleaner FastAPI lifespan starting
06:04:11 INFO geoserver_cleaner MCP HTTP disabled
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:11 INFO geoserver_cleaner HTML export requested
06:04:11 INFO geoserver_cleaner HTTP request completed
06:04:11 INFO httpx HTTP Request: GET http://testserver/reports/latest.html "HTTP/1.1 200 OK"
06:04:11 INFO geoserver_cleaner FastAPI lifespan stopping
test_latest_html_download_uses_completed_snapshot (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_latest_html_download_uses_completed_snapshot) ... ok
06:04:11 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:11 INFO geoserver_cleaner.runtime Database initialized
06:04:11 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:11 INFO geoserver_cleaner Creating FastAPI application
06:04:11 INFO geoserver_cleaner FastAPI lifespan starting
06:04:11 INFO geoserver_cleaner MCP HTTP disabled
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:11 INFO geoserver_cleaner HTTP request completed
06:04:11 INFO httpx HTTP Request: GET http://testserver/mcp "HTTP/1.1 404 Not Found"
06:04:11 INFO geoserver_cleaner FastAPI lifespan stopping
test_mcp_http_path_is_disabled_by_default (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_mcp_http_path_is_disabled_by_default) ... ok
06:04:11 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:11 INFO geoserver_cleaner.runtime Database initialized
06:04:11 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:11 INFO geoserver_cleaner Creating FastAPI application
06:04:11 INFO geoserver_cleaner FastAPI lifespan starting
06:04:11 INFO geoserver_cleaner.mcp Building MCP server
06:04:11 INFO geoserver_cleaner.mcp Configured MCP streamable HTTP app
06:04:11 INFO geoserver_cleaner MCP HTTP enabled
06:04:11 INFO mcp.server.streamable_http_manager StreamableHTTP session manager started
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:11 INFO geoserver_cleaner HTTP request completed
06:04:11 INFO httpx HTTP Request: GET http://testserver/mcp "HTTP/1.1 307 Temporary Redirect"
06:04:11 INFO geoserver_cleaner HTTP request started
06:04:12 INFO geoserver_cleaner HTTP request completed
06:04:12 INFO httpx HTTP Request: GET http://testserver/mcp/mcp "HTTP/1.1 404 Not Found"
06:04:12 INFO mcp.server.streamable_http_manager StreamableHTTP session manager shutting down
06:04:12 INFO geoserver_cleaner FastAPI lifespan stopping
test_mcp_http_path_is_mounted_exactly_once_when_enabled (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_mcp_http_path_is_mounted_exactly_once_when_enabled) ... ok
06:04:12 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:12 INFO geoserver_cleaner.runtime Database initialized
06:04:12 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:12 INFO geoserver_cleaner Creating FastAPI application
06:04:12 INFO geoserver_cleaner FastAPI lifespan starting
06:04:12 INFO geoserver_cleaner MCP HTTP disabled
06:04:12 INFO geoserver_cleaner HTTP request started
06:04:12 INFO geoserver_cleaner Delete preview requested
06:04:12 INFO geoserver_cleaner.deletion Building delete preview
06:04:12 INFO geoserver_cleaner.deletion Delete preview built
06:04:12 INFO geoserver_cleaner Delete preview built
06:04:12 INFO geoserver_cleaner HTTP request completed
06:04:12 INFO httpx HTTP Request: POST http://testserver/delete/preview "HTTP/1.1 200 OK"
06:04:12 INFO geoserver_cleaner HTTP request started
06:04:12 INFO geoserver_cleaner Delete execution requested
06:04:12 INFO geoserver_cleaner.deletion Building delete preview
06:04:12 INFO geoserver_cleaner.deletion Delete preview built
06:04:12 INFO geoserver_cleaner HTTP request completed
06:04:12 INFO httpx HTTP Request: POST http://testserver/delete/execute "HTTP/1.1 400 Bad Request"
06:04:12 INFO geoserver_cleaner FastAPI lifespan stopping
test_orphan_rows_cannot_be_deleted (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_orphan_rows_cannot_be_deleted) ... ok
06:04:12 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:12 INFO geoserver_cleaner.runtime Database initialized
06:04:12 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:12 INFO geoserver_cleaner Creating FastAPI application
06:04:12 INFO geoserver_cleaner FastAPI lifespan starting
06:04:12 INFO geoserver_cleaner MCP HTTP disabled
06:04:12 INFO geoserver_cleaner HTTP request started
06:04:12 INFO geoserver_cleaner HTTP request completed
06:04:12 INFO httpx HTTP Request: GET http://testserver/jobs/1/status "HTTP/1.1 200 OK"
06:04:12 INFO geoserver_cleaner FastAPI lifespan stopping
test_scan_job_status_fragment_shows_discovery_counts (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_scan_job_status_fragment_shows_discovery_counts) ... ok
06:04:12 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:12 INFO geoserver_cleaner.runtime Database initialized
06:04:12 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:12 INFO geoserver_cleaner Creating FastAPI application
06:04:12 INFO geoserver_cleaner FastAPI lifespan starting
06:04:12 INFO geoserver_cleaner MCP HTTP disabled
06:04:12 INFO geoserver_cleaner HTTP request started
06:04:12 INFO geoserver_cleaner Inventory scan requested
06:04:12 INFO geoserver_cleaner Inventory scan queued
06:04:12 INFO geoserver_cleaner HTTP request completed
06:04:12 INFO httpx HTTP Request: POST http://testserver/scan "HTTP/1.1 303 See Other"
06:04:12 INFO geoserver_cleaner FastAPI lifespan stopping
test_scan_route_passes_excluded_workspaces_to_job_manager (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_scan_route_passes_excluded_workspaces_to_job_manager) ... ok
test_settings_include_logging_defaults (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_settings_include_logging_defaults) ... ok
06:04:12 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:12 INFO geoserver_cleaner.runtime Database initialized
06:04:12 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:12 INFO geoserver_cleaner Creating FastAPI application
06:04:12 INFO geoserver_cleaner FastAPI lifespan starting
06:04:12 INFO geoserver_cleaner MCP HTTP disabled
06:04:12 INFO geoserver_cleaner HTTP request started
06:04:12 INFO geoserver_cleaner HTTP request completed
06:04:12 INFO httpx HTTP Request: GET http://testserver/stores "HTTP/1.1 200 OK"
06:04:12 INFO geoserver_cleaner FastAPI lifespan stopping
test_stores_page_renders_latest_snapshot_and_download_actions (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_stores_page_renders_latest_snapshot_and_download_actions) ... ok
06:04:12 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:12 INFO geoserver_cleaner.runtime Database initialized
06:04:12 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:12 INFO geoserver_cleaner Creating FastAPI application
06:04:12 INFO geoserver_cleaner FastAPI lifespan starting
06:04:12 INFO geoserver_cleaner MCP HTTP disabled
06:04:12 INFO geoserver_cleaner HTTP request started
06:04:12 INFO geoserver_cleaner HTTP request completed
06:04:12 INFO httpx HTTP Request: GET http://testserver/stores/table?workspace=raster "HTTP/1.1 200 OK"
06:04:12 INFO geoserver_cleaner FastAPI lifespan stopping
test_stores_table_filters_by_workspace (test_geoserver_cleaner_app.GeoServerCleanerAppTests.test_stores_table_filters_by_workspace) ... ok
06:04:12 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:12 INFO geoserver_cleaner.runtime Database initialized
06:04:12 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:13 INFO geoserver_cleaner.mcp MCP tool request started
06:04:13 INFO geoserver_cleaner.deletion Building delete preview
06:04:13 INFO geoserver_cleaner.deletion Delete preview built
06:04:13 INFO geoserver_cleaner.mcp MCP tool request completed
test_delete_stores_rejects_orphans_and_starts_job_for_store_rows (test_mcp_server.GeoServerCleanerMcpTests.test_delete_stores_rejects_orphans_and_starts_job_for_store_rows) ... ok
06:04:13 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:13 INFO geoserver_cleaner.runtime Database initialized
06:04:13 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:13 INFO geoserver_cleaner.mcp MCP tool request started
06:04:13 INFO geoserver_cleaner.snapshots Writing snapshot export
06:04:13 INFO geoserver_cleaner.snapshots Snapshot export written
06:04:13 INFO geoserver_cleaner.mcp MCP tool request completed
06:04:13 INFO geoserver_cleaner.mcp MCP tool request started
06:04:13 INFO geoserver_cleaner.snapshots Writing snapshot export
06:04:13 INFO geoserver_cleaner.snapshots Snapshot export written
06:04:13 INFO geoserver_cleaner.mcp MCP tool request completed
test_export_snapshot_tools_write_files (test_mcp_server.GeoServerCleanerMcpTests.test_export_snapshot_tools_write_files) ... ok
06:04:13 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:13 INFO geoserver_cleaner.runtime Database initialized
06:04:13 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:13 INFO geoserver_cleaner.mcp MCP tool request started
06:04:13 INFO geoserver_cleaner.mcp MCP tool request completed
test_list_heaviest_stores_returns_sorted_rows (test_mcp_server.GeoServerCleanerMcpTests.test_list_heaviest_stores_returns_sorted_rows) ... ok
06:04:13 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:13 INFO geoserver_cleaner.runtime Database initialized
06:04:13 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:13 INFO geoserver_cleaner.mcp MCP tool request started
06:04:13 INFO geoserver_cleaner.mcp MCP tool request completed
test_list_orphans_returns_only_orphan_rows (test_mcp_server.GeoServerCleanerMcpTests.test_list_orphans_returns_only_orphan_rows) ... ok
06:04:13 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:13 INFO geoserver_cleaner.runtime Database initialized
06:04:13 INFO geoserver_cleaner.runtime Application runtime initialized
test_mcp_stdio_server_lists_tools_and_reads_latest_snapshot (test_mcp_server.GeoServerCleanerMcpTests.test_mcp_stdio_server_lists_tools_and_reads_latest_snapshot) ... ok
06:04:14 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:14 INFO geoserver_cleaner.runtime Database initialized
06:04:14 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:14 INFO geoserver_cleaner Creating FastAPI application
06:04:14 INFO geoserver_cleaner FastAPI lifespan starting
06:04:14 INFO geoserver_cleaner.mcp Building MCP server
06:04:14 INFO geoserver_cleaner.mcp Configured MCP streamable HTTP app
06:04:14 INFO geoserver_cleaner MCP HTTP enabled
06:04:14 INFO mcp.server.streamable_http_manager StreamableHTTP session manager started
06:04:14 INFO mcp.client.streamable_http Connecting to StreamableHTTP endpoint: http://127.0.0.1:54168/mcp/
06:04:14 INFO geoserver_cleaner HTTP request started
06:04:14 INFO mcp.server.streamable_http_manager Created new transport with session ID: e39f5b8a70de44b5ad130615ff387f96
06:04:14 INFO geoserver_cleaner HTTP request completed
06:04:14 INFO httpx HTTP Request: POST http://127.0.0.1:54168/mcp/ "HTTP/1.1 200 OK"
06:04:14 INFO mcp.client.streamable_http Received session ID: e39f5b8a70de44b5ad130615ff387f96
06:04:14 INFO geoserver_cleaner HTTP request started
06:04:14 INFO geoserver_cleaner HTTP request completed
06:04:14 INFO httpx HTTP Request: POST http://127.0.0.1:54168/mcp/ "HTTP/1.1 202 Accepted"
06:04:14 INFO geoserver_cleaner HTTP request started
06:04:14 WARN py.warnings C:\Users\shale\AppData\Roaming\Python\Python313\site-packages\anyio\streams\memory.py:189: ResourceWarning: Unclosed <MemoryObjectReceiveStream at 1cb280d1c70>
  warnings.warn(

06:04:14 INFO geoserver_cleaner HTTP request completed
06:04:14 INFO httpx HTTP Request: GET http://127.0.0.1:54168/mcp/ "HTTP/1.1 200 OK"
06:04:14 INFO geoserver_cleaner HTTP request started
06:04:14 INFO geoserver_cleaner HTTP request completed
06:04:14 INFO mcp.server.lowlevel.server Processing request of type ListToolsRequest
06:04:14 INFO httpx HTTP Request: POST http://127.0.0.1:54168/mcp/ "HTTP/1.1 200 OK"
06:04:14 INFO geoserver_cleaner HTTP request started
06:04:14 INFO geoserver_cleaner HTTP request completed
06:04:14 INFO mcp.server.lowlevel.server Processing request of type CallToolRequest
06:04:14 INFO httpx HTTP Request: POST http://127.0.0.1:54168/mcp/ "HTTP/1.1 200 OK"
06:04:14 INFO geoserver_cleaner.mcp MCP tool request started
06:04:14 INFO geoserver_cleaner.mcp MCP tool request completed
06:04:14 INFO geoserver_cleaner HTTP request started
06:04:14 INFO mcp.server.streamable_http Terminating session: e39f5b8a70de44b5ad130615ff387f96
06:04:14 INFO geoserver_cleaner HTTP request completed
06:04:14 INFO httpx HTTP Request: DELETE http://127.0.0.1:54168/mcp/ "HTTP/1.1 200 OK"
06:04:15 INFO mcp.server.streamable_http_manager StreamableHTTP session manager shutting down
06:04:15 INFO geoserver_cleaner FastAPI lifespan stopping
test_streamable_http_server_matches_stdio_tooling (test_mcp_server.GeoServerCleanerMcpTests.test_streamable_http_server_matches_stdio_tooling) ... ok
06:04:15 INFO geoserver_cleaner.runtime Initializing application runtime
06:04:15 INFO geoserver_cleaner.runtime Database initialized
06:04:15 INFO geoserver_cleaner.runtime Application runtime initialized
06:04:15 INFO geoserver_cleaner.mcp MCP tool request started
06:04:15 INFO geoserver_cleaner.mcp MCP tool request completed
test_summarize_workspace_usage_aggregates_by_workspace (test_mcp_server.GeoServerCleanerMcpTests.test_summarize_workspace_usage_aggregates_by_workspace) ... ok
test_excluded_workspace_is_not_reported_or_marked_orphan (test_reporting.GeoServerStoreReportTests.test_excluded_workspace_is_not_reported_or_marked_orphan) ... Filesystem catalog discovery failed, falling back to REST: GeoServer workspaces directory does not exist: C:\Users\shale\AppData\Local\Temp\tmp0g1mmj36\workspaces
ok
test_filesystem_catalog_inventory_uses_local_workspaces (test_reporting.GeoServerStoreReportTests.test_filesystem_catalog_inventory_uses_local_workspaces) ... ok
test_html_report_is_generated_with_sorting_ui (test_reporting.GeoServerStoreReportTests.test_html_report_is_generated_with_sorting_ui) ... ok
test_invalid_store_listing_continues (test_reporting.GeoServerStoreReportTests.test_invalid_store_listing_continues) ... Filesystem catalog discovery failed, falling back to REST: GeoServer workspaces directory does not exist: C:\Users\shale\AppData\Local\Temp\tmp500hef0f\workspaces
Failed to list datastores for workspace ws_bad: bad rest response
ok
test_reporting_cli_generates_csv_html_and_summary (test_reporting.GeoServerStoreReportTests.test_reporting_cli_generates_csv_html_and_summary) ... 06:04:15 INFO Discovered 1 workspace(s) and 1 store(s) via filesystem catalog
06:04:15 INFO Processed 1/1 store(s)
ok
test_reporting_cli_uses_app_native_default_output_name (test_reporting.GeoServerStoreReportTests.test_reporting_cli_uses_app_native_default_output_name) ... ok
test_size_gb_is_rounded_to_two_decimals (test_reporting.GeoServerStoreReportTests.test_size_gb_is_rounded_to_two_decimals) ... ok
System.Management.Automation.RemoteException
----------------------------------------------------------------------
Ran 35 tests in 6.467s
System.Management.Automation.RemoteException
OK
```

## docker compose -f geoserver_test/docker-compose.geoserver-test.yml config

```text
name: geoserver_test
services:
  geoserver_test:
    container_name: geoserver_test
    environment:
      SKIP_DEMO_DATA: "true"
    image: docker.osgeo.org/geoserver:2.28.0
    networks:
      default: null
    ports:
      - mode: ingress
        target: 8080
        published: "8081"
        protocol: tcp
    restart: unless-stopped
    volumes:
      - type: bind
        source: C:\Alex\work\geoserver_cleaner\geoserver_test\geoserver_data
        target: /opt/geoserver_data
        bind: {}
networks:
  default:
    name: geoserver_test_default
```

## docker compose --env-file [temp validation env] -f docker-compose.production.yml config

```text
name: geoserver_cleaner
services:
  geoserver-cleaner:
    environment:
      APP_DATABASE_PATH: /app_data/geoserver_cleaner.sqlite3
      APP_ENABLE_MCP_HTTP: "true"
      APP_EXPORT_DIR: /app_exports
      APP_LOG_BACKUP_COUNT: "5"
      APP_LOG_LEVEL: INFO
      APP_LOG_MAX_BYTES: "10485760"
      APP_LOG_PATH: C:/Alex/work/geoserver_cleaner/app_data/logs/geoserver_cleaner.log
      APP_MCP_HTTP_PATH: /mcp
      APP_PAGE_SIZE_DEFAULT: "100"
      APP_PAGE_SIZE_MAX: "500"
      APP_TITLE: GeoServer Cleaner
      GEOSERVER_CATALOG_SOURCE: filesystem
      GEOSERVER_DATA_DIR: /geoserver_data
      GEOSERVER_EXCLUDE_WORKSPACES: ""
      GEOSERVER_INSECURE: "false"
      GEOSERVER_PASSWORD: geoserver
      GEOSERVER_TIMEOUT: "60"
      GEOSERVER_URL: http://host.docker.internal:8081/geoserver
      GEOSERVER_USER: admin
      GEOSERVER_WORKERS: "8"
    image: ghcr.io/ashapira/geoserver-cleaner:2.5.0
    networks:
      default: null
    ports:
      - mode: ingress
        target: 8000
        published: "8000"
        protocol: tcp
    restart: unless-stopped
    volumes:
      - type: bind
        source: C:/Alex/work/geoserver_cleaner/geoserver_test/geoserver_data
        target: /geoserver_data
        bind: {}
      - type: volume
        source: geoserver-cleaner-app-data
        target: /app_data
        volume: {}
      - type: volume
        source: geoserver-cleaner-exports
        target: /app_exports
        volume: {}
networks:
  default:
    name: geoserver_cleaner_default
volumes:
  geoserver-cleaner-app-data:
    name: geoserver_cleaner_geoserver-cleaner-app-data
  geoserver-cleaner-exports:
    name: geoserver_cleaner_geoserver-cleaner-exports
```

## docker pull ghcr.io/ashapira/geoserver-cleaner:2.5.0

```text
2.5.0: Pulling from ashapira/geoserver-cleaner
Digest: sha256:07b800ddb78889d6e60350e7bba409b8b19248f93eab65d4a917ecc35870a5db
Status: Image is up to date for ghcr.io/ashapira/geoserver-cleaner:2.5.0
ghcr.io/ashapira/geoserver-cleaner:2.5.0
```

## docker compose --env-file [temp validation env] -f docker-compose.production.yml down --remove-orphans

```text
<no output>
```

## docker compose -f geoserver_test/docker-compose.geoserver-test.yml down --remove-orphans

```text
<no output>
```

## docker compose -f geoserver_test/docker-compose.geoserver-test.yml up -d

```text
 Network geoserver_test_default Creating 
 Network geoserver_test_default Created 
 Container geoserver_test Creating 
 Container geoserver_test Created 
 Container geoserver_test Starting 
 Container geoserver_test Started
```

## docker compose --env-file [temp validation env] -f docker-compose.production.yml up -d

```text
 Network geoserver_cleaner_default Creating 
 Network geoserver_cleaner_default Created 
 Container geoserver_cleaner-geoserver-cleaner-1 Creating 
 Container geoserver_cleaner-geoserver-cleaner-1 Created 
 Container geoserver_cleaner-geoserver-cleaner-1 Starting 
 Container geoserver_cleaner-geoserver-cleaner-1 Started
```

## docker image inspect ghcr.io/ashapira/geoserver-cleaner:2.5.0 --format {{json .RepoDigests}}

```text
["ghcr.io/ashapira/geoserver-cleaner@sha256:07b800ddb78889d6e60350e7bba409b8b19248f93eab65d4a917ecc35870a5db"]
```

## docker inspect 6ba3daa7e382a9111d44c5de5e18d257deb640c3420e2d9b2b6564ed8e2566be --format {{.Image}}

```text
sha256:5e8db265ffc35c90ac785c1ccadd6181e37016d858d9ccb7337a5930f6fb5727
```

## docker logs 6ba3daa7e382a9111d44c5de5e18d257deb640c3420e2d9b2b6564ed8e2566be --since 2026-04-13T03:04:07Z

```text
03:04:18 INFO geoserver_cleaner.run Starting runtime entrypoint
03:04:19 INFO geoserver_cleaner.runtime Initializing application runtime
03:04:19 INFO geoserver_cleaner.runtime Database initialized
03:04:19 INFO geoserver_cleaner.runtime Application runtime initialized
03:04:19 INFO geoserver_cleaner Creating FastAPI application
03:04:19 INFO uvicorn.error Started server process [1]
03:04:19 INFO uvicorn.error Waiting for application startup.
03:04:19 INFO geoserver_cleaner FastAPI lifespan starting
03:04:19 INFO geoserver_cleaner.mcp Building MCP server
03:04:19 INFO geoserver_cleaner.mcp Configured MCP streamable HTTP app
03:04:19 INFO geoserver_cleaner MCP HTTP enabled
03:04:19 INFO mcp.server.streamable_http_manager StreamableHTTP session manager started
03:04:19 INFO uvicorn.error Application startup complete.
03:04:19 INFO uvicorn.error Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
03:04:32 INFO geoserver_cleaner HTTP request started
03:04:32 INFO geoserver_cleaner HTTP request completed
03:04:32 INFO geoserver_cleaner HTTP request started
03:04:32 INFO mcp.server.streamable_http_manager Created new transport with session ID: 3c28743f457140a6924f1648a1403fad
03:04:32 INFO geoserver_cleaner HTTP request completed
03:04:32 INFO geoserver_cleaner HTTP request started
03:04:32 INFO geoserver_cleaner Inventory scan requested
03:04:32 INFO geoserver_cleaner.jobs Queueing inventory scan job
03:04:32 INFO geoserver_cleaner.jobs Inventory scan job started
03:04:32 INFO geoserver_cleaner.jobs Inventory scan job queued
03:04:32 INFO geoserver_cleaner Inventory scan queued
03:04:32 INFO geoserver_cleaner HTTP request completed
03:04:32 INFO geoserver_cleaner HTTP request started
03:04:32 INFO geoserver_cleaner.inventory Starting inventory scan
03:04:32 INFO geoserver_cleaner HTTP request completed
03:04:32 INFO geoserver_cleaner HTTP request started
03:04:32 INFO geoserver_cleaner HTTP request completed
03:04:34 INFO geoserver_cleaner HTTP request started
03:04:34 INFO geoserver_cleaner HTTP request completed
03:04:36 INFO geoserver_cleaner HTTP request started
03:04:36 INFO geoserver_cleaner HTTP request completed
03:04:39 INFO geoserver_cleaner HTTP request started
03:04:39 INFO geoserver_cleaner HTTP request completed
03:04:41 INFO geoserver_cleaner HTTP request started
03:04:41 INFO geoserver_cleaner HTTP request completed
03:04:43 INFO geoserver_cleaner HTTP request started
03:04:43 INFO geoserver_cleaner HTTP request completed
03:04:45 INFO geoserver_cleaner HTTP request started
03:04:45 INFO geoserver_cleaner HTTP request completed
03:04:47 INFO geoserver_cleaner HTTP request started
03:04:47 INFO geoserver_cleaner HTTP request completed
03:04:49 INFO geoserver_cleaner HTTP request started
03:04:49 INFO geoserver_cleaner HTTP request completed
03:04:51 INFO geoserver_cleaner HTTP request started
03:04:51 INFO geoserver_cleaner HTTP request completed
03:04:53 INFO geoserver_cleaner HTTP request started
03:04:53 INFO geoserver_cleaner HTTP request completed
03:04:55 INFO geoserver_cleaner HTTP request started
03:04:55 INFO geoserver_cleaner HTTP request completed
03:04:56 INFO geoserver_cleaner.inventory Discovered 7 workspace(s) and 625 store(s) via filesystem catalog
03:04:57 INFO geoserver_cleaner HTTP request started
03:04:57 INFO geoserver_cleaner HTTP request completed
03:04:58 INFO geoserver_cleaner.inventory Inventory scan completed
03:04:59 INFO geoserver_cleaner.jobs Inventory scan job completed
03:04:59 INFO geoserver_cleaner HTTP request started
03:04:59 INFO geoserver_cleaner HTTP request completed
03:04:59 INFO geoserver_cleaner HTTP request started
03:04:59 INFO geoserver_cleaner CSV export requested
03:04:59 INFO geoserver_cleaner HTTP request completed
```

