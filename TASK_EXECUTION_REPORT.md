# GeoServer Docker Deployment And Script Test Report

## Task Summary

The task was to:

1. create a GeoServer deployment on Docker based on the official GeoServer Docker documentation
2. populate the deployment with data from Natural Earth
3. run and test `geoserver_store_report.py`
4. report whether the environment permissions were sufficient

## Permissions Assessment

Yes. The environment had the necessary permissions to execute this task.

The following capabilities were available and were used successfully:

- filesystem write access in the project directory
- outbound network access for downloading Docker images and Natural Earth datasets
- Docker engine access
- Docker Compose access
- local process execution with Python

Verified locally:

- `Docker version 29.3.0`
- `Docker Compose version v5.1.0`
- `Python 3.13.5`

## Source References

Deployment was based on the official GeoServer Docker documentation and official GeoServer Docker project:

- https://docs.geoserver.org/latest/en/user/installation/docker.html
- https://github.com/geoserver/docker

Natural Earth source data used:

- Countries shapefile:
  https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-0-countries/
- Direct download used in automation:
  https://naciscdn.org/naturalearth/10m/cultural/ne_10m_admin_0_countries.zip

- Gray Earth raster:
  https://www.naturalearthdata.com/downloads/50m-raster-data/50m-gray-earth/
- Direct download used in automation:
  https://naciscdn.org/naturalearth/50m/raster/GRAY_50M_SR_W.zip

## Files Created Or Updated

- [docker-compose.geoserver-test.yml](c:\Alex\work\geoserver_cleaner\docker-compose.geoserver-test.yml)
- [populate_geoserver_natural_earth.py](c:\Alex\work\geoserver_cleaner\populate_geoserver_natural_earth.py)
- [geoserver_store_report.py](c:\Alex\work\geoserver_cleaner\geoserver_store_report.py)
- [reports\geoserver_store_report_test.csv](c:\Alex\work\geoserver_cleaner\reports\geoserver_store_report_test.csv)
- [TASK_EXECUTION_REPORT.md](c:\Alex\work\geoserver_cleaner\TASK_EXECUTION_REPORT.md)

Runtime directories created:

- `docker\geoserver_data`
- `docker\downloads`

## Deployment Implementation

### Docker Deployment

An isolated GeoServer test deployment was created with:

- image: `docker.osgeo.org/geoserver:2.28.0`
- host port: `8081`
- container port: `8080`
- mounted data directory: `./docker/geoserver_data -> /opt/geoserver_data`
- demo data disabled with `SKIP_DEMO_DATA=true`

The deployment is defined in:

- [docker-compose.geoserver-test.yml](c:\Alex\work\geoserver_cleaner\docker-compose.geoserver-test.yml)

Container status after deployment:

- container name: `geoserver_test`
- status: `healthy`
- endpoint: `http://localhost:8081/geoserver`

Observed container state:

```text
NAME             IMAGE                               COMMAND                  SERVICE          CREATED         STATUS                   PORTS
geoserver_test   docker.osgeo.org/geoserver:2.28.0   "bash /opt/startup.sh"   geoserver_test   7 minutes ago   Up 7 minutes (healthy)   0.0.0.0:8081->8080/tcp, [::]:8081->8080/tcp
```

### Data Population

The helper script:

- downloaded the Natural Earth countries shapefile zip
- downloaded the Natural Earth Gray Earth raster zip
- extracted the files into the mounted GeoServer data directory under:
  - `docker\geoserver_data\data\naturalearth\vector`
  - `docker\geoserver_data\data\naturalearth\raster`
- created a test orphan directory:
  - `docker\geoserver_data\data\orphaned_demo`
- created a GeoServer workspace:
  - `naturalearth`
- published:
  - shapefile store `ne_admin0_countries`
  - GeoTIFF store `ne_gray_world`

Published catalog contents verified through REST:

- datastore:
  - `ne_admin0_countries`
- coveragestore:
  - `ne_gray_world`

## Execution Details

### Commands Executed

Deployment:

```powershell
docker compose -f docker-compose.geoserver-test.yml up -d
```

Population:

```powershell
python populate_geoserver_natural_earth.py --base-dir .
```

Report test:

```powershell
python geoserver_store_report.py `
  --geoserver-url http://localhost:8081/geoserver `
  --username admin `
  --password geoserver `
  --data-dir .\docker\geoserver_data `
  --output-csv .\reports\geoserver_store_report_test.csv
```

## Issue Encountered And Resolution

### Initial Issue

The first automation attempt used GeoServer REST `external.shp` and `external.geotiff` endpoints.

That failed because the endpoint rejected the path forms used during automation. The data files were present in the mounted directory, but GeoServer returned HTTP 400 during external publish attempts.

### Resolution

The population logic was changed to the explicit REST object creation flow:

- create datastore / coveragestore with JSON
- publish feature type / coverage with JSON

This approach worked reliably and also preserved the desired `file:data/...` path style in the GeoServer catalog, which is important for testing the reporting script against realistic store paths.

### Script Fix Discovered During Testing

The first test run of `geoserver_store_report.py` incorrectly flagged Natural Earth `README` and `VERSION` side files as orphaned.

Cause:

- file-bundle matching for single-file stores was too narrow

Fix applied:

- broadened matching to claim the full same-prefix file family for Shapefile, GeoTIFF, and GeoPackage-style bundles

Result:

- report output changed from `2 store rows and 5 orphan rows`
- to `2 store rows and 1 orphan rows`

The remaining orphan row is the intentionally created test directory.

## Test Results

### Report Execution

`geoserver_store_report.py` completed successfully against the live Docker deployment.

Output file:

- [reports\geoserver_store_report_test.csv](c:\Alex\work\geoserver_cleaner\reports\geoserver_store_report_test.csv)

Observed completion message:

```text
Wrote 2 store rows and 1 orphan rows to .\reports\geoserver_store_report_test.csv
```

### Validated Store Rows

Store 1:

- workspace: `naturalearth`
- store: `ne_admin0_countries`
- type: `Shapefile`
- layer names: `ne_10m_admin_0_countries`
- configured path: `file:data/naturalearth/vector/ne_10m_admin_0_countries.shp`
- path kind: `file`
- size bytes: `9726410`
- file count: `7`
- status: `ok`

Store 2:

- workspace: `naturalearth`
- store: `ne_gray_world`
- type: `GeoTIFF`
- layer names: `ne_gray_world`
- configured path: `file:data/naturalearth/raster/GRAY_50M_SR_W.tif`
- path kind: `file`
- size bytes: `58435806`
- file count: `5`
- status: `ok`

### Validated Orphan Detection

The report correctly detected the intentionally created orphan directory:

- path: `docker\geoserver_data\data\orphaned_demo`
- path kind: `directory`
- file count: `1`
- status: `orphaned`

This confirms that orphan scanning under `data_dir/data` is functioning in the test deployment.

## Overall Outcome

The task was completed successfully.

Completed:

- Docker-based GeoServer deployment created from the official GeoServer Docker guidance
- Natural Earth data downloaded and published into GeoServer
- reporting script executed against the live deployment
- reporting script bug found during live test and fixed
- CSV output verified for both published stores and orphaned data detection

## Re-run Instructions

Start or recreate the deployment:

```powershell
docker compose -f docker-compose.geoserver-test.yml up -d
```

Populate GeoServer:

```powershell
python populate_geoserver_natural_earth.py --base-dir .
```

Run the report:

```powershell
python geoserver_store_report.py `
  --geoserver-url http://localhost:8081/geoserver `
  --username admin `
  --password geoserver `
  --data-dir .\docker\geoserver_data `
  --output-csv .\reports\geoserver_store_report_test.csv
```

## Extended Test Scope

The test deployment was later extended to include:

- an `ImageMosaic` store published from a ZIP upload of four GeoTIFF tiles
- a `GeoPackage` vector store with two published layers

### Additional Test Data

Additional Natural Earth vector dataset used:

- populated places:
  https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-populated-places/
- direct download used:
  https://naciscdn.org/naturalearth/10m/cultural/ne_10m_populated_places.zip

### Extended Published Stores

Additional stores now published in workspace `naturalearth`:

- `ne_vector_multi`
  - type: `GeoPackage`
  - layers: `countries`, `populated_places`
  - configured path: `file:data/naturalearth/vector/naturalearth_multi.gpkg`

- `ne_gray_world_mosaic_upload`
  - type: `ImageMosaic`
  - layer: `ne_gray_world_mosaic_upload`
  - configured path: `file:data/naturalearth/ne_gray_world_mosaic_upload`

### Extended Report Output

Extended CSV output:

- [geoserver_store_report_test_extended.csv](c:\Alex\work\geoserver_cleaner\reports\geoserver_store_report_test_extended.csv)

Observed completion message:

```text
Wrote 4 store rows and 1 orphan rows to .\reports\geoserver_store_report_test_extended.csv
```

Validated store rows from the extended test:

- `ne_admin0_countries`
  - type: `Shapefile`
  - layer names: `ne_10m_admin_0_countries`
  - file count: `7`
  - size bytes: `9726410`

- `ne_gray_world`
  - type: `GeoTIFF`
  - layer names: `ne_gray_world`
  - file count: `5`
  - size bytes: `58435806`

- `ne_gray_world_mosaic_upload`
  - type: `ImageMosaic`
  - layer names: `ne_gray_world_mosaic_upload`
  - file count: `12`
  - size bytes: `58452923`

- `ne_vector_multi`
  - type: `GeoPackage`
  - layer names: `countries, populated_places`
  - file count: `1`
  - size bytes: `15634432`

Validated orphan detection from the extended test:

- only the intentional orphan remained:
  - `data\orphaned_demo`

### Implementation Changes For The Extended Test

The population helper was updated to:

- download the Natural Earth populated places dataset
- create a multi-layer GeoPackage using QGIS Python
- create four raster tiles using `gdal_translate`
- upload the image mosaic through GeoServer REST `file.imagemosaic`
- keep staging artifacts outside `data_dir/data` so they do not pollute orphan detection

The updated helper script is:

- [populate_geoserver_natural_earth.py](c:\Alex\work\geoserver_cleaner\populate_geoserver_natural_earth.py)

## Report Refactor And Feature Update

The reporting script was later refactored and extended with the following changes:

- hardened handling of invalid GeoServer REST responses for individual stores and store lists
- added logging for major execution steps
- changed `size_gb` formatting to 2 decimal places
- added `--exclude-workspaces` so specific workspaces can be omitted from report rows
- prevented excluded workspaces from being treated as orphaned data
- added a sortable HTML report with filtering and summary cards

### Files Updated

- [geoserver_store_report.py](c:\Alex\work\geoserver_cleaner\geoserver_store_report.py)
- [README.md](c:\Alex\work\geoserver_cleaner\README.md)
- [TASK_EXECUTION_REPORT.md](c:\Alex\work\geoserver_cleaner\TASK_EXECUTION_REPORT.md)
- [tests\test_geoserver_store_report.py](c:\Alex\work\geoserver_cleaner\tests\test_geoserver_store_report.py)
- [.github\workflows\ci.yml](c:\Alex\work\geoserver_cleaner\.github\workflows\ci.yml)

### Automated Tests Added

Unit tests were added for:

- invalid store-list REST failures continuing without aborting the run
- excluded workspaces not appearing in report rows and not becoming orphaned
- `size_gb` formatting to 2 decimal places
- HTML report generation and sortable UI markers

Executed test command:

```powershell
python -m unittest discover -s tests -v
```

Observed result:

```text
Ran 4 tests in 0.059s
OK
```

### Live Verification Against Docker GeoServer

The refactored script was executed against the Docker test fixture.

Normal run:

```powershell
python geoserver_store_report.py `
  --geoserver-url http://localhost:8081/geoserver `
  --username admin `
  --password geoserver `
  --data-dir .\docker\geoserver_data `
  --output-csv .\reports\geoserver_store_report_test_refactored.csv `
  --log-level INFO
```

Observed result:

```text
Wrote 4 store rows and 1 orphan rows to .\reports\geoserver_store_report_test_refactored.csv and C:\Alex\work\geoserver_cleaner\reports\geoserver_store_report_test_refactored.html
```

Generated outputs:

- [geoserver_store_report_test_refactored.csv](c:\Alex\work\geoserver_cleaner\reports\geoserver_store_report_test_refactored.csv)
- [geoserver_store_report_test_refactored.html](c:\Alex\work\geoserver_cleaner\reports\geoserver_store_report_test_refactored.html)

Excluded workspace run:

```powershell
python geoserver_store_report.py `
  --geoserver-url http://localhost:8081/geoserver `
  --username admin `
  --password geoserver `
  --data-dir .\docker\geoserver_data `
  --output-csv .\reports\geoserver_store_report_excluded.csv `
  --exclude-workspaces naturalearth `
  --log-level INFO
```

Observed result:

```text
Wrote 0 store rows and 1 orphan rows to .\reports\geoserver_store_report_excluded.csv and C:\Alex\work\geoserver_cleaner\reports\geoserver_store_report_excluded.html
```

This confirmed that:

- the live catalog still reports correctly after the refactor
- the HTML report is generated successfully
- excluded workspace data is not marked as orphaned
