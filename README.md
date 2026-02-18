# ais-noaa-fetch

Download [NOAA AIS](https://coast.noaa.gov/htdata/CMSP/AISDataHandler/) vessel tracking data and convert to GeoParquet.

## Install

```bash
uv sync
```

## Usage

Fetch and convert a date range in one step:

```bash
uv run ais-noaa-fetch run --start-date 2025-01-01 --end-date 2025-01-01
```

Download only:

```bash
uv run ais-noaa-fetch fetch --start-date 2024-06-01 --end-date 2024-06-30
```

Convert already-downloaded files:

```bash
uv run ais-noaa-fetch convert --year 2024
```

### Options

| Flag | Subcommands | Description |
|------|-------------|-------------|
| `--data-dir DIR` | all | Base directory for data files (default: `data`) |
| `--start-date YYYY-MM-DD` | `run`, `fetch` | Start of date range (required) |
| `--end-date YYYY-MM-DD` | `run`, `fetch` | End of date range (required) |
| `--year YYYY` | `convert` | Year to convert (required) |
| `--force` | `run`, `fetch` | Re-download existing files |
| `--delete-raw` | `run`, `convert` | Delete raw files after conversion |
| `--workers N` | `run`, `convert` | Parallel conversion workers (default: `1`) |

## Data layout

```
data/
├── raw/{year}/                    # Downloaded .zip or .csv.zst files
└── parquet/
    ├── broadcasts/{year}/         # Full AIS broadcast data
    │   └── ais-YYYY-MM-DD.parquet
    └── index/{year}/              # Per-MMSI daily summaries
        └── ais-YYYY-MM-DD.parquet
```

Separate folder trees make it easy to load with DuckDB wildcards:

```sql
SELECT * FROM read_parquet('data/parquet/broadcasts/**/*.parquet');
SELECT * FROM read_parquet('data/parquet/index/**/*.parquet');
```

## Broadcast schema

Each broadcast file contains 21 columns — 17 from the raw NOAA data plus 4 derived columns. Sorted by (mmsi, timestamp).

| Column | Type | Description |
|--------|------|-------------|
| `mmsi` | int32 | Maritime Mobile Service Identity |
| **`date`** | **date32** | **Derived — file date** |
| `base_date_time` | string | Original timestamp string |
| `latitude` | float64 | Latitude |
| `longitude` | float64 | Longitude |
| `sog` | float64 | Speed Over Ground |
| `cog` | float64 | Course Over Ground |
| `heading` | float64 | Heading |
| `vessel_name` | string | Vessel name |
| `imo` | string | IMO number |
| `call_sign` | string | Call sign |
| `vessel_type` | int32 | Vessel type code |
| `status` | int32 | Navigation status code |
| `length` | float64 | Vessel length |
| `width` | float64 | Vessel width |
| `draft` | float64 | Vessel draft |
| `cargo` | int32 | Cargo type code |
| `transceiver` | string | Transceiver class |
| **`timestamp`** | **timestamp(us, UTC)** | **Derived — parsed from `base_date_time`** |
| **`geometry`** | **binary (WKB)** | **Derived — WKB POINT from lon/lat** |
| **`h3_res15`** | **uint64** | **Derived — H3 cell index at resolution 15** |

## Index schema

One row per MMSI per day. Enables fast lookups and filtering without scanning the full broadcast files.

### Identity & metadata

| Column | Type | Description |
|--------|------|-------------|
| `mmsi` | string | Group key |
| `date` | date32 | File date |
| `vessel_names` | list\<string\> | Distinct non-null values observed |
| `imos` | list\<string\> | Distinct non-null values observed |
| `call_signs` | list\<string\> | Distinct non-null values observed |
| `vessel_types` | list\<int32\> | Distinct non-null values observed |
| `cargos` | list\<int32\> | Distinct non-null values observed |
| `lengths` | list\<float64\> | Distinct non-null values observed |
| `widths` | list\<float64\> | Distinct non-null values observed |
| `drafts` | list\<float64\> | Distinct non-null values observed |
| `transceiver_classes` | list\<string\> | Distinct non-null values observed |

### Message stats

| Column | Type | Description |
|--------|------|-------------|
| `message_count` | int64 | Total broadcasts |
| `first_timestamp` | timestamp(us, UTC) | Earliest broadcast |
| `last_timestamp` | timestamp(us, UTC) | Latest broadcast |
| `duration_s` | float64 | Last − first in seconds |

### Geospatial

| Column | Type | Description |
|--------|------|-------------|
| `centroid_lat` | float64 | Mean latitude |
| `centroid_lon` | float64 | Mean longitude |
| `min_lat` / `max_lat` | float64 | Latitude bounding box |
| `min_lon` / `max_lon` | float64 | Longitude bounding box |
| `distance_m` | float64 | Sum of haversine distances between consecutive broadcasts (metres) |
| `h3_cell_count` | int64 | Distinct H3 res-15 cells visited |

### Navigation

| Column | Type | Description |
|--------|------|-------------|
| `sog_min` / `sog_max` / `sog_mean` | float64 | Speed Over Ground stats |
| `max_inter_msg_speed_ms` | float64 | Max haversine distance / time delta between consecutive broadcasts (m/s) |
| `status_codes` | list\<int32\> | Distinct navigation status codes observed |

## GeoParquet

Broadcast files are [GeoParquet 1.0.0](https://geoparquet.org/) compliant — WKB-encoded Point geometries in EPSG:4326 (WGS 84), zstd compressed.

Works natively with DuckDB Spatial and GeoPandas:

```sql
-- DuckDB
LOAD spatial;
SELECT * FROM 'data/parquet/broadcasts/2025/ais-2025-01-01.parquet' LIMIT 10;
```

```python
# GeoPandas
import geopandas as gpd
gdf = gpd.read_parquet("data/parquet/broadcasts/2025/ais-2025-01-01.parquet")
```

## Supported years

- **2025+**: `.csv.zst` format
- **2015–2024**: `.zip` format

## License

[MPL-2.0](LICENSE) — Copyright (c) 2026 James Thompson
