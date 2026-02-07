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

## Data layout

```
data/
├── raw/{year}/        # Downloaded .zip or .csv.zst files
└── parquet/{year}/    # Converted .parquet files
```

## Parquet schema

Each output file contains 20 columns — 17 from the raw NOAA data plus 3 derived columns.

| Column | Type | Description |
|--------|------|-------------|
| `mmsi` | string | Maritime Mobile Service Identity |
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

## GeoParquet

Output files are [GeoParquet 1.1.0](https://geoparquet.org/) compliant — WKB-encoded Point geometries in EPSG:4326 (WGS 84), zstd compressed.

Works natively with DuckDB Spatial and GeoPandas:

```sql
-- DuckDB
LOAD spatial;
SELECT * FROM 'data/parquet/2025/ais-2025-01-01.parquet' LIMIT 10;
```

```python
# GeoPandas
import geopandas as gpd
gdf = gpd.read_parquet("data/parquet/2025/ais-2025-01-01.parquet")
```

## Supported years

- **2025+**: `.csv.zst` format
- **2015–2024**: `.zip` format
