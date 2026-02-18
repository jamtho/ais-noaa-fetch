# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 James Thompson

"""Convert raw AIS data files (ZIP/ZST) to Parquet."""

from __future__ import annotations

import re
import tempfile
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb

# Map old column names (pre-2025) to canonical names
_COLUMN_RENAME = {
    "MMSI": "mmsi",
    "BaseDateTime": "base_date_time",
    "LAT": "latitude",
    "LON": "longitude",
    "SOG": "sog",
    "COG": "cog",
    "Heading": "heading",
    "VesselName": "vessel_name",
    "IMO": "imo",
    "CallSign": "call_sign",
    "VesselType": "vessel_type",
    "Status": "status",
    "Length": "length",
    "Width": "width",
    "Draft": "draft",
    "Cargo": "cargo",
    "TransceiverClass": "transceiver",
}

# Reverse mapping: canonical name -> old column name
_CANONICAL_TO_OLD = {v: k for k, v in _COLUMN_RENAME.items()}

# Canonical column order for broadcast parquet (including derived columns)
CANONICAL_COLUMNS = [
    "mmsi", "date", "base_date_time", "latitude", "longitude", "sog", "cog",
    "heading", "vessel_name", "imo", "call_sign", "vessel_type", "status",
    "length", "width", "draft", "cargo", "transceiver",
    "timestamp", "geometry", "h3_res15",
]

# Index column names for the per-MMSI daily summary
INDEX_COLUMNS = [
    "mmsi", "date",
    "vessel_names", "imos", "call_signs", "vessel_types", "cargos",
    "lengths", "widths", "drafts", "transceiver_classes",
    "message_count", "first_timestamp", "last_timestamp", "duration_s",
    "centroid_lat", "centroid_lon",
    "min_lat", "max_lat", "min_lon", "max_lon",
    "distance_m", "h3_cell_count",
    "sog_min", "sog_max", "sog_mean", "max_inter_msg_speed_ms",
    "status_codes",
]


def _extract_date(filename: str) -> str:
    """Extract a YYYY-MM-DD date string from a raw AIS filename.

    Handles both 'AIS_2024_01_01.zip' and 'ais-2025-01-01.csv.zst'.
    """
    # Try AIS_YYYY_MM_DD pattern first
    m = re.search(r"(\d{4})_(\d{2})_(\d{2})", filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Try ais-YYYY-MM-DD pattern
    m = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if m:
        return m.group(1)
    raise ValueError(f"Cannot extract date from filename: {filename}")


def _extract_zip(path: Path) -> bytes:
    """Extract the CSV from a .zip file and return CSV bytes."""
    with zipfile.ZipFile(path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV file found in {path}")
        return zf.read(csv_names[0])


def _col_ref(canonical: str, is_old_format: bool) -> str:
    """Return a quoted SQL column reference for a canonical column name."""
    if is_old_format:
        return f'"{_CANONICAL_TO_OLD.get(canonical, canonical)}"'
    return f'"{canonical}"'


def _esc(path: str | Path) -> str:
    """Escape a file path for embedding in a DuckDB SQL string literal."""
    return str(path).replace("'", "''").replace("\\", "/")


def _duckdb_pipeline(
    csv_source: str,
    date_str: str,
    out_path: Path,
    index_path: Path,
) -> tuple[int, int]:
    """Run the full conversion pipeline using DuckDB.

    Returns (broadcast_row_count, index_mmsi_count).
    """
    con = duckdb.connect()
    con.execute("INSTALL h3 FROM community; LOAD h3;")
    con.execute("INSTALL spatial; LOAD spatial;")

    csv_esc = _esc(csv_source)

    # Read CSV — all_varchar avoids type detection issues (e.g. base_date_time
    # auto-detected as TIMESTAMP in 2025+ files; we want it as VARCHAR).
    con.execute(
        f"CREATE VIEW raw AS SELECT * FROM read_csv('{csv_esc}', all_varchar=true)",
    )

    # Detect old (MMSI, BaseDateTime, …) vs new (mmsi, base_date_time, …)
    cols = {r[0] for r in con.execute("DESCRIBE raw").fetchall()}
    is_old = "MMSI" in cols

    def c(name: str) -> str:
        return _col_ref(name, is_old)

    n_raw = con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]

    # Build broadcast table: clean MMSI, cast types, derive columns, sort.
    # TRY_CAST on mmsi filters out dirty values (10-digit overflows,
    # letter-prefixed strings, fractional floats) by returning NULL.
    con.execute(f"""
        CREATE TABLE broadcast AS
        SELECT
            TRY_CAST({c('mmsi')} AS INTEGER) AS mmsi,
            '{date_str}'::DATE AS date,
            {c('base_date_time')} AS base_date_time,
            TRY_CAST({c('latitude')} AS DOUBLE) AS latitude,
            TRY_CAST({c('longitude')} AS DOUBLE) AS longitude,
            TRY_CAST({c('sog')} AS DOUBLE) AS sog,
            TRY_CAST({c('cog')} AS DOUBLE) AS cog,
            TRY_CAST({c('heading')} AS DOUBLE) AS heading,
            {c('vessel_name')} AS vessel_name,
            {c('imo')} AS imo,
            {c('call_sign')} AS call_sign,
            TRY_CAST({c('vessel_type')} AS INTEGER) AS vessel_type,
            TRY_CAST({c('status')} AS INTEGER) AS status,
            TRY_CAST({c('length')} AS DOUBLE) AS length,
            TRY_CAST({c('width')} AS DOUBLE) AS width,
            TRY_CAST({c('draft')} AS DOUBLE) AS draft,
            TRY_CAST({c('cargo')} AS INTEGER) AS cargo,
            {c('transceiver')} AS transceiver,
            strptime({c('base_date_time')}, '%Y-%m-%d %H:%M:%S')
                ::TIMESTAMPTZ AS timestamp,
            ST_Point(
                TRY_CAST({c('longitude')} AS DOUBLE),
                TRY_CAST({c('latitude')} AS DOUBLE)
            ) AS geometry,
            h3_latlng_to_cell(
                TRY_CAST({c('latitude')} AS DOUBLE),
                TRY_CAST({c('longitude')} AS DOUBLE),
                15
            )::UBIGINT AS h3_res15
        FROM raw
        WHERE TRY_CAST({c('mmsi')} AS INTEGER) IS NOT NULL
          AND TRY_CAST({c('mmsi')} AS INTEGER) BETWEEN 0 AND 2147483647
        ORDER BY mmsi, timestamp
    """)

    n_clean = con.execute("SELECT COUNT(*) FROM broadcast").fetchone()[0]
    n_dropped = n_raw - n_clean
    if n_dropped:
        print(f"  Dropped {n_dropped:,} rows with invalid MMSI values")

    # Write broadcast parquet (spatial extension adds GeoParquet metadata)
    con.execute(
        f"COPY broadcast TO '{_esc(out_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)",
    )

    # Build and write index using window functions + GROUP BY
    con.execute(f"""
        COPY (
            WITH prev AS (
                SELECT *,
                    LAG(latitude) OVER w AS prev_lat,
                    LAG(longitude) OVER w AS prev_lon,
                    LAG(timestamp) OVER w AS prev_ts
                FROM broadcast
                WINDOW w AS (PARTITION BY mmsi ORDER BY timestamp)
            ),
            dists AS (
                SELECT *,
                    CASE
                        WHEN prev_lat IS NOT NULL AND latitude IS NOT NULL
                        THEN ST_Distance_Sphere(
                                 ST_Point(longitude, latitude),
                                 ST_Point(prev_lon, prev_lat))
                        ELSE 0.0
                    END AS dist_m,
                    CASE
                        WHEN prev_ts IS NOT NULL
                        THEN EPOCH(timestamp - prev_ts)
                        ELSE 0.0
                    END AS dt_s
                FROM prev
            )
            SELECT
                mmsi,
                '{date_str}'::DATE AS date,
                COALESCE(list(DISTINCT vessel_name ORDER BY vessel_name)
                    FILTER (WHERE vessel_name IS NOT NULL),
                    []::VARCHAR[]) AS vessel_names,
                COALESCE(list(DISTINCT imo ORDER BY imo)
                    FILTER (WHERE imo IS NOT NULL),
                    []::VARCHAR[]) AS imos,
                COALESCE(list(DISTINCT call_sign ORDER BY call_sign)
                    FILTER (WHERE call_sign IS NOT NULL),
                    []::VARCHAR[]) AS call_signs,
                COALESCE(list(DISTINCT vessel_type ORDER BY vessel_type)
                    FILTER (WHERE vessel_type IS NOT NULL),
                    []::INTEGER[]) AS vessel_types,
                COALESCE(list(DISTINCT cargo ORDER BY cargo)
                    FILTER (WHERE cargo IS NOT NULL),
                    []::INTEGER[]) AS cargos,
                COALESCE(list(DISTINCT length ORDER BY length)
                    FILTER (WHERE length IS NOT NULL),
                    []::DOUBLE[]) AS lengths,
                COALESCE(list(DISTINCT width ORDER BY width)
                    FILTER (WHERE width IS NOT NULL),
                    []::DOUBLE[]) AS widths,
                COALESCE(list(DISTINCT draft ORDER BY draft)
                    FILTER (WHERE draft IS NOT NULL),
                    []::DOUBLE[]) AS drafts,
                COALESCE(list(DISTINCT transceiver ORDER BY transceiver)
                    FILTER (WHERE transceiver IS NOT NULL),
                    []::VARCHAR[]) AS transceiver_classes,
                COUNT(*)::BIGINT AS message_count,
                MIN(timestamp) AS first_timestamp,
                MAX(timestamp) AS last_timestamp,
                EPOCH(MAX(timestamp) - MIN(timestamp)) AS duration_s,
                AVG(latitude) AS centroid_lat,
                AVG(longitude) AS centroid_lon,
                MIN(latitude) AS min_lat,
                MAX(latitude) AS max_lat,
                MIN(longitude) AS min_lon,
                MAX(longitude) AS max_lon,
                SUM(dist_m) AS distance_m,
                COUNT(DISTINCT h3_res15)::BIGINT AS h3_cell_count,
                MIN(sog) AS sog_min,
                MAX(sog) AS sog_max,
                AVG(sog) AS sog_mean,
                MAX(CASE WHEN dt_s > 0 THEN dist_m / dt_s
                    ELSE NULL END) AS max_inter_msg_speed_ms,
                COALESCE(list(DISTINCT status ORDER BY status)
                    FILTER (WHERE status IS NOT NULL),
                    []::INTEGER[]) AS status_codes
            FROM dists
            GROUP BY mmsi
            ORDER BY mmsi
        ) TO '{_esc(index_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    n_mmsi = con.execute(
        "SELECT COUNT(DISTINCT mmsi) FROM broadcast"
    ).fetchone()[0]
    con.close()
    return n_clean, n_mmsi


def convert_file(
    raw_path: Path,
    data_dir: Path,
    delete_raw: bool = False,
) -> tuple[Path, Path]:
    """Convert a single raw AIS file to Parquet.

    Returns (broadcast_parquet_path, index_parquet_path).
    """
    year = raw_path.parent.name
    date_str = _extract_date(raw_path.name)

    broadcasts_dir = data_dir / "parquet" / "broadcasts" / year
    index_dir = data_dir / "parquet" / "index" / year
    broadcasts_dir.mkdir(parents=True, exist_ok=True)
    index_dir.mkdir(parents=True, exist_ok=True)

    out_path = broadcasts_dir / f"ais-{date_str}.parquet"
    index_path = index_dir / f"ais-{date_str}.parquet"

    print(f"Converting {raw_path.name} -> {out_path.name}")

    # DuckDB reads .csv.zst natively; for .zip we extract to a temp file.
    tmp_path: Path | None = None
    if raw_path.suffix == ".zst":
        csv_source = str(raw_path)
    elif raw_path.suffix == ".zip":
        csv_bytes = _extract_zip(raw_path)
        tmp = tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False, dir=raw_path.parent,
        )
        tmp.write(csv_bytes)
        tmp.close()
        csv_source = tmp.name
        tmp_path = Path(tmp.name)
    else:
        raise ValueError(f"Unsupported file type: {raw_path}")

    try:
        n_rows, n_mmsi = _duckdb_pipeline(
            csv_source, date_str, out_path, index_path,
        )
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

    if delete_raw:
        raw_path.unlink()
        print(f"Deleted {raw_path.name}")

    print(f"Wrote {out_path} ({n_rows:,} rows)")
    print(f"Wrote {index_path} ({n_mmsi:,} MMSIs)")
    return out_path, index_path


def convert_year(
    year: int,
    data_dir: Path,
    delete_raw: bool = False,
    workers: int = 1,
) -> list[tuple[Path, Path]]:
    """Convert all raw files for a given year to Parquet.

    Returns a list of (broadcast_parquet_path, index_parquet_path) tuples.
    When *workers* > 1, files are converted in parallel using a process pool.
    """
    raw_dir = data_dir / "raw" / str(year)
    if not raw_dir.exists():
        print(f"No raw data directory found: {raw_dir}")
        return []

    raw_files = sorted(
        p for p in raw_dir.iterdir()
        if p.suffix in (".zip", ".zst")
    )

    if not raw_files:
        print(f"No raw files found in {raw_dir}")
        return []

    converted: list[tuple[Path, Path]] = []

    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(convert_file, p, data_dir, delete_raw): p
                for p in raw_files
            }
            for future in as_completed(futures):
                raw_path = futures[future]
                try:
                    converted.append(future.result())
                except Exception as e:
                    print(f"Failed to convert {raw_path.name}: {e}")
    else:
        for raw_path in raw_files:
            try:
                out = convert_file(raw_path, data_dir, delete_raw=delete_raw)
                converted.append(out)
            except Exception as e:
                print(f"Failed to convert {raw_path.name}: {e}")

    return converted
