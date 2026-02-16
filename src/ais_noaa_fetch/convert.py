# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 James Thompson

"""Convert raw AIS data files (ZIP/ZST) to Parquet."""

from __future__ import annotations

import datetime
import io
import json
import re
import struct
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from h3.api import numpy_int as h3_np
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import zstandard as zstd

# Canonical column names (2025+ snake_case format)
CANONICAL_COLUMNS = [
    "mmsi", "base_date_time", "latitude", "longitude", "sog", "cog",
    "heading", "vessel_name", "imo", "call_sign", "vessel_type", "status",
    "length", "width", "draft", "cargo", "transceiver",
    "timestamp", "geometry", "h3_res15",
]

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

# Column types keyed by canonical name
COLUMN_TYPES = {
    "mmsi": pa.int32(),
    "base_date_time": pa.string(),
    "latitude": pa.float64(),
    "longitude": pa.float64(),
    "sog": pa.float64(),
    "cog": pa.float64(),
    "heading": pa.float64(),
    "vessel_name": pa.string(),
    "imo": pa.string(),
    "call_sign": pa.string(),
    "vessel_type": pa.int32(),
    "status": pa.int32(),
    "length": pa.float64(),
    "width": pa.float64(),
    "draft": pa.float64(),
    "cargo": pa.int32(),
    "transceiver": pa.string(),
}


# WKB constants for POINT geometry (little-endian)
_WKB_BYTE_ORDER = b"\x01"  # little-endian
_WKB_POINT_TYPE = struct.pack("<I", 1)  # wkbPoint = 1

# GeoParquet metadata template
_GEO_METADATA = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": {
                "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                "type": "GeographicCRS",
                "name": "WGS 84",
                "datum": {
                    "type": "GeodeticReferenceFrame",
                    "name": "World Geodetic System 1984",
                    "ellipsoid": {
                        "name": "WGS 84",
                        "semi_major_axis": 6378137,
                        "inverse_flattening": 298.257223563,
                    },
                },
                "coordinate_system": {
                    "subtype": "ellipsoidal",
                    "axis": [
                        {
                            "name": "Geodetic latitude",
                            "abbreviation": "Lat",
                            "direction": "north",
                            "unit": "degree",
                        },
                        {
                            "name": "Geodetic longitude",
                            "abbreviation": "Lon",
                            "direction": "east",
                            "unit": "degree",
                        },
                    ],
                },
                "id": {"authority": "EPSG", "code": 4326},
            },
            "bbox": [-180.0, -90.0, 180.0, 90.0],
        }
    },
}


def _make_wkb_point(lon: float, lat: float) -> bytes:
    """Build a WKB POINT (little-endian, 21 bytes)."""
    return _WKB_BYTE_ORDER + _WKB_POINT_TYPE + struct.pack("<dd", lon, lat)


def _add_derived_columns(table: pa.Table) -> pa.Table:
    """Add timestamp, geometry, and h3_res15 columns to the table."""
    # --- timestamp ---
    ts_array = pc.strptime(
        table.column("base_date_time"),
        format="%Y-%m-%d %H:%M:%S",
        unit="us",
    )
    ts_array = ts_array.cast(pa.timestamp("us", tz="UTC"))

    # --- geometry (WKB POINT) + h3_res15 in a single pass ---
    # Use numpy arrays instead of to_pylist() to avoid expensive Python object creation
    lons_np = table.column("longitude").to_numpy(zero_copy_only=False)
    lats_np = table.column("latitude").to_numpy(zero_copy_only=False)
    valid = ~(np.isnan(lons_np) | np.isnan(lats_np))

    n = len(lons_np)
    prefix = _WKB_BYTE_ORDER + _WKB_POINT_TYPE
    wkb_points: list[bytes | None] = [None] * n
    h3_cells: list[int | None] = [None] * n
    for i in range(n):
        if valid[i]:
            lon = float(lons_np[i])
            lat = float(lats_np[i])
            wkb_points[i] = prefix + struct.pack("<dd", lon, lat)
            h3_cells[i] = h3_np.latlng_to_cell(lat, lon, 15)

    geom_array = pa.array(wkb_points, type=pa.binary())
    h3_array = pa.array(h3_cells, type=pa.uint64())

    table = table.append_column("timestamp", ts_array)
    table = table.append_column("geometry", geom_array)
    table = table.append_column("h3_res15", h3_array)
    return table


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


def _normalize_columns(table: pa.Table) -> pa.Table:
    """Rename columns to canonical snake_case names."""
    new_names = [_COLUMN_RENAME.get(name, name) for name in table.column_names]
    return table.rename_columns(new_names)


_EARTH_RADIUS_M = 6_371_000.0


def _haversine_m(
    lat1: np.ndarray,
    lon1: np.ndarray,
    lat2: np.ndarray,
    lon2: np.ndarray,
) -> np.ndarray:
    """Vectorized haversine distance in metres between consecutive points."""
    lat1, lon1, lat2, lon2 = (np.radians(a) for a in (lat1, lon1, lat2, lon2))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * _EARTH_RADIUS_M * np.arcsin(np.sqrt(a))


# Index schema for the per-MMSI daily summary
_INDEX_SCHEMA = pa.schema([
    ("mmsi", pa.int32()),
    ("date", pa.date32()),
    # Identity & metadata
    ("vessel_names", pa.list_(pa.string())),
    ("imos", pa.list_(pa.string())),
    ("call_signs", pa.list_(pa.string())),
    ("vessel_types", pa.list_(pa.int32())),
    ("cargos", pa.list_(pa.int32())),
    ("lengths", pa.list_(pa.float64())),
    ("widths", pa.list_(pa.float64())),
    ("drafts", pa.list_(pa.float64())),
    ("transceiver_classes", pa.list_(pa.string())),
    # Message stats
    ("message_count", pa.int64()),
    ("first_timestamp", pa.timestamp("us", tz="UTC")),
    ("last_timestamp", pa.timestamp("us", tz="UTC")),
    ("duration_s", pa.float64()),
    # Geospatial
    ("centroid_lat", pa.float64()),
    ("centroid_lon", pa.float64()),
    ("min_lat", pa.float64()),
    ("max_lat", pa.float64()),
    ("min_lon", pa.float64()),
    ("max_lon", pa.float64()),
    ("distance_m", pa.float64()),
    ("h3_cell_count", pa.int64()),
    # Navigation
    ("sog_min", pa.float64()),
    ("sog_max", pa.float64()),
    ("sog_mean", pa.float64()),
    ("max_inter_msg_speed_ms", pa.float64()),
    ("status_codes", pa.list_(pa.int32())),
])


def _build_index(table: pa.Table, date_str: str) -> pa.Table:
    """Build a per-MMSI daily index/summary from a sorted position table.

    ``table`` must already be sorted by (mmsi, timestamp).
    """
    date_val = datetime.date.fromisoformat(date_str)

    # Extract columns as numpy / python arrays for fast iteration
    mmsi_arr = table.column("mmsi").to_pylist()
    lat_np = table.column("latitude").to_numpy(zero_copy_only=False)
    lon_np = table.column("longitude").to_numpy(zero_copy_only=False)
    sog_np = table.column("sog").to_numpy(zero_copy_only=False)
    h3_arr = table.column("h3_res15").to_pylist()
    vname_arr = table.column("vessel_name").to_pylist()
    imo_arr = table.column("imo").to_pylist()
    csign_arr = table.column("call_sign").to_pylist()
    vtype_arr = table.column("vessel_type").to_pylist()
    status_arr = table.column("status").to_pylist()
    cargo_arr = table.column("cargo").to_pylist()
    length_arr = table.column("length").to_pylist()
    width_arr = table.column("width").to_pylist()
    draft_arr = table.column("draft").to_pylist()
    trans_arr = table.column("transceiver").to_pylist()

    # Timestamps as int64 microseconds (avoids tzdata dependency on Windows)
    ts_us = table.column("timestamp").to_numpy(zero_copy_only=False).astype("int64")

    # Pre-compute haversine distances between consecutive rows
    dists = np.empty(len(lat_np), dtype=np.float64)
    dists[0] = 0.0
    if len(lat_np) > 1:
        dists[1:] = _haversine_m(lat_np[:-1], lon_np[:-1], lat_np[1:], lon_np[1:])

    # Pre-compute time deltas (seconds) between consecutive rows
    td_s = np.empty(len(ts_us), dtype=np.float64)
    td_s[0] = 0.0
    if len(ts_us) > 1:
        td_s[1:] = (ts_us[1:] - ts_us[:-1]) / 1_000_000.0  # microseconds -> seconds

    n = len(mmsi_arr)

    # Output accumulators — one list per column
    out: dict[str, list] = {field.name: [] for field in _INDEX_SCHEMA}

    def _emit(start: int, end: int) -> None:
        """Emit one index row for rows [start, end)."""
        out["mmsi"].append(mmsi_arr[start])
        out["date"].append(date_val)

        # Distinct value sets — identity/metadata
        def _distinct_str(arr: list, s: int, e: int) -> list[str]:
            return sorted({v for v in arr[s:e] if v is not None})

        def _distinct_int(arr: list, s: int, e: int) -> list[int]:
            return sorted({v for v in arr[s:e] if v is not None})

        def _distinct_float(arr: list, s: int, e: int) -> list[float]:
            vals = {v for v in arr[s:e] if v is not None}
            return sorted(vals)

        out["vessel_names"].append(_distinct_str(vname_arr, start, end))
        out["imos"].append(_distinct_str(imo_arr, start, end))
        out["call_signs"].append(_distinct_str(csign_arr, start, end))
        out["vessel_types"].append(_distinct_int(vtype_arr, start, end))
        out["cargos"].append(_distinct_int(cargo_arr, start, end))
        out["lengths"].append(_distinct_float(length_arr, start, end))
        out["widths"].append(_distinct_float(width_arr, start, end))
        out["drafts"].append(_distinct_float(draft_arr, start, end))
        out["transceiver_classes"].append(_distinct_str(trans_arr, start, end))

        # Message stats
        count = end - start
        out["message_count"].append(count)
        first_us = int(ts_us[start])
        last_us = int(ts_us[end - 1])
        out["first_timestamp"].append(first_us)
        out["last_timestamp"].append(last_us)
        out["duration_s"].append((last_us - first_us) / 1_000_000.0)

        # Geospatial
        slat = lat_np[start:end]
        slon = lon_np[start:end]
        valid_lat = slat[~np.isnan(slat)]
        valid_lon = slon[~np.isnan(slon)]
        out["centroid_lat"].append(float(np.mean(valid_lat)) if len(valid_lat) else None)
        out["centroid_lon"].append(float(np.mean(valid_lon)) if len(valid_lon) else None)
        out["min_lat"].append(float(np.min(valid_lat)) if len(valid_lat) else None)
        out["max_lat"].append(float(np.max(valid_lat)) if len(valid_lat) else None)
        out["min_lon"].append(float(np.min(valid_lon)) if len(valid_lon) else None)
        out["max_lon"].append(float(np.max(valid_lon)) if len(valid_lon) else None)

        # Distance — sum haversine for consecutive points within this MMSI
        # The first row of each group has dist=0 (computed globally, but the
        # first row of a new MMSI's segment should not use the previous MMSI's
        # last point). We zeroed dists[0] globally; for subsequent groups the
        # cross-boundary dist is wrong, so we exclude it.
        group_dists = dists[start:end].copy()
        group_dists[0] = 0.0  # first row has no predecessor in this group
        # Mask out NaN distances (from NaN lat/lon)
        valid_d = group_dists[~np.isnan(group_dists)]
        out["distance_m"].append(float(np.sum(valid_d)))

        # H3 cell count
        h3_set = {v for v in h3_arr[start:end] if v is not None}
        out["h3_cell_count"].append(len(h3_set))

        # Navigation
        ssog = sog_np[start:end]
        valid_sog = ssog[~np.isnan(ssog)]
        out["sog_min"].append(float(np.min(valid_sog)) if len(valid_sog) else None)
        out["sog_max"].append(float(np.max(valid_sog)) if len(valid_sog) else None)
        out["sog_mean"].append(float(np.mean(valid_sog)) if len(valid_sog) else None)

        # Max inter-message speed (m/s)
        if count >= 2:
            group_td = td_s[start:end].copy()
            group_td[0] = 0.0
            group_d = dists[start:end].copy()
            group_d[0] = 0.0
            # Only where time delta > 0 to avoid division by zero
            mask = group_td > 0
            if np.any(mask):
                speeds = group_d[mask] / group_td[mask]
                valid_speeds = speeds[~np.isnan(speeds)]
                out["max_inter_msg_speed_ms"].append(
                    float(np.max(valid_speeds)) if len(valid_speeds) else None
                )
            else:
                out["max_inter_msg_speed_ms"].append(None)
        else:
            out["max_inter_msg_speed_ms"].append(None)

        out["status_codes"].append(_distinct_int(status_arr, start, end))

    # Stream through sorted rows, emit when MMSI changes
    if n > 0:
        group_start = 0
        cur_mmsi = mmsi_arr[0]
        for i in range(1, n):
            if mmsi_arr[i] != cur_mmsi:
                _emit(group_start, i)
                group_start = i
                cur_mmsi = mmsi_arr[i]
        _emit(group_start, n)

    # Build the index table
    arrays = [pa.array(out[field.name], type=field.type) for field in _INDEX_SCHEMA]
    return pa.table(arrays, schema=_INDEX_SCHEMA)


def _read_csv_bytes(data: bytes) -> pa.Table:
    """Read CSV bytes into a PyArrow table with normalized column names."""
    parse_options = pcsv.ParseOptions(delimiter=",")
    convert_options = pcsv.ConvertOptions(strings_can_be_null=True)

    table = pcsv.read_csv(
        io.BytesIO(data),
        parse_options=parse_options,
        convert_options=convert_options,
    )
    table = _normalize_columns(table)

    # Cast to canonical types
    fields = []
    for name in table.column_names:
        if name in COLUMN_TYPES:
            fields.append(pa.field(name, COLUMN_TYPES[name]))
        else:
            fields.append(pa.field(name, table.schema.field(name).type))
    table = table.cast(pa.schema(fields))

    # Add missing canonical columns as null (raw columns only)
    raw_cols = [c for c in CANONICAL_COLUMNS if c in COLUMN_TYPES]
    for col_name in raw_cols:
        if col_name not in table.column_names:
            table = table.append_column(
                col_name, pa.nulls(len(table), type=COLUMN_TYPES[col_name])
            )

    # Add derived columns (timestamp, geometry, h3_res15)
    table = _add_derived_columns(table)

    # Enforce canonical column order
    return table.select(CANONICAL_COLUMNS)


def _decompress_zst(path: Path) -> bytes:
    """Decompress a .csv.zst file and return CSV bytes."""
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as f:
        return dctx.decompress(f.read(), max_output_size=2 * 1024 * 1024 * 1024)


def _extract_zip(path: Path) -> bytes:
    """Extract the CSV from a .zip file and return CSV bytes."""
    with zipfile.ZipFile(path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV file found in {path}")
        return zf.read(csv_names[0])


def convert_file(
    raw_path: Path,
    data_dir: Path,
    delete_raw: bool = False,
) -> tuple[Path, Path]:
    """Convert a single raw AIS file to Parquet.

    Returns (main_parquet_path, index_parquet_path).
    """
    # Determine year from parent directory name
    year = raw_path.parent.name
    date_str = _extract_date(raw_path.name)

    broadcasts_dir = data_dir / "parquet" / "broadcasts" / year
    index_dir = data_dir / "parquet" / "index" / year
    broadcasts_dir.mkdir(parents=True, exist_ok=True)
    index_dir.mkdir(parents=True, exist_ok=True)

    # Normalize output filename to ais-YYYY-MM-DD.parquet
    out_path = broadcasts_dir / f"ais-{date_str}.parquet"
    index_path = index_dir / f"ais-{date_str}.parquet"

    # Decompress
    if raw_path.suffix == ".zst":
        csv_bytes = _decompress_zst(raw_path)
    elif raw_path.suffix == ".zip":
        csv_bytes = _extract_zip(raw_path)
    else:
        raise ValueError(f"Unsupported file type: {raw_path}")

    print(f"Converting {raw_path.name} -> {out_path.name}")

    table = _read_csv_bytes(csv_bytes)

    # Add file date column
    date_val = datetime.date.fromisoformat(date_str)
    date_col = pa.array([date_val] * table.num_rows, type=pa.date32())
    table = table.append_column("date", date_col)
    # Reorder so date comes right after mmsi
    cols = list(table.column_names)
    cols.remove("date")
    cols.insert(1, "date")
    table = table.select(cols)

    # Sort by mmsi then timestamp for fast seeking by vessel
    table = table.sort_by([("mmsi", "ascending"), ("timestamp", "ascending")])

    # Build per-MMSI daily index
    index_table = _build_index(table, date_str)

    # Attach GeoParquet metadata to schema
    existing_meta = table.schema.metadata or {}
    existing_meta[b"geo"] = json.dumps(_GEO_METADATA).encode()
    table = table.replace_schema_metadata(existing_meta)

    pq.write_table(table, out_path, compression="zstd")
    pq.write_table(index_table, index_path, compression="zstd")

    if delete_raw:
        raw_path.unlink()
        print(f"Deleted {raw_path.name}")

    print(f"Wrote {out_path} ({table.num_rows:,} rows)")
    print(f"Wrote {index_path} ({index_table.num_rows:,} MMSIs)")
    return out_path, index_path


def convert_year(
    year: int,
    data_dir: Path,
    delete_raw: bool = False,
    workers: int = 1,
) -> list[tuple[Path, Path]]:
    """Convert all raw files for a given year to Parquet.

    Returns a list of (main_parquet_path, index_parquet_path) tuples.
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
