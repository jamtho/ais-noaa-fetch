"""Convert raw AIS data files (ZIP/ZST) to Parquet."""

from __future__ import annotations

import io
import json
import re
import struct
import zipfile
from pathlib import Path

import h3
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
    "mmsi": pa.string(),
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

    # --- geometry (WKB POINT) ---
    lons = table.column("longitude").to_pylist()
    lats = table.column("latitude").to_pylist()
    wkb_points = [
        _make_wkb_point(lon, lat) if lon is not None and lat is not None else None
        for lon, lat in zip(lons, lats)
    ]
    geom_array = pa.array(wkb_points, type=pa.binary())

    # --- h3_res15 ---
    h3_cells = [
        int(h3.latlng_to_cell(lat, lon, 15), 16)
        if lat is not None and lon is not None
        else None
        for lat, lon in zip(lats, lons)
    ]
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
) -> Path:
    """Convert a single raw AIS file to Parquet.

    Returns the path to the output Parquet file.
    """
    # Determine year from parent directory name
    year = raw_path.parent.name

    parquet_dir = data_dir / "parquet" / year
    parquet_dir.mkdir(parents=True, exist_ok=True)

    # Normalize output filename to ais-YYYY-MM-DD.parquet
    out_path = parquet_dir / f"ais-{_extract_date(raw_path.name)}.parquet"

    # Decompress
    if raw_path.suffix == ".zst":
        csv_bytes = _decompress_zst(raw_path)
    elif raw_path.suffix == ".zip":
        csv_bytes = _extract_zip(raw_path)
    else:
        raise ValueError(f"Unsupported file type: {raw_path}")

    print(f"Converting {raw_path.name} -> {out_path.name}")

    table = _read_csv_bytes(csv_bytes)

    # Attach GeoParquet metadata to schema
    existing_meta = table.schema.metadata or {}
    existing_meta[b"geo"] = json.dumps(_GEO_METADATA).encode()
    table = table.replace_schema_metadata(existing_meta)

    pq.write_table(table, out_path, compression="zstd")

    if delete_raw:
        raw_path.unlink()
        print(f"Deleted {raw_path.name}")

    print(f"Wrote {out_path} ({table.num_rows:,} rows)")
    return out_path


def convert_year(
    year: int,
    data_dir: Path,
    delete_raw: bool = False,
) -> list[Path]:
    """Convert all raw files for a given year to Parquet."""
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

    converted: list[Path] = []
    for raw_path in raw_files:
        try:
            out = convert_file(raw_path, data_dir, delete_raw=delete_raw)
            converted.append(out)
        except Exception as e:
            print(f"Failed to convert {raw_path.name}: {e}")

    return converted
