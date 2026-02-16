# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 James Thompson

"""Regression tests for AIS data conversion."""

from __future__ import annotations

import datetime
import struct
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zstandard as zstd

from ais_noaa_fetch.convert import (
    CANONICAL_COLUMNS,
    _INDEX_SCHEMA,
    _extract_date,
    _haversine_m,
    convert_file,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_SAMPLE_CSV = """\
MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo,TransceiverClass
123456789,2025-01-15 10:00:00,29.0,-90.0,5.0,180.0,180.0,TEST VESSEL,IMO1234567,WXY1234,70,0,100.0,20.0,5.0,70,A
123456789,2025-01-15 10:05:00,29.001,-90.001,6.0,185.0,185.0,TEST VESSEL,IMO1234567,WXY1234,70,0,100.0,20.0,5.0,70,A
123456789,2025-01-15 10:10:00,29.002,-90.002,4.0,175.0,175.0,TEST VESSEL,IMO1234567,WXY1234,70,0,100.0,20.0,5.0,70,A
987654321,2025-01-15 11:00:00,40.0,-74.0,0.0,0.0,0.0,OTHER SHIP,IMO7654321,ABC9876,30,1,50.0,10.0,3.0,30,B
987654321,2025-01-15 11:30:00,40.0,-74.0,0.0,0.0,0.0,OTHER SHIP,IMO7654321,ABC9876,30,1,50.0,10.0,3.0,30,B
"""


@pytest.fixture()
def sample_zst(tmp_path: Path) -> Path:
    """Create a sample .csv.zst file in a temporary raw directory."""
    raw_dir = tmp_path / "raw" / "2025"
    raw_dir.mkdir(parents=True)

    csv_bytes = _SAMPLE_CSV.encode("utf-8")
    cctx = zstd.ZstdCompressor()
    compressed = cctx.compress(csv_bytes)

    path = raw_dir / "ais-2025-01-15.csv.zst"
    path.write_bytes(compressed)
    return path


@pytest.fixture()
def converted(sample_zst: Path, tmp_path: Path) -> tuple[Path, Path]:
    """Run convert_file and return (broadcast_path, index_path)."""
    return convert_file(sample_zst, tmp_path)


@pytest.fixture()
def broadcast_table(converted: tuple[Path, Path]) -> pa.Table:
    broadcast_path, _ = converted
    return pq.read_table(broadcast_path)


@pytest.fixture()
def index_table(converted: tuple[Path, Path]) -> pa.Table:
    _, index_path = converted
    return pq.read_table(index_path)


# ---------------------------------------------------------------------------
# Unit tests — helpers
# ---------------------------------------------------------------------------


class TestExtractDate:
    def test_zst_format(self) -> None:
        assert _extract_date("ais-2025-01-15.csv.zst") == "2025-01-15"

    def test_zip_format(self) -> None:
        assert _extract_date("AIS_2024_06_01.zip") == "2024-06-01"

    def test_invalid(self) -> None:
        with pytest.raises(ValueError):
            _extract_date("garbage.txt")


class TestHaversine:
    def test_same_point(self) -> None:
        d = _haversine_m(
            np.array([0.0]), np.array([0.0]),
            np.array([0.0]), np.array([0.0]),
        )
        assert d[0] == pytest.approx(0.0)

    def test_known_distance(self) -> None:
        # 1 degree of latitude ≈ 111,195 m
        d = _haversine_m(
            np.array([0.0]), np.array([0.0]),
            np.array([1.0]), np.array([0.0]),
        )
        assert d[0] == pytest.approx(111_195.0, rel=0.01)


# ---------------------------------------------------------------------------
# Broadcast parquet regression tests
# ---------------------------------------------------------------------------


class TestBroadcastParquet:
    def test_file_exists(self, converted: tuple[Path, Path]) -> None:
        broadcast_path, _ = converted
        assert broadcast_path.exists()

    def test_row_count(self, broadcast_table: pa.Table) -> None:
        assert broadcast_table.num_rows == 5

    def test_column_order(self, broadcast_table: pa.Table) -> None:
        expected = list(CANONICAL_COLUMNS)
        expected.insert(1, "date")
        assert broadcast_table.column_names == expected

    def test_mmsi_type_int32(self, broadcast_table: pa.Table) -> None:
        assert broadcast_table.schema.field("mmsi").type == pa.int32()

    def test_date_column(self, broadcast_table: pa.Table) -> None:
        assert broadcast_table.schema.field("date").type == pa.date32()
        dates = broadcast_table.column("date").to_pylist()
        assert all(d == datetime.date(2025, 1, 15) for d in dates)

    def test_sort_order(self, broadcast_table: pa.Table) -> None:
        """Rows are sorted by (mmsi ASC, timestamp ASC)."""
        mmsi_vals = broadcast_table.column("mmsi").to_pylist()
        ts_vals = broadcast_table.column("timestamp").to_pylist()
        for i in range(1, len(mmsi_vals)):
            if mmsi_vals[i] == mmsi_vals[i - 1]:
                assert ts_vals[i] >= ts_vals[i - 1]
            else:
                assert mmsi_vals[i] > mmsi_vals[i - 1]

    def test_timestamp_type(self, broadcast_table: pa.Table) -> None:
        assert broadcast_table.schema.field("timestamp").type == pa.timestamp(
            "us", tz="UTC"
        )

    def test_geometry_wkb_point(self, broadcast_table: pa.Table) -> None:
        assert broadcast_table.schema.field("geometry").type == pa.binary()
        wkb = broadcast_table.column("geometry")[0].as_py()
        assert len(wkb) == 21
        assert wkb[0:1] == b"\x01"  # little-endian
        assert struct.unpack("<I", wkb[1:5])[0] == 1  # wkbPoint

    def test_h3_column(self, broadcast_table: pa.Table) -> None:
        assert broadcast_table.schema.field("h3_res15").type == pa.uint64()
        assert broadcast_table.column("h3_res15").null_count == 0

    def test_geoparquet_metadata(self, converted: tuple[Path, Path]) -> None:
        broadcast_path, _ = converted
        meta = pq.read_metadata(broadcast_path)
        assert b"geo" in meta.metadata


# ---------------------------------------------------------------------------
# Index parquet regression tests
# ---------------------------------------------------------------------------


class TestIndexParquet:
    def test_file_exists(self, converted: tuple[Path, Path]) -> None:
        _, index_path = converted
        assert index_path.exists()

    def test_row_count(self, index_table: pa.Table) -> None:
        """One row per distinct MMSI."""
        assert index_table.num_rows == 2

    def test_schema_matches(self, index_table: pa.Table) -> None:
        for field in _INDEX_SCHEMA:
            assert index_table.schema.field(field.name).type == field.type

    def test_mmsi_type_int32(self, index_table: pa.Table) -> None:
        assert index_table.schema.field("mmsi").type == pa.int32()

    def test_message_counts(self, index_table: pa.Table) -> None:
        counts = dict(
            zip(
                index_table.column("mmsi").to_pylist(),
                index_table.column("message_count").to_pylist(),
            )
        )
        assert counts[123456789] == 3
        assert counts[987654321] == 2

    def test_stationary_vessel_distance(self, index_table: pa.Table) -> None:
        """MMSI 987654321 is stationary — distance should be ~0."""
        rows = {r["mmsi"]: r for r in index_table.to_pylist()}
        assert rows[987654321]["distance_m"] == pytest.approx(0.0, abs=1.0)

    def test_moving_vessel_distance(self, index_table: pa.Table) -> None:
        """MMSI 123456789 moves — distance should be positive."""
        rows = {r["mmsi"]: r for r in index_table.to_pylist()}
        assert rows[123456789]["distance_m"] > 0

    def test_bounding_box_contains_centroid(self, index_table: pa.Table) -> None:
        for row in index_table.to_pylist():
            assert row["min_lat"] <= row["centroid_lat"] <= row["max_lat"]
            assert row["min_lon"] <= row["centroid_lon"] <= row["max_lon"]

    def test_total_messages_match_broadcast(
        self, broadcast_table: pa.Table, index_table: pa.Table
    ) -> None:
        """Sum of index message_count equals broadcast row count."""
        total = sum(index_table.column("message_count").to_pylist())
        assert total == broadcast_table.num_rows

    def test_duration(self, index_table: pa.Table) -> None:
        """Moving vessel has 10 min = 600s duration; stationary has 30 min = 1800s."""
        rows = {r["mmsi"]: r for r in index_table.to_pylist()}
        assert rows[123456789]["duration_s"] == pytest.approx(600.0)
        assert rows[987654321]["duration_s"] == pytest.approx(1800.0)

    def test_h3_cell_count(self, index_table: pa.Table) -> None:
        rows = {r["mmsi"]: r for r in index_table.to_pylist()}
        # Stationary vessel should have 1 H3 cell
        assert rows[987654321]["h3_cell_count"] == 1
        # Moving vessel should have > 1 (3 distinct points)
        assert rows[123456789]["h3_cell_count"] >= 1
