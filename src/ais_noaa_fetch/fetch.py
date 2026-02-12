# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 James Thompson

"""Download AIS data files from NOAA/Marine Cadastre."""

from __future__ import annotations

import datetime
from pathlib import Path

import requests
from tqdm import tqdm

BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler"

# Year when NOAA switched from .zip to .csv.zst
ZST_START_YEAR = 2025


def build_url(date: datetime.date) -> str:
    """Build the download URL for a given date."""
    year = date.year
    if year >= ZST_START_YEAR:
        filename = f"ais-{date.isoformat()}.csv.zst"
    else:
        filename = f"AIS_{year}_{date.month:02d}_{date.day:02d}.zip"
    return f"{BASE_URL}/{year}/{filename}"


def filename_for_date(date: datetime.date) -> str:
    """Return the expected raw filename for a given date."""
    if date.year >= ZST_START_YEAR:
        return f"ais-{date.isoformat()}.csv.zst"
    return f"AIS_{date.year}_{date.month:02d}_{date.day:02d}.zip"


def download_file(
    date: datetime.date,
    data_dir: Path,
    force: bool = False,
) -> Path:
    """Download a single day's AIS data file.

    Returns the path to the downloaded file.
    """
    raw_dir = data_dir / "raw" / str(date.year)
    raw_dir.mkdir(parents=True, exist_ok=True)

    dest = raw_dir / filename_for_date(date)

    if dest.exists() and not force:
        print(f"Skipping {dest.name} (already exists)")
        return dest

    url = build_url(date)
    print(f"Downloading {url}")

    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()

    total = int(response.headers.get("content-length", 0))

    with (
        open(dest, "wb") as f,
        tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as bar,
    ):
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))

    return dest


def fetch_date_range(
    start_date: datetime.date,
    end_date: datetime.date,
    data_dir: Path,
    force: bool = False,
) -> list[Path]:
    """Download AIS data for a range of dates (inclusive)."""
    downloaded: list[Path] = []
    current = start_date
    while current <= end_date:
        try:
            path = download_file(current, data_dir, force=force)
            downloaded.append(path)
        except requests.HTTPError as e:
            print(f"Failed to download {current}: {e}")
        current += datetime.timedelta(days=1)
    return downloaded
