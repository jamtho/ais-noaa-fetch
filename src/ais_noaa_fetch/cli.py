# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 James Thompson

"""CLI entry point for ais-noaa-fetch."""

from __future__ import annotations

import argparse
import datetime
from pathlib import Path

from ais_noaa_fetch.convert import convert_file, convert_year
from ais_noaa_fetch.fetch import fetch_date_range


def parse_date(s: str) -> datetime.date:
    """Parse a YYYY-MM-DD date string."""
    return datetime.date.fromisoformat(s)


def resolve_data_dir(args: argparse.Namespace) -> Path:
    """Resolve the data directory from CLI args."""
    return Path(args.data_dir).resolve()


def cmd_fetch(args: argparse.Namespace) -> None:
    """Handle the 'fetch' subcommand."""
    data_dir = resolve_data_dir(args)
    fetch_date_range(
        start_date=args.start_date,
        end_date=args.end_date,
        data_dir=data_dir,
        force=args.force,
    )


def cmd_convert(args: argparse.Namespace) -> None:
    """Handle the 'convert' subcommand."""
    data_dir = resolve_data_dir(args)
    convert_year(
        year=args.year,
        data_dir=data_dir,
        delete_raw=args.delete_raw,
        workers=args.workers,
    )


def cmd_run(args: argparse.Namespace) -> None:
    """Handle the 'run' subcommand (fetch + convert)."""
    data_dir = resolve_data_dir(args)
    downloaded = fetch_date_range(
        start_date=args.start_date,
        end_date=args.end_date,
        data_dir=data_dir,
        force=args.force,
    )
    workers = args.workers
    if workers > 1:
        from concurrent.futures import ProcessPoolExecutor, as_completed

        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(convert_file, p, data_dir, args.delete_raw): p
                for p in downloaded
            }
            for future in as_completed(futures):
                raw_path = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"Failed to convert {raw_path.name}: {e}")
    else:
        for raw_path in downloaded:
            try:
                convert_file(raw_path, data_dir, delete_raw=args.delete_raw)
            except Exception as e:
                print(f"Failed to convert {raw_path.name}: {e}")


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="ais-noaa-fetch",
        description="Download AIS data from NOAA and convert to Parquet",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Base directory for data files (default: data)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- fetch ---
    fetch_parser = subparsers.add_parser("fetch", help="Download raw AIS data files")
    fetch_parser.add_argument("--start-date", type=parse_date, required=True)
    fetch_parser.add_argument("--end-date", type=parse_date, required=True)
    fetch_parser.add_argument(
        "--force", action="store_true", help="Re-download existing files"
    )
    fetch_parser.set_defaults(func=cmd_fetch)

    # --- convert ---
    convert_parser = subparsers.add_parser(
        "convert", help="Convert downloaded raw files to Parquet"
    )
    convert_parser.add_argument("--year", type=int, required=True)
    convert_parser.add_argument(
        "--delete-raw",
        action="store_true",
        help="Delete raw files after conversion",
    )
    convert_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers for conversion (default: 1)",
    )
    convert_parser.set_defaults(func=cmd_convert)

    # --- run ---
    run_parser = subparsers.add_parser(
        "run", help="Fetch and convert in one step"
    )
    run_parser.add_argument("--start-date", type=parse_date, required=True)
    run_parser.add_argument("--end-date", type=parse_date, required=True)
    run_parser.add_argument(
        "--force", action="store_true", help="Re-download existing files"
    )
    run_parser.add_argument(
        "--delete-raw",
        action="store_true",
        help="Delete raw files after conversion",
    )
    run_parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel workers for conversion (default: 1)",
    )
    run_parser.set_defaults(func=cmd_run)

    return parser


def main() -> None:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)
