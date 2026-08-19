# Changelog

All behaviour changes that affect generated AIS data are recorded here.

## 2026-08-19

- Normalize `base_date_time` strings to `YYYY-MM-DD HH:MM:SS` across all
  converted files. Older NOAA ZIP files with `T` separators are rewritten with
  a space separator.
- Parse `timestamp` as true UTC from `base_date_time`. Previous files parsed
  NOAA UTC strings through the process-local timezone before storing UTC,
  shifting timestamps by one or two hours depending on European DST.
- Fix index distance calculations to use latitude/longitude order for DuckDB
  Spatial `ST_Distance_Sphere`. Previous `distance_m` and
  `max_inter_msg_speed_ms` values used longitude/latitude order and could be
  much too small near frozen-longitude drift artefacts.
- Add `position_spread_m` to the per-MMSI daily index. It is the great-circle
  distance between opposing corners of the day's lat/lon bounding box.
- Add `stationary_position_suspect` to the per-MMSI daily index. It is true
  when `message_count >= 10`, `sog_max <= 0.5`, and
  `position_spread_m >= 1000`.
- Regenerated 2024 and 2025 parquet outputs consistently with the changes
  above. Validation found 366 broadcast and 366 index files for 2024, 365
  broadcast and 365 index files for 2025, and broadcast row counts matched the
  summed index `message_count` for both years.
- Restored `2024-08-26` from the local NOAA ZIP source. The source was present
  locally, so this was a previous conversion/upload gap rather than a confirmed
  upstream NOAA absence.
