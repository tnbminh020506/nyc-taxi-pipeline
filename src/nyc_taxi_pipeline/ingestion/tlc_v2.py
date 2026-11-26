"""Normalize original monthly TLC Parquet with bounded DuckDB memory."""

from datetime import timedelta
from zoneinfo import ZoneInfo

import duckdb
import pyarrow.parquet as pq

from .common import resolve_path
from .trip_rows import TRIP_SCHEMA

_ONE_HOUR = timedelta(hours=1)


def monthly_files(config, window):
    zone = ZoneInfo("America/New_York")
    if config["business_timezone"] != zone.key:
        raise ValueError("TLC monthly files use America/New_York")
    start, end = (value.astimezone(zone).replace(tzinfo=None) for value in window)
    cursor = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    files = {}
    while cursor < end or not files:
        key = cursor.strftime("%Y-%m")
        path = config["source"].get("files", {}).get(key)
        if not path:
            raise ValueError(f"Configure the original TLC Parquet path for {key} in source.files")
        path = resolve_path(config, path)
        schema = pq.read_schema(path)
        if not {"tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"}.issubset(schema.names):
            raise ValueError(f"{path.name} lacks required TLC pickup/dropoff fields")
        files[key] = path
        cursor = cursor.replace(year=cursor.year + (cursor.month == 12), month=cursor.month % 12 + 1)
    return files


def write_parquet(files, file_uris, output, window):
    sql = """
        WITH source AS ({sources}), converted AS (
            SELECT *, timezone('America/New_York', pu_local) AS pu,
                      timezone('America/New_York', do_local) AS do_at
            FROM source
        ), checked AS (
            SELECT *,
                CASE WHEN timezone('America/New_York', pu) = pu_local
                      AND timezone('America/New_York', pu - INTERVAL 1 HOUR) <> pu_local
                      AND timezone('America/New_York', pu + INTERVAL 1 HOUR) <> pu_local THEN pu END AS pickup_at,
                CASE WHEN timezone('America/New_York', do_at) = do_local
                      AND timezone('America/New_York', do_at - INTERVAL 1 HOUR) <> do_local
                      AND timezone('America/New_York', do_at + INTERVAL 1 HOUR) <> do_local THEN do_at END AS dropoff_at
            FROM converted
        ), normalized AS (
            SELECT source_record_id, source_file, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id,
                CASE WHEN pu_lat BETWEEN -90 AND 90 AND pu_lon BETWEEN -180 AND 180 THEN pu_lat END AS pickup_lat,
                CASE WHEN pu_lat BETWEEN -90 AND 90 AND pu_lon BETWEEN -180 AND 180 THEN pu_lon END AS pickup_lon,
                CASE WHEN do_lat BETWEEN -90 AND 90 AND do_lon BETWEEN -180 AND 180 THEN do_lat END AS dropoff_lat,
                CASE WHEN do_lat BETWEEN -90 AND 90 AND do_lon BETWEEN -180 AND 180 THEN do_lon END AS dropoff_lon,
                CASE WHEN dropoff_at >= pickup_at THEN epoch(dropoff_at - pickup_at) END AS source_duration_seconds
            FROM checked
            WHERE pickup_at >= ? AND pickup_at < ?
               OR (pickup_at IS NULL AND pu_local >= ? AND pu_local < ?)
        )
        SELECT *,
            nullif(concat_ws(';', CASE WHEN pickup_at IS NULL THEN 'invalid_pickup_time' END,
                                  CASE WHEN pickup_zone_id IS NULL THEN 'invalid_pickup_zone' END), '') AS validation_error,
            nullif(concat_ws(';', CASE WHEN dropoff_zone_id IS NULL THEN 'missing_dropoff_zone' END,
                                  CASE WHEN source_duration_seconds IS NULL THEN 'invalid_duration' END), '') AS matching_error
        FROM normalized
        ORDER BY pickup_at NULLS LAST, source_record_id
    """
    zone = ZoneInfo("America/New_York")
    limits = [value.astimezone(zone).replace(tzinfo=None) for value in window]
    # Include both wall-clock folds before converting to absolute instants.
    local_start = min(limits[0], (window[0] - _ONE_HOUR).astimezone(zone).replace(tzinfo=None))
    local_end = max(limits[1], (window[1] + _ONE_HOUR).astimezone(zone).replace(tzinfo=None))
    sources, args = [], []
    for month, path in files.items():
        columns = set(pq.read_schema(path).names)
        points = ", ".join(
            f'TRY_CAST("{name}" AS DOUBLE) AS {alias}' if name in columns else f"NULL::DOUBLE AS {alias}"
            for name, alias in (("pickup_latitude", "pu_lat"), ("pickup_longitude", "pu_lon"),
                                ("dropoff_latitude", "do_lat"), ("dropoff_longitude", "do_lon"))
        )
        sources.append(f"""
            SELECT ? || '#' || file_row_number::VARCHAR AS source_record_id, ? AS source_file,
                   TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS pu_local,
                   TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS do_local,
                   CASE WHEN TRY_CAST(PULocationID AS DOUBLE) = TRY_CAST(PULocationID AS INTEGER)
                             AND TRY_CAST(PULocationID AS INTEGER) > 0
                        THEN TRY_CAST(PULocationID AS INTEGER) END AS pickup_zone_id,
                   CASE WHEN TRY_CAST(DOLocationID AS DOUBLE) = TRY_CAST(DOLocationID AS INTEGER)
                             AND TRY_CAST(DOLocationID AS INTEGER) > 0
                        THEN TRY_CAST(DOLocationID AS INTEGER) END AS dropoff_zone_id,
                   {points}
            FROM read_parquet(?, file_row_number = true)
            WHERE tpep_pickup_datetime >= ? AND tpep_pickup_datetime < ?
        """)
        args.extend((month, file_uris[path], str(path), local_start, local_end))
    with duckdb.connect() as connection:
        connection.execute("SET TimeZone = 'UTC'")
        connection.execute("SET memory_limit = '512MB'")
        connection.execute("SET threads = 2")
        connection.execute("SET temp_directory = ?", [str(output.parent / "duckdb")])
        connection.execute("SET max_temp_directory_size = '8GB'")
        result = connection.execute(sql.format(sources=" UNION ALL ".join(sources)),
                                    [*args, *window, *limits]).to_arrow_reader(batch_size=65536)
        with pq.ParquetWriter(output, TRIP_SCHEMA, compression="zstd") as writer:
            for batch in result:
                writer.write_batch(batch.cast(TRIP_SCHEMA))
