"""TLC monthly Parquet -> bounded DuckDB COPY -> the shared ingestion contract.

No Socrata account/token is needed. Remote scans use projection/filter pushdown;
the source descriptor records provenance, not a claimed copy of the original file.
"""

import gzip
import logging
import os
import re
import time
import uuid
from pathlib import Path
from zoneinfo import ZoneInfo

import certifi
import duckdb

from .common import atomic_write, aware_time, resolve_path, source_time, write_json
from .ingest import file_sha256, http_session
from .manifest import upsert_manifest

LOG = logging.getLogger(__name__)
DEFAULT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


def literal(value):
    """Quote SQL values (paths/timezones included), never interpolate SQL syntax."""
    return "'" + str(value).replace("'", "''") + "'"


def sources_for_range(config, role):
    section = config["datasets"][role]
    zone = config.get("source_timezone", "America/New_York")
    start, end = (aware_time(section[key]).astimezone(ZoneInfo(zone)).replace(tzinfo=None)
                  for key in ("range_start", "range_end"))
    # Ambiguous endpoints cannot be represented by the naive TLC timestamp.
    source_time(start.isoformat(), zone)
    source_time(end.isoformat(), zone)
    if start >= end:
        raise ValueError("TLC local time range must be increasing")
    current = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    files = []
    source = config["source"]
    while current < end:
        month = current.strftime("%Y-%m")
        if "files" in source:
            if month not in source["files"]:
                raise ValueError(f"Missing TLC source file for {month}")
            path = str(resolve_path(config, source["files"][month]))
        else:
            path = source.get("url_template", DEFAULT_URL).format(month=month)
            if not path.startswith("https://"):
                raise ValueError("TLC remote source must use HTTPS")
        files.append({"month": month, "path": path, "filename": Path(path).name})
        current = current.replace(year=current.year + (current.month == 12),
                                  month=current.month % 12 + 1)
    if len({item["filename"] for item in files}) != len(files):
        raise ValueError("TLC filenames must be unique across months")
    return files, start, end


def connect(config, remote=False):
    settings = config.get("duckdb", {})
    temporary = resolve_path(config, settings.get("temp_directory", "output/duckdb-temp"))
    temporary.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(config={
        "memory_limit": settings.get("memory_limit", "512MB"),
        "threads": settings.get("threads", 2),
        "temp_directory": str(temporary),
        "max_temp_directory_size": settings.get("max_temp_directory_size", "8GB"),
    })
    try:
        if remote:
            # INSTALL's cache rename fails on some Windows machines. Download the
            # official signed extension into our own cache, then LOAD it. DuckDB
            # still verifies its signature; unsigned extensions remain disabled.
            version = duckdb.__version__
            platform = connection.execute("PRAGMA platform").fetchone()[0]
            if not all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in (version, platform)):
                raise ValueError("Unexpected DuckDB version/platform")
            cache = resolve_path(config, settings.get("extension_directory", "output/duckdb-extensions"))
            target = cache / version / platform / "httpfs.duckdb_extension"
            if not target.is_file():
                target.parent.mkdir(parents=True, exist_ok=True)
                url = f"https://extensions.duckdb.org/v{version}/{platform}/httpfs.duckdb_extension.gz"
                with http_session({"max_retries": 3}) as session:
                    response = session.get(url, timeout=60)
                    response.raise_for_status()
                    atomic_write(target, gzip.decompress(response.content))
            connection.execute(f"LOAD {literal(target)}")
            connection.execute("SET enable_server_cert_verification=true")
            connection.execute(f"SET ca_cert_file={literal(certifi.where())}")
            connection.execute("SET http_timeout=60")
            connection.execute("SET http_retries=4")
            connection.execute("SET http_retry_wait_ms=500")
            connection.execute("SET http_retry_backoff=2")
        connection.execute("SET TimeZone='UTC'")
        return connection
    except Exception:
        connection.close()
        raise


def selection_sql(files, start, end, zone):
    paths = "[" + ",".join(literal(item["path"]) for item in files) + "]"
    timezone = literal(zone)
    return f"""
        WITH selected AS (
            SELECT parse_filename(filename) AS source_filename, file_row_number,
                   tpep_pickup_datetime AS local_pickup, PULocationID AS raw_zone
            FROM read_parquet({paths}, union_by_name=true, filename=true, file_row_number=true)
            WHERE tpep_pickup_datetime >= TIMESTAMP {literal(start.isoformat())}
              AND tpep_pickup_datetime < TIMESTAMP {literal(end.isoformat())}
        ), converted AS (
            SELECT *, timezone({timezone}, local_pickup) AS converted_pickup,
                   TRY_CAST(raw_zone AS INTEGER) AS zone_id
            FROM selected
        ), checked AS (
            SELECT *, CASE
                WHEN timezone({timezone}, converted_pickup) <> local_pickup THEN 'nonexistent_pickup_time'
                WHEN timezone({timezone}, converted_pickup - INTERVAL '1 hour') = local_pickup
                  OR timezone({timezone}, converted_pickup + INTERVAL '1 hour') = local_pickup
                    THEN 'ambiguous_pickup_time'
                END AS time_error
            FROM converted
        )
        SELECT source_filename || '#' || file_row_number::VARCHAR AS source_record_id,
               file_row_number::BIGINT AS source_row,
               local_pickup::VARCHAR AS tpep_pickup_datetime,
               raw_zone::VARCHAR AS pulocationid,
               CASE WHEN time_error IS NULL THEN converted_pickup END AS pickup_at,
               CASE WHEN zone_id > 0 THEN zone_id END::INTEGER AS pickup_zone_id,
               NULLIF(concat_ws(';', time_error,
                   CASE WHEN zone_id IS NULL OR zone_id <= 0 THEN 'invalid_pickup_zone_id' END), '')
                   AS validation_error
        FROM checked
        ORDER BY local_pickup, source_filename, file_row_number
    """


def ingest_tlc(config, role):
    section = config["datasets"][role]
    name = f"tlc-{role}-{uuid.uuid4().hex}"
    raw = resolve_path(config, config["paths"]["raw"]) / name
    parquet = resolve_path(config, config["paths"]["parquet"]) / f"{name}.parquet"
    manifest = resolve_path(config, config["paths"]["manifest"])
    raw.mkdir(parents=True, exist_ok=True)
    parquet.parent.mkdir(parents=True, exist_ok=True)
    partial = parquet.with_suffix(".parquet.tmp")
    entry = {"dataset_role": role, "source_id": config["source"].get("source_id", "tlc-yellow-taxi"),
             "range_start": aware_time(section["range_start"]).isoformat(),
             "range_end": aware_time(section["range_end"]).isoformat(),
             "raw_path": str(raw), "parquet_path": str(parquet),
             "input_mode": "tlc_parquet", "source_timezone": config.get("source_timezone", "America/New_York"),
             "status": "downloading", "coverage_complete": False,
             "row_count": 0, "invalid_row_count": 0}
    upsert_manifest(manifest, entry)
    started = time.monotonic()
    try:
        files, start, end = sources_for_range(config, role)
        entry["source_access"] = "https" if any(f["path"].startswith("https://") for f in files) else "local"
        query = selection_sql(files, start, end, entry["source_timezone"])
        descriptor = {**entry, "source_files": files, "duckdb_version": duckdb.__version__,
                      "query": query, "original_files_retained": False,
                      "coverage_scope": "All selected monthly files, filtered by pickup [start,end); not an API snapshot"}
        write_json(raw / "source.json", descriptor)
        with connect(config, remote=any(f["path"].startswith("https://") for f in files)) as connection:
            source_rows = 0
            for item in files:
                columns = {row[0].lower(): row[1] for row in connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet({literal(item['path'])})").fetchall()}
                if (columns.get("tpep_pickup_datetime") not in ("TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_MS")
                        or columns.get("pulocationid") not in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT",
                                                               "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")):
                    raise ValueError(f"Unsupported TLC timestamp/zone schema: {item['filename']}")
                item["schema"] = columns
                # Metadata COUNT is provenance only; COPY below actually scans
                # the timestamp/zone columns and materializes every selected row.
                item["file_row_count"] = connection.execute(
                    f"SELECT count(*) FROM read_parquet({literal(item['path'])})").fetchone()[0]
                source_rows += item["file_row_count"]
            connection.execute(f"COPY ({query}) TO {literal(partial)} "
                               "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 122880)")
            count, invalid, outside = connection.execute(f"""
                SELECT count(*), count(*) FILTER (WHERE validation_error IS NOT NULL),
                       count(*) FILTER (WHERE pickup_at < TIMESTAMPTZ {literal(entry['range_start'])}
                                           OR pickup_at >= TIMESTAMPTZ {literal(entry['range_end'])})
                FROM read_parquet({literal(partial)})""").fetchone()
            if outside:
                raise ValueError("TLC output contains timestamps outside configured UTC range")
        os.replace(partial, parquet)
        entry.update(row_count=count, invalid_row_count=invalid, source_file_count=len(files),
                     source_file_rows=source_rows, parquet_sha256=file_sha256(parquet),
                     parquet_bytes=parquet.stat().st_size, duckdb_version=duckdb.__version__,
                     elapsed_seconds=round(time.monotonic() - started, 3))
        entry.update(status="ready", coverage_complete=True)
        descriptor.update(entry, source_files=files)
        write_json(raw / "source.json", descriptor)
    except Exception as error:
        partial.unlink(missing_ok=True)
        entry.update(status="failed", error=str(error), elapsed_seconds=round(time.monotonic() - started, 3))
        LOG.exception("TLC ingestion failed role=%s", role)
    upsert_manifest(manifest, entry)
    LOG.info("TLC role=%s status=%s rows=%d invalid=%d elapsed=%.3fs", role, entry["status"],
             entry["row_count"], entry["invalid_row_count"], entry["elapsed_seconds"])
    return entry
