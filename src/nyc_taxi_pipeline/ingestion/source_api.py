"""Collect ordered Socrata pages and verify coverage before publication."""

import json
import logging
import os
import re
import sqlite3
import time
from contextlib import closing
from datetime import timedelta
from email.utils import parsedate_to_datetime
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from .common import atomic_write, source_time
from .trip_rows import TRIP_SCHEMA, normalize_trip

LOG = logging.getLogger(__name__)
RETRY_STATUSES = {429, 500, 502, 503, 504}
TRIP_FIELDS = {
    "source_record_id": ":id", "pickup_at": "tpep_pickup_datetime",
    "dropoff_at": "tpep_dropoff_datetime", "pickup_zone_id": "pulocationid",
    "dropoff_zone_id": "dolocationid",
}
OPTIONAL_FIELDS = {"pickup_lat", "pickup_lon", "dropoff_lat", "dropoff_lon", "source_duration_seconds"}
IDENTIFIER = re.compile(r":?[a-zA-Z_][a-zA-Z_0-9]*")


def fetch_bytes(session, method, url, settings, **kwargs):
    retries = settings.get("max_retries", 3)
    timeout = settings.get("timeout_seconds", 30)
    if type(retries) is not int or not 0 <= retries <= 10 or not 0 < timeout <= 120:
        raise ValueError("Configure max_retries (0..10) and timeout_seconds (0..120]")
    for attempt in range(retries + 1):
        retry_after = None
        try:
            with session.request(method, url, timeout=(min(timeout, 8), timeout), **kwargs) as response:
                retry_after = response.headers.get("Retry-After")
                response.raise_for_status()
                return response.content
        except requests.HTTPError as error:
            if error.response.status_code not in RETRY_STATUSES or attempt == retries:
                raise
            reason = error.response.status_code
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as error:
            if isinstance(error, requests.exceptions.SSLError) or attempt == retries:
                raise
            reason = type(error).__name__
        delay = min(8, 0.5 * 2 ** attempt)
        if retry_after:
            try:
                requested = float(retry_after) if retry_after.isdigit() else parsedate_to_datetime(
                    retry_after).timestamp() - time.time()
            except (ValueError, TypeError, OverflowError):
                requested = 0
            if requested > 60:
                raise ValueError("Server requested a retry delay beyond the 60-second HTTP budget")
            delay = max(delay, requested)
        LOG.warning("Retrying source HTTP request method=%s reason=%s retry=%d", method, reason, attempt + 1)
        time.sleep(delay)


def _where(pickup_column, window, timezone):
    zone = ZoneInfo(timezone)
    start, end = (stamp.astimezone(zone) for stamp in window)
    cursor = window[0]
    offsets = {start.utcoffset(), end.utcoffset()}
    while cursor < window[1] and len(offsets) == 1:
        offsets.add(cursor.astimezone(zone).utcoffset())
        cursor += timedelta(hours=6)
    if len(offsets) != 1:
        raise ValueError("Split floating-timestamp API ranges around DST transitions")
    limits = [stamp.replace(tzinfo=None).isoformat() for stamp in (start, end)]
    for stamp in limits:
        source_time(stamp, timezone)
    return f"WHERE {pickup_column} >= '{limits[0]}' AND {pickup_column} < '{limits[1]}'"


def _page(session, settings, query, page, size):
    version = settings.get("version", 2)
    token_env = settings.get("app_token_env", "SOCRATA_APP_TOKEN")
    token = os.environ.get(token_env)
    headers = {"X-App-Token": token} if token else {}
    if version == 3:
        if not token:
            raise ValueError(f"Set {token_env} for SODA v3")
        raw = fetch_bytes(session, "POST", settings["endpoint"], settings, headers=headers, json={
            "query": query, "page": {"pageNumber": page, "pageSize": size},
            "includeSynthetic": False, "includeSystem": False,
        })
    elif version == 2:
        raw = fetch_bytes(session, "GET", settings["endpoint"], settings, headers=headers, params={
            "$query": f"{query} LIMIT {size} OFFSET {(page - 1) * size}",
        })
    else:
        raise ValueError("api.version must be 2 or 3")
    rows = json.loads(raw)
    if not isinstance(rows, list) or len(rows) > size or any(not isinstance(row, dict) for row in rows):
        raise ValueError("Unexpected JSON page shape or size")
    return raw, rows


def collect_pages(config, window, stage):
    settings = config["api"]
    fields = {**TRIP_FIELDS, **settings.get("fields", {})}
    if set(fields) - (TRIP_FIELDS.keys() | OPTIONAL_FIELDS) or any(
        not isinstance(value, str) or not IDENTIFIER.fullmatch(value) for value in fields.values()
    ):
        raise ValueError("Invalid API field mapping")
    size, max_pages = settings.get("page_size", 1000), settings.get("max_pages", 100)
    if type(size) is not int or not 1 <= size <= 50000 or type(max_pages) is not int or max_pages < 1:
        raise ValueError("Set page_size (1..50000) and a positive max_pages")
    where = _where(fields["pickup_at"], window, config["business_timezone"])
    count_query = f"SELECT count(*) AS row_count {where}"
    select = ", ".join(f"{column} AS {name}" for name, column in fields.items())
    query = f"SELECT {select} {where} ORDER BY {fields['pickup_at']} ASC, {fields['source_record_id']} ASC"
    pages = []
    with requests.Session() as session:
        before, expected = _page(session, settings, count_query, 1, 1)
        expected = int(expected[0]["row_count"])
        if not 0 <= expected <= max_pages * size:
            raise ValueError(f"API row count {expected} exceeds page budget ({max_pages} × {size})")
        atomic_write(stage / "count-before.json", before)
        for page in range(1, (expected + size - 1) // size + 1):
            raw, rows = _page(session, settings, query, page, size)
            if len(rows) != min(size, expected - (page - 1) * size):
                raise ValueError(f"API page {page} is incomplete")
            path = stage / f"page-{page:08d}.json"
            atomic_write(path, raw)
            pages.append(path)
        after, final_count = _page(session, settings, count_query, 1, 1)
        if int(final_count[0]["row_count"]) != expected:
            raise ValueError("API count changed during pagination")
        atomic_write(stage / "count-after.json", after)
    LOG.info("Collected %d pages and %d rows from %s", len(pages), expected, settings["dataset_id"])
    return pages, {
        "endpoint": settings["endpoint"], "api_version": settings.get("version", 2),
        "query": query, "count_query": count_query, "pages": len(pages),
        "page_size": size, "row_count": expected, "method": "ordered_pages_and_source_counts",
    }


def write_parquet(pages, file_uris, output, timezone, window):
    previous = None
    with closing(sqlite3.connect(output.parent / "source-ids.sqlite3")) as ids, pq.ParquetWriter(
        output, TRIP_SCHEMA, compression="zstd"
    ) as writer:
        ids.execute("CREATE TABLE source_id (value TEXT PRIMARY KEY) WITHOUT ROWID")
        for path in pages:
            normalized = []
            for row in json.loads(path.read_bytes()):
                identifier = row.get("source_record_id")
                if type(identifier) not in (str, int) or not str(identifier).strip():
                    raise ValueError("Source page has a missing or non-scalar row ID")
                try:
                    ids.execute("INSERT INTO source_id VALUES (?)", (str(identifier),))
                except sqlite3.IntegrityError as error:
                    raise ValueError(f"Duplicate source ID across API pages: {identifier}") from error
                item = normalize_trip(row, timezone, file_uris[path])
                timestamp = item["pickup_at"]
                if timestamp is not None:
                    if not window[0] <= timestamp < window[1] or (previous is not None and timestamp < previous):
                        raise ValueError("API page is outside the requested range or ordering")
                    previous = timestamp
                normalized.append(item)
            writer.write_table(pa.Table.from_pylist(normalized, schema=TRIP_SCHEMA))
