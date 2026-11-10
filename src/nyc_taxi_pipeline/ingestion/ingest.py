# Bước 1.1–1.2 | Chính | Gọi NYC Open Data API, lưu phản hồi và tạo Parquet.
#
# Đầu vào
# - Endpoint SODA, mã dataset, app token từ biến môi trường khi dùng SODA v3 và kích thước trang.
# - Hai kỳ history/replay theo tpep_pickup_datetime và các trường API cần lấy, gồm pulocationid.
# - CSV Taxi Zone Lookup và các đường dẫn lưu phản hồi gốc, Parquet và manifest.
#
# Đầu ra
# - Các trang JSON gốc, CSV lookup nguyên bản và Parquet dẫn xuất riêng cho history/replay.
# - Metadata bàn giao cho manifest.py gồm mã dataset, bộ lọc, đường dẫn, trạng thái và lỗi nếu có.
# - Log số trang, số dòng và tình trạng hoàn tất theo kỳ.
#
# Yêu cầu
# - Lọc theo [range_start, range_end) và phân trang theo thứ tự ổn định. Ghi rõ khi dừng do lỗi hoặc giới hạn.
# - Xác nhận ánh xạ từ trường API sang Parquet. Chỉ báo ready khi đã lấy đủ phạm vi và ghi Parquet xong.
# - Bàn giao cho Mạnh schema và parquet_path. Bàn giao cho Bảo raw_path và tên cột của lookup.

import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter

from .common import atomic_write, aware_time, resolve_path, source_time, write_json
from .manifest import entry_key, read_manifest, upsert_manifest

LOG = logging.getLogger(__name__)
TRIP_SCHEMA = pa.schema([
    ("source_record_id", pa.string()),
    ("source_row", pa.int64()),
    ("tpep_pickup_datetime", pa.string()),
    ("pulocationid", pa.string()),
    ("pickup_at", pa.timestamp("us", tz="UTC")),
    ("pickup_zone_id", pa.int32()),
    ("validation_error", pa.string()),
])


class MeasuredSession(requests.Session):
    """Bounded read-only API retries, including failures while reading the body."""

    def __init__(self, api):
        super().__init__()
        self.api = api
        self.metrics = api.get("_request_metrics")
        self.min_interval = api.get("min_request_interval_seconds", 0)
        self.last_request_start = None
        # Retries are implemented once, above the complete response-body read.
        # No hidden adapter retry can multiply the configured attempt budget.
        self.mount("https://", HTTPAdapter(max_retries=0))
        self.mount("http://", HTTPAdapter(max_retries=0))
        if api.get("connection_close", False):
            self.headers["Connection"] = "close"

    @staticmethod
    def retry_after(response):
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        try:
            delay = float(value)
            return max(0, delay) if 0 <= delay < float("inf") else None
        except ValueError:
            try:
                return max(0, parsedate_to_datetime(value).timestamp() - time.time())
            except (ValueError, TypeError, OverflowError):
                return None

    def request(self, method, url, **kwargs):
        started = time.monotonic()
        if self.last_request_start is not None:
            pause = max(0, self.min_interval - (started - self.last_request_start))
            if pause:
                time.sleep(pause)
        self.last_request_start = time.monotonic()
        deadline = started + self.api.get("request_budget_seconds", 120)
        event = {"method": method, "kind": "lookup" if "cloudfront.net" in url else "api"}
        statuses = []
        response, error = None, None
        max_retries = self.api.get("max_retries", 3)
        configured_timeout = kwargs.pop("timeout", self.api.get("timeout_seconds", 30))
        connect, read = configured_timeout if isinstance(configured_timeout, tuple) else (8, configured_timeout)
        try:
            for attempt in range(max_retries + 1):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    event["exhausted_reason"] = "request_budget"
                    break
                try:
                    response = super().request(method, url, timeout=(min(connect, remaining), min(read, remaining)), **kwargs)
                    # Reading content here also covers a caller that sets stream.
                    response.content
                    statuses.append(response.status_code)
                    error = None
                    retryable = response.status_code in (429, 500, 502, 503, 504)
                    if not retryable:
                        return response
                except requests.RequestException as caught:
                    response, error = None, caught
                    statuses.append(type(caught).__name__)
                    retryable = isinstance(caught, (requests.ConnectionError, requests.Timeout,
                        requests.exceptions.ChunkedEncodingError, requests.exceptions.ContentDecodingError))
                    if not retryable or isinstance(caught, requests.exceptions.SSLError):
                        raise
                if method.upper() not in ("GET", "POST") or attempt == max_retries:
                    event["exhausted_reason"] = "attempt_limit"
                    break
                delay = min(self.api.get("retry_backoff_max_seconds", 20),
                            self.api.get("retry_backoff_seconds", .5) * 2 ** attempt)
                delay = max(self.api.get("min_retry_delay_seconds", 0), delay)
                delay += random.uniform(0, self.api.get("retry_jitter_seconds", 0))
                if response is not None:
                    supplied = self.retry_after(response)
                    if supplied is not None:
                        delay = max(delay, supplied)
                    response.close()
                # Clear idle pooled connections after an error. The next attempt
                # performs a fresh connection to the same authorized endpoint.
                self.get_adapter(url).close()
                if delay >= deadline - time.monotonic():
                    event["exhausted_reason"] = "retry_delay_exceeds_budget"
                    break
                LOG.warning("HTTP retry method=%s attempt=%d status=%s wait=%.2fs", method,
                            attempt + 1, statuses[-1], delay)
                if delay:
                    time.sleep(delay)
            if response is not None:
                return response
            raise error or requests.Timeout("Request time budget exhausted")
        finally:
            success = response is not None and response.ok
            event.update(status=response.status_code if response is not None else None,
                         attempt_statuses=statuses, attempts=len(statuses), success=success,
                         first_attempt_success=success and len(statuses) == 1)
            if error is not None:
                event["error"] = type(error).__name__
            event["seconds"] = round(time.monotonic() - started, 3)
            if self.metrics is not None:
                self.metrics.append(event)


def http_session(api=None):
    return MeasuredSession(api or {})


def query_for_range(section, zone_name):
    # NYC floating timestamps represent local wall time, not UTC. Reject intervals
    # crossing an offset transition: they cannot be expressed unambiguously here.
    from zoneinfo import ZoneInfo
    zone = ZoneInfo(zone_name)
    start = aware_time(section["range_start"]).astimezone(zone)
    end = aware_time(section["range_end"]).astimezone(zone)
    if start.utcoffset() != end.utcoffset():
        raise ValueError("Split API ranges at DST transitions; use unambiguous local periods")
    cursor = start
    while cursor < end:
        if cursor.utcoffset() != start.utcoffset():
            raise ValueError("Split API ranges at DST transitions; use unambiguous local periods")
        cursor += timedelta(hours=12)
    for point in (start, end):
        source_time(point.replace(tzinfo=None).isoformat(), zone_name)
    lower, upper = (point.replace(tzinfo=None).isoformat() for point in (start, end))
    return ("SELECT :id AS source_record_id, tpep_pickup_datetime, pulocationid "
            f"WHERE tpep_pickup_datetime >= '{lower}' AND tpep_pickup_datetime < '{upper}' "
            "ORDER BY tpep_pickup_datetime ASC, :id ASC")


def api_page(session, api, query, page_number):
    size = api.get("page_size", 1000)
    token = os.environ.get(api.get("app_token_env", "SOCRATA_APP_TOKEN"))
    headers = {"X-App-Token": token} if token else {}
    if api.get("version", 3) == 3 and not token:
        raise ValueError(f"Set {api.get('app_token_env', 'SOCRATA_APP_TOKEN')} for SODA v3")
    timeout = api.get("timeout_seconds", 30)
    if api.get("version", 3) == 3:
        response = session.post(api["endpoint"], headers=headers, timeout=timeout, json={
            "query": query, "page": {"pageNumber": page_number, "pageSize": size},
            "includeSynthetic": False, "includeSystem": False,
        })
    else:
        response = session.get(api["endpoint"], headers=headers, timeout=timeout,
                               params={"$query": f"{query} LIMIT {size} OFFSET {(page_number - 1) * size}"})
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("API response must be a JSON array of objects")
    if len(rows) > size:
        raise ValueError("API returned more rows than requested")
    return response.content, rows


def normalize_row(row, row_number, zone_name):
    pickup, zone, errors = None, None, []
    try:
        pickup = source_time(row.get("tpep_pickup_datetime"), zone_name)
    except (ValueError, TypeError, OverflowError):
        errors.append("invalid_pickup_time")
    raw_zone = row.get("pulocationid")
    if isinstance(raw_zone, bool) or not re.fullmatch(r"[0-9]+", str(raw_zone)):
        errors.append("invalid_pickup_zone")
    else:
        zone = int(raw_zone)
        if not 0 < zone <= 2147483647:
            errors.append("invalid_pickup_zone")
            zone = None
    return {
        "source_record_id": str(row["source_record_id"]) if row.get("source_record_id") is not None else None,
        "source_row": row_number,
        "tpep_pickup_datetime": str(row["tpep_pickup_datetime"]) if row.get("tpep_pickup_datetime") is not None else None,
        "pulocationid": str(raw_zone) if raw_zone is not None else None,
        "pickup_at": pickup,
        "pickup_zone_id": zone,
        "validation_error": ";".join(errors) or None,
    }


def resumable_attempt(config, entry, query):
    """Only resume a non-ready attempt with an intact prefix of durable pages."""
    if config["api"].get("resume", True) is False:
        return None
    manifest = resolve_path(config, config["paths"]["manifest"])
    for previous in read_manifest(manifest)["datasets"]:
        if entry_key(previous) != entry_key(entry) or previous["status"] == "ready":
            continue
        raw = Path(previous["raw_path"]).resolve()
        parquet = Path(previous["parquet_path"]).resolve()
        if (not raw.is_relative_to(resolve_path(config, config["paths"]["raw"]))
                or not parquet.is_relative_to(resolve_path(config, config["paths"]["parquet"]))):
            continue
        try:
            metadata = json.loads((raw / "request.json").read_text(encoding="utf-8"))
            expected = (config["api"]["endpoint"], config["api"].get("version", 3),
                        config["api"].get("page_size", 1000), query)
            actual = (metadata["endpoint"], metadata["api_version"], metadata["page_size"], metadata["query"])
            if expected != actual:
                continue
            pages = []
            for number, page in enumerate(metadata["pages"], 1):
                if page["file"] != f"page-{number:06d}.json":
                    break
                path = raw / page["file"]
                if not page.get("sha256") or not path.is_file() or file_sha256(path) != page["sha256"]:
                    break
                pages.append(page)
            if pages:
                return raw, parquet, pages
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return None


def ingest_trips(config, role, session):
    section = config["datasets"][role]
    api = config["api"]
    zone_name = config.get("source_timezone", "America/New_York")
    source_id = api["dataset_id"]
    # Immutable per-attempt paths prevent a failed rerun from corrupting files
    # referenced by a previous manifest read. Only the manifest is replaced.
    run_name = f"{role}-{uuid.uuid4().hex}"
    raw = resolve_path(config, config["paths"]["raw"]) / run_name
    parquet = resolve_path(config, config["paths"]["parquet"]) / f"{run_name}.parquet"
    manifest = resolve_path(config, config["paths"]["manifest"])
    parquet.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "dataset_role": role, "source_id": source_id,
        "range_start": aware_time(section["range_start"]).isoformat(),
        "range_end": aware_time(section["range_end"]).isoformat(),
        "raw_path": str(raw), "parquet_path": str(parquet),
        "status": "downloading", "coverage_complete": False,
        "page_count": 0, "row_count": 0, "invalid_row_count": 0,
        "source_timezone": zone_name,
        "pages_reused": 0, "pages_downloaded": 0,
    }
    if role == "history" and section.get("fixture_pages") is not None:
        entry["confirmed_windows"] = config.get("confirmed_history_windows", [])
    cached_pages = []
    if section.get("fixture_pages") is None:
        query = query_for_range(section, zone_name)
        previous = resumable_attempt(config, entry, query)
        if previous:
            raw, parquet, cached_pages = previous
            entry.update(raw_path=str(raw), parquet_path=str(parquet))
            LOG.info("role=%s resuming %d cached pages", role, len(cached_pages))
    raw.mkdir(parents=True, exist_ok=True)
    upsert_manifest(manifest, entry)
    partial = parquet.with_suffix(".parquet.tmp")
    try:
        query = query_for_range(section, zone_name)
        metadata = {"source_id": source_id, "endpoint": api["endpoint"], "query": query,
                    "api_version": api.get("version", 3), "page_size": api.get("page_size", 1000),
                    "pages": [], "dataset_role": role, "source_timezone": zone_name,
                    "range_start": entry["range_start"], "range_end": entry["range_end"]}
        write_json(raw / "request.json", metadata)
        fixture_pages = section.get("fixture_pages")
        if fixture_pages is not None and not fixture_pages:
            raise ValueError("fixture_pages must contain at least one JSON page (use [] for an empty page)")
        limit = len(fixture_pages) if fixture_pages is not None else api.get("max_pages", 100)
        with pq.ParquetWriter(partial, TRIP_SCHEMA, compression="snappy") as writer:
            for number in range(1, limit + 1):
                if fixture_pages is not None:
                    content = resolve_path(config, fixture_pages[number - 1]).read_bytes()
                    rows = json.loads(content)
                    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                        raise ValueError("Fixture page must be an array of objects")
                elif number <= len(cached_pages):
                    content = (raw / cached_pages[number - 1]["file"]).read_bytes()
                    rows = json.loads(content)
                    entry["pages_reused"] += 1
                else:
                    content, rows = api_page(session, api, query, number)
                    entry["pages_downloaded"] += 1
                page_file = raw / f"page-{number:06d}.json"
                atomic_write(page_file, content)
                normalized = [normalize_row(row, entry["row_count"] + index, zone_name)
                              for index, row in enumerate(rows)]
                for row in normalized:
                    if row["pickup_at"] is not None and not (
                        aware_time(entry["range_start"]) <= row["pickup_at"] < aware_time(entry["range_end"])
                    ):
                        raise ValueError(f"Page {number}: source row outside configured {role} range")
                writer.write_table(pa.Table.from_pylist(normalized, schema=TRIP_SCHEMA))
                entry["row_count"] += len(rows)
                entry["invalid_row_count"] += sum(bool(row["validation_error"]) for row in normalized)
                entry["page_count"] += 1
                metadata["pages"].append({"file": page_file.name, "rows": len(rows),
                                           "sha256": hashlib.sha256(content).hexdigest()})
                write_json(raw / "request.json", metadata)
                LOG.info("role=%s page=%d rows=%d total=%d", role, number, len(rows), entry["row_count"])
                if fixture_pages is None and len(rows) < api.get("page_size", 1000):
                    entry["coverage_complete"] = True
                    break
            if fixture_pages is not None:
                # Explicit local fixture declaration, never evidence of API coverage.
                entry["coverage_complete"] = True
        entry["input_mode"] = "fixture" if fixture_pages is not None else "api"
        if entry["coverage_complete"]:
            os.replace(partial, parquet)
            entry["status"] = "ready"
            entry["parquet_sha256"] = file_sha256(parquet)
        else:
            entry["status"] = "incomplete"
            entry["error"] = "max_pages reached before an end-of-range page; increase max_pages and rerun"
            partial.unlink(missing_ok=True)
    except Exception as error:
        partial.unlink(missing_ok=True)
        entry["status"] = "failed"
        entry["error"] = str(error)
        LOG.error("role=%s failed: %s", role, error)
    upsert_manifest(manifest, entry)
    LOG.info("role=%s status=%s pages=%d rows=%d invalid=%d", role, entry["status"],
             entry["page_count"], entry["row_count"], entry["invalid_row_count"])
    return entry


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ingest_lookup(config, session):
    section = config["zone_lookup"]
    target = resolve_path(config, config["paths"]["raw"]) / f"zone-lookup-{uuid.uuid4().hex}.csv"
    entry = {"dataset_role": "zone_lookup", "source_id": section.get("source_id", "taxi-zone-lookup"),
             "range_start": None, "range_end": None, "raw_path": str(target),
             "parquet_path": None, "status": "downloading"}
    manifest = resolve_path(config, config["paths"]["manifest"])
    upsert_manifest(manifest, entry)
    try:
        source = section["source"]
        if source.startswith(("https://", "http://")):
            response = session.get(source, timeout=config.get("api", {}).get("timeout_seconds", 30))
            response.raise_for_status()
            content = response.content
        else:
            content = resolve_path(config, source).read_bytes()
        reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
        if not {"LocationID", "Zone", "Borough"}.issubset(reader.fieldnames or []):
            raise ValueError("Lookup CSV must contain LocationID, Zone, Borough")
        seen = set()
        for row in reader:
            zone_id = int(row["LocationID"])
            if zone_id <= 0 or zone_id in seen or not row["Zone"] or not row["Borough"]:
                raise ValueError("Lookup must have unique positive LocationID and nonempty Zone/Borough")
            seen.add(zone_id)
        if not seen:
            raise ValueError("Lookup CSV is empty")
        atomic_write(target, content)
        entry.update(status="ready", row_count=len(seen), sha256=file_sha256(target),
                     columns=reader.fieldnames)
    except Exception as error:
        entry.update(status="failed", error=str(error))
        LOG.error("zone_lookup failed: %s", error)
    upsert_manifest(manifest, entry)
    LOG.info("role=zone_lookup status=%s rows=%d", entry["status"], entry.get("row_count", 0))
    return entry


def run_ingestion(config, roles=("history", "replay", "zone_lookup")):
    if config.get("storage", {}).get("kind", "local") == "hdfs":
        from .hdfs import run_hdfs_ingestion
        return run_hdfs_ingestion(config, roles)
    if config.get("source", {}).get("kind", "api") == "tlc_parquet":
        from .tlc import ingest_tlc
        with http_session(config.get("api")) as session:
            return [ingest_lookup(config, session) if role == "zone_lookup" else ingest_tlc(config, role)
                    for role in roles]
    with http_session(config.get("api")) as session:
        return [ingest_lookup(config, session) if role == "zone_lookup" else ingest_trips(config, role, session)
                for role in roles]
