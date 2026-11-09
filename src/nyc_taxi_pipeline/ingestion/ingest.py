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
import re
import uuid
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .common import atomic_write, aware_time, resolve_path, source_time, write_json
from .manifest import upsert_manifest

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


def http_session(api=None):
    api = api or {}
    session = requests.Session()
    retries = Retry(total=api.get("max_retries", 3),
                    backoff_factor=api.get("retry_backoff_seconds", 0.5),
                    backoff_max=api.get("retry_backoff_max_seconds", 20),
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=frozenset(("GET", "POST")), respect_retry_after_header=True)
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


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
    raw.mkdir(parents=True)
    parquet.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "dataset_role": role, "source_id": source_id,
        "range_start": aware_time(section["range_start"]).isoformat(),
        "range_end": aware_time(section["range_end"]).isoformat(),
        "raw_path": str(raw), "parquet_path": str(parquet),
        "status": "downloading", "coverage_complete": False,
        "page_count": 0, "row_count": 0, "invalid_row_count": 0,
        "source_timezone": zone_name,
    }
    if role == "history" and section.get("fixture_pages") is not None:
        entry["confirmed_windows"] = config.get("confirmed_history_windows", [])
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
                else:
                    content, rows = api_page(session, api, query, number)
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
                metadata["pages"].append({"file": page_file.name, "rows": len(rows)})
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
            response = session.get(source, timeout=config["api"].get("timeout_seconds", 30))
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
    with http_session(config.get("api")) as session:
        return [ingest_lookup(config, session) if role == "zone_lookup" else ingest_trips(config, role, session)
                for role in roles]
