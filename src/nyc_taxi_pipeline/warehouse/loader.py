"""Bước 3.1–3.2 | Bảo | Khởi tạo bảng và nạp dữ liệu vào PostgreSQL.

Hỗ trợ:
1. ``init_schema``: Thực thi ``database/schema.sql`` để khởi tạo 3 bảng và các index.
2. ``load_zone_lookup``: Nạp CSV Taxi Zone Lookup (trực tiếp hoặc từ mục ``zone_lookup`` ready trong manifest).
3. ``load_baseline``: Kiểm tra tính duy nhất + cùng kỳ tham chiếu và thay trọn bộ ``historical_baseline`` trong 1 giao dịch.
4. ``upsert_replay_counts`` & ``PostgresReplayUpsertSink``: Nạp/cập nhật số chuyến lũy kế vào ``replay_pickup_counts``.
"""
from __future__ import annotations

import csv
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from nyc_taxi_pipeline.warehouse.db import DATABASE_DIR, managed_connection

log = logging.getLogger(__name__)

SCHEMA_SQL_PATH = DATABASE_DIR / "schema.sql"
ZONE_LOOKUP_SQL_PATH = DATABASE_DIR / "load" / "zone_lookup.sql"
BASELINE_SQL_PATH = DATABASE_DIR / "load" / "baseline.sql"
REPLAY_COUNTS_SQL_PATH = DATABASE_DIR / "load" / "replay_counts.sql"


def _read_sql_statement(path: Path) -> str:
    """Đọc nội dung câu lệnh SQL từ file (bỏ qua các dòng comment đầu file nếu cần)."""
    return path.read_text(encoding="utf-8")


def _parse_iso_timestamp(value: Any) -> datetime:
    """Chuyển chuỗi ISO 8601 có múi giờ (bao gồm hậu tố Z) sang đối tượng datetime có tzinfo."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError(f"Mốc thời gian thiếu múi giờ (tzinfo): {value!r}")
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError("Mốc thời gian bị trống")
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(f"Mốc thời gian phải có múi giờ (offset): {value!r}")
    return dt


def init_schema(
    conn_or_dsn: Optional[Union[str, Any]] = None,
    schema_sql_path: Union[str, Path] = SCHEMA_SQL_PATH,
) -> None:
    """Bước 3.1: Thực thi ``database/schema.sql`` để tạo 3 bảng và các index trên PostgreSQL."""
    sql_text = Path(schema_sql_path).read_text(encoding="utf-8")
    with managed_connection(conn_or_dsn) as conn:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
    log.info("Đã khởi tạo schema PostgreSQL từ %s", schema_sql_path)


# ---------------------------------------------------------------------------
# Bước 3.2a: Nạp Taxi Zone Lookup
# ---------------------------------------------------------------------------
def resolve_zone_lookup_from_manifest(
    manifest_path: Union[str, Path],
    base_dir: Optional[Union[str, Path]] = None,
) -> Path:
    """Tìm đường dẫn ``raw_path`` của mục ``zone_lookup`` có ``status = ready`` trong file manifest."""
    mpath = Path(manifest_path)
    payload = json.loads(mpath.read_text(encoding="utf-8"))
    entries: List[Mapping[str, Any]] = (
        payload.get("datasets", []) if isinstance(payload, Mapping) else list(payload)
    )
    ready_entries = [
        e
        for e in entries
        if str(e.get("dataset_role") or e.get("role") or "").strip() == "zone_lookup"
        and str(e.get("status") or "").strip().lower() == "ready"
    ]
    if not ready_entries:
        raise ValueError(f"Không tìm thấy mục zone_lookup có status='ready' trong manifest: {mpath}")
    raw_path_str = str(ready_entries[-1].get("raw_path") or "").strip()
    if not raw_path_str:
        raise ValueError("Mục zone_lookup trong manifest thiếu trường 'raw_path'")
    raw_path = Path(raw_path_str)
    if not raw_path.is_absolute():
        root = Path(base_dir) if base_dir is not None else mpath.parent
        candidate = root / raw_path
        if candidate.exists():
            return candidate
        repo_candidate = DATABASE_DIR.parent / raw_path
        if repo_candidate.exists():
            return repo_candidate
    return raw_path


def _iter_zone_rows(source: Union[str, Path, Iterable[Mapping[str, Any]]]) -> Iterable[Mapping[str, Any]]:
    if isinstance(source, (str, Path)):
        path = Path(source)
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    else:
        for row in source:
            yield row


def _extract_field(row: Mapping[str, Any], candidates: Sequence[str]) -> Any:
    lower_map = {str(k).strip().lower(): v for k, v in row.items() if k is not None}
    for key in candidates:
        if key in row and row[key] is not None:
            return row[key]
        lk = key.lower()
        if lk in lower_map and lower_map[lk] is not None:
            return lower_map[lk]
    return None


def load_zone_lookup(
    conn_or_dsn: Optional[Union[str, Any]] = None,
    source: Optional[Union[str, Path, Iterable[Mapping[str, Any]]]] = None,
    manifest_path: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """Bước 3.2: Nạp bảng ``taxi_zones`` từ CSV hoặc danh sách bản ghi.

    Kiểm tra ánh xạ ``LocationID -> pickup_zone_id``, ``Zone -> zone_name``, ``Borough -> borough``.
    Cập nhật theo ``pickup_zone_id`` (idempotent khi nạp lại).
    """
    if source is None:
        if manifest_path is None:
            raise ValueError("Cần cung cấp 'source' (đường dẫn CSV/dòng dữ liệu) hoặc 'manifest_path'")
        source = resolve_zone_lookup_from_manifest(manifest_path)

    valid_by_id: Dict[int, Dict[str, Any]] = {}
    invalid_rows = 0
    errors: List[Dict[str, Any]] = []

    for idx, raw_row in enumerate(_iter_zone_rows(source), start=1):
        raw_id = _extract_field(raw_row, ("pickup_zone_id", "LocationID", "location_id", "pulocationid"))
        raw_zone = _extract_field(raw_row, ("zone_name", "Zone", "zone"))
        raw_borough = _extract_field(raw_row, ("borough", "Borough"))

        try:
            if raw_id is None or str(raw_id).strip() == "":
                raise ValueError("missing_pickup_zone_id")
            zone_id = int(str(raw_id).strip())
            if zone_id <= 0:
                raise ValueError("invalid_pickup_zone_id")
        except ValueError as exc:
            invalid_rows += 1
            errors.append({"row": idx, "reason": str(exc), "raw": dict(raw_row)})
            continue

        zone_name = str(raw_zone or "").strip()
        borough = str(raw_borough or "").strip()
        if not zone_name or zone_name.upper() in ("N/A", "NA", "NULL"):
            # Nếu file TLC có dòng Unknown/NV hoặc N/A ở cột Zone, cho phép fallback hoặc kiểm tra:
            if not zone_name:
                invalid_rows += 1
                errors.append({"row": idx, "reason": "missing_zone_name", "raw": dict(raw_row)})
                continue
        if not borough:
            invalid_rows += 1
            errors.append({"row": idx, "reason": "missing_borough", "raw": dict(raw_row)})
            continue

        valid_by_id[zone_id] = {
            "pickup_zone_id": zone_id,
            "zone_name": zone_name,
            "borough": borough,
        }

    upsert_sql = _read_sql_statement(ZONE_LOOKUP_SQL_PATH)
    records = list(valid_by_id.values())

    with managed_connection(conn_or_dsn) as conn:
        with conn:
            with conn.cursor() as cur:
                for rec in records:
                    cur.execute(upsert_sql, rec)

    summary = {
        "table": "taxi_zones",
        "loaded_rows": len(records),
        "invalid_rows": invalid_rows,
        "errors": errors,
    }
    log.info("Nạp taxi_zones hoàn tất: %d hợp lệ, %d lỗi", len(records), invalid_rows)
    return summary


# ---------------------------------------------------------------------------
# Bước 3.2b: Nạp mức lịch sử (Spark Batch -> historical_baseline)
# ---------------------------------------------------------------------------
def _iter_baseline_rows(source: Union[str, Path, Iterable[Mapping[str, Any]]]) -> Iterable[Mapping[str, Any]]:
    if isinstance(source, (str, Path)):
        path = Path(source)
        if path.is_dir():
            csv_files = sorted(path.glob("part-*.csv")) or sorted(path.glob("*.csv"))
            if not csv_files:
                raise FileNotFoundError(f"Không tìm thấy file CSV nào trong thư mục baseline: {path}")
            for csv_file in csv_files:
                with csv_file.open("r", encoding="utf-8-sig", newline="") as f:
                    for row in csv.DictReader(f):
                        yield row
        else:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    yield row
    else:
        for row in source:
            yield row


def load_baseline(
    conn_or_dsn: Optional[Union[str, Any]] = None,
    source: Union[str, Path, Iterable[Mapping[str, Any]]] = (),
    strict: bool = True,
) -> Dict[str, Any]:
    """Bước 3.2: Nạp bảng ``historical_baseline`` từ đầu ra Spark Batch.

    Yêu cầu:
    - Kiểm tra toàn bộ bản ghi thuộc cùng kỳ ``(history_start, history_end)`` và duy nhất theo
      khóa ``(pickup_zone_id, iso_weekday, slot_15m)``.
    - Thay trọn bộ bảng ``historical_baseline`` trong một giao dịch (Transaction) sau khi kiểm tra thành công.
    """
    validated: List[Dict[str, Any]] = []
    seen_keys: set[Tuple[int, int, int]] = set()
    seen_periods: set[Tuple[datetime, datetime]] = set()
    invalid_rows = 0
    errors: List[Dict[str, Any]] = []

    for idx, raw_row in enumerate(_iter_baseline_rows(source), start=1):
        try:
            zone_id = int(str(_extract_field(raw_row, ("pickup_zone_id",)) or "").strip())
            iso_weekday = int(str(_extract_field(raw_row, ("iso_weekday",)) or "").strip())
            slot_15m = int(str(_extract_field(raw_row, ("slot_15m",)) or "").strip())
            baseline_val = Decimal(str(_extract_field(raw_row, ("baseline_trip_count",)) or "").strip())
            ref_windows = int(str(_extract_field(raw_row, ("reference_windows",)) or "").strip())
            h_start = _parse_iso_timestamp(_extract_field(raw_row, ("history_start",)))
            h_end = _parse_iso_timestamp(_extract_field(raw_row, ("history_end",)))

            if zone_id <= 0:
                raise ValueError("pickup_zone_id phải > 0")
            if not (1 <= iso_weekday <= 7):
                raise ValueError("iso_weekday phải nằm trong 1..7")
            if not (0 <= slot_15m <= 95):
                raise ValueError("slot_15m phải nằm trong 0..95")
            if baseline_val < 0:
                raise ValueError("baseline_trip_count phải >= 0")
            if ref_windows <= 0:
                raise ValueError("reference_windows phải > 0")
            if h_end <= h_start:
                raise ValueError("history_end phải > history_start")

            key = (zone_id, iso_weekday, slot_15m)
            if key in seen_keys:
                raise ValueError(f"Trùng khóa (pickup_zone_id, iso_weekday, slot_15m) = {key}")
            seen_keys.add(key)

            period_key = (h_start, h_end)
            seen_periods.add(period_key)
            if len(seen_periods) > 1:
                raise ValueError(
                    "Đầu vào chứa nhiều kỳ lịch sử (history_start, history_end) khác nhau trong cùng một lần nạp"
                )

            validated.append(
                {
                    "pickup_zone_id": zone_id,
                    "iso_weekday": iso_weekday,
                    "slot_15m": slot_15m,
                    "baseline_trip_count": baseline_val,
                    "reference_windows": ref_windows,
                    "history_start": h_start.isoformat(),
                    "history_end": h_end.isoformat(),
                }
            )
        except (ValueError, InvalidOperation) as exc:
            invalid_rows += 1
            errors.append({"row": idx, "reason": str(exc), "raw": dict(raw_row)})
            if strict:
                raise ValueError(f"Dòng baseline thứ {idx} không hợp lệ: {exc}") from exc

    insert_sql = _read_sql_statement(BASELINE_SQL_PATH)
    with managed_connection(conn_or_dsn) as conn:
        with conn:  # Một transaction duy nhất: TRUNCATE + INSERT
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE historical_baseline;")
                for rec in validated:
                    cur.execute(insert_sql, rec)

    summary = {
        "table": "historical_baseline",
        "loaded_rows": len(validated),
        "invalid_rows": invalid_rows,
        "errors": errors,
    }
    log.info("Nạp historical_baseline hoàn tất: %d dòng hợp lệ, %d lỗi", len(validated), invalid_rows)
    return summary


# ---------------------------------------------------------------------------
# Bước 3.2c: Nạp số đếm lũy kế từ Spark Structured Streaming
# ---------------------------------------------------------------------------
def _iter_replay_rows(source: Union[str, Path, Iterable[Mapping[str, Any]]]) -> Iterable[Mapping[str, Any]]:
    if isinstance(source, (str, Path)):
        path = Path(source)
        if path.suffix.lower() in (".jsonl", ".ndjson"):
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)
        elif path.suffix.lower() == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            rows = data if isinstance(data, list) else data.get("rows", [])
            for row in rows:
                yield row
        else:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    yield row
    else:
        for row in source:
            yield row


def upsert_replay_counts(
    conn_or_dsn: Optional[Union[str, Any]] = None,
    source: Union[str, Path, Iterable[Mapping[str, Any]]] = (),
    strict: bool = True,
) -> Dict[str, Any]:
    """Bước 3.2: Nạp/cập nhật số chuyến lũy kế của stream vào bảng ``replay_pickup_counts``.

    Cập nhật theo khóa ``(replay_id, pickup_zone_id, window_start)``, gán ``trip_count`` bằng tổng mới.
    Ghi theo giao dịch; nạp lại cùng lần cập nhật giữ nguyên số dòng và số đếm.
    """
    deduped: Dict[Tuple[str, int, str], Dict[str, Any]] = {}
    invalid_rows = 0
    errors: List[Dict[str, Any]] = []

    for idx, raw_row in enumerate(_iter_replay_rows(source), start=1):
        try:
            replay_id = str(_extract_field(raw_row, ("replay_id",)) or "").strip()
            if not replay_id:
                raise ValueError("missing_replay_id")
            zone_id = int(str(_extract_field(raw_row, ("pickup_zone_id",)) or "").strip())
            if zone_id <= 0:
                raise ValueError("pickup_zone_id phải > 0")
            w_start = _parse_iso_timestamp(_extract_field(raw_row, ("window_start",)))
            w_end = _parse_iso_timestamp(_extract_field(raw_row, ("window_end",)))
            if w_end <= w_start:
                raise ValueError("window_end phải > window_start")
            trip_count = int(str(_extract_field(raw_row, ("trip_count", "observed_trip_count")) or "").strip())
            if trip_count < 0:
                raise ValueError("trip_count phải >= 0")

            key = (replay_id, zone_id, w_start.isoformat())
            deduped[key] = {
                "replay_id": replay_id,
                "pickup_zone_id": zone_id,
                "window_start": w_start.isoformat(),
                "window_end": w_end.isoformat(),
                "trip_count": trip_count,
            }
        except ValueError as exc:
            invalid_rows += 1
            errors.append({"row": idx, "reason": str(exc), "raw": dict(raw_row)})
            if strict:
                raise ValueError(f"Dòng replay_pickup_counts thứ {idx} không hợp lệ: {exc}") from exc

    upsert_sql = _read_sql_statement(REPLAY_COUNTS_SQL_PATH)
    records = list(deduped.values())

    with managed_connection(conn_or_dsn) as conn:
        with conn:
            with conn.cursor() as cur:
                for rec in records:
                    cur.execute(upsert_sql, rec)

    summary = {
        "table": "replay_pickup_counts",
        "loaded_rows": len(records),
        "invalid_rows": invalid_rows,
        "errors": errors,
    }
    log.info("Upsert replay_pickup_counts hoàn tất: %d dòng hợp lệ, %d lỗi", len(records), invalid_rows)
    return summary


class PostgresReplayUpsertSink:
    """Sink callable tương thích trực tiếp với ``foreachBatch`` trong ``scripts/run_stream.py`` của Mạnh."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self.dsn = dsn

    def __call__(self, rows: List[Dict[str, Any]], batch_id: int = 0) -> None:
        if not rows:
            return
        res = upsert_replay_counts(self.dsn, rows)
        log.info("micro-batch %s: đã upsert %d dòng vào replay_pickup_counts", batch_id, res["loaded_rows"])

