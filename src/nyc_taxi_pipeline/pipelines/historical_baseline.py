# Bước 2.2 | Mạnh | Tính mức lịch sử bằng Spark Batch.
#
# Đầu vào
# - Các mục ready có dataset_role = history trong manifest và file Parquet dẫn xuất tại parquet_path.
# - Danh sách cửa sổ có độ phủ đã xác nhận trong kỳ lịch sử và các vùng thuộc phạm vi so sánh.
# - Quy tắc kiểm tra và phân khung trong processing/pickup.py.
#
# Đầu ra
# - Một dòng cho mỗi bộ pickup_zone_id, iso_weekday, slot_15m trong kỳ tham chiếu đang dùng.
# - Các cột bàn giao gồm pickup_zone_id, iso_weekday, slot_15m, baseline_trip_count,
#   reference_windows, history_start và history_end.
# - Log số dòng đọc, hợp lệ và lỗi. Tập kết quả chuyển cho database/load/baseline.sql.
#
# Yêu cầu
# - Lọc đúng [range_start, range_end) của từng dòng manifest trước khi đếm.
# - baseline_trip_count là số chuyến trung bình trên các cửa sổ đã xác nhận độ phủ.
# - Mẫu số reference_windows gồm cả cửa sổ có độ phủ nhưng có 0 chuyến.
# - Chỉ tính từ cửa sổ đã xác nhận. Ghi rõ phạm vi khi mới xác nhận được một phần kỳ lịch sử.
"""Spark Batch: mức lịch sử theo vùng, thứ và khung 15 phút.

Các bước
--------
1. Đọc manifest, chọn mục ``dataset_role = history`` và ``status = ready``.
2. Đọc Parquet của từng mục, chuẩn hóa bằng ``pickup.prepare_pickups``, giữ dòng hợp lệ
   có ``pickup_at`` thuộc ``[range_start, range_end)`` của chính mục đó.
3. Đếm chuyến theo (vùng, cửa sổ 15 phút cụ thể).
4. Dựng lưới (vùng × mọi cửa sổ đã xác nhận độ phủ) rồi left join số đếm. Ô trống điền 0,
   nhờ vậy cửa sổ có độ phủ nhưng không có chuyến vẫn nằm trong mẫu số.
5. Lấy trung bình theo (vùng, thứ ISO, slot_15m). ``reference_windows`` là số cửa sổ tham gia.

Độ phủ
------
Thứ tự ưu tiên khi chọn cửa sổ đã xác nhận:

1. ``coverage`` truyền vào (``--coverage`` của ``run_batch.py``).
2. ``confirmed_windows`` của từng mục manifest (Chính ghi khi nhập bộ mẫu).
3. Toàn bộ ``[range_start, range_end)`` của mục, vì ``ready`` và ``coverage_complete``
   nghĩa là đã tải đủ phạm vi (trường hợp TLC/DuckDB).

Báo cáo chất lượng ghi lại phạm vi thực sự đã dùng và nguồn của nó.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from nyc_taxi_pipeline.processing import pickup as P

log = logging.getLogger(__name__)

HISTORY_ROLE = "history"
READY_STATUS = "ready"

BASELINE_COLUMNS = (
    P.PICKUP_ZONE_ID,
    P.ISO_WEEKDAY,
    P.SLOT_15M,
    "baseline_trip_count",
    "reference_windows",
    "history_start",
    "history_end",
)


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class TimeRange:
    """Khoảng thời gian ``[start, end)`` có múi giờ."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("Mốc thời gian phải có múi giờ")
        if self.start >= self.end:
            raise ValueError(f"Khoảng thời gian rỗng: {self.start.isoformat()} / {self.end.isoformat()}")

    @property
    def start_epoch(self) -> int:
        return int(self.start.timestamp())

    @property
    def end_epoch(self) -> int:
        return int(self.end.timestamp())

    def contains_window(self, window_start_epoch: int) -> bool:
        return self.start_epoch <= window_start_epoch and window_start_epoch + P.WINDOW_SECONDS <= self.end_epoch

    def label(self) -> str:
        return f"{self.start.isoformat()}/{self.end.isoformat()}"


@dataclass(frozen=True)
class ManifestEntry:
    dataset_role: str
    status: str
    source_id: Optional[str] = None
    raw_path: Optional[str] = None
    parquet_path: Optional[str] = None
    time_range: Optional[TimeRange] = None
    coverage_complete: Optional[bool] = None
    confirmed_windows: Tuple[TimeRange, ...] = ()
    extra: Dict[str, Any] = field(default_factory=dict, compare=False)

    @property
    def is_ready(self) -> bool:
        return self.status == READY_STATUS


def parse_timestamp(value: Any, field_name: str) -> datetime:
    """Đọc mốc ISO 8601 có offset. Mốc không có múi giờ bị từ chối vì dễ lệch giờ."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} phải là chuỗi ISO 8601, nhận {value!r}")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name}={value!r} thiếu múi giờ. Hợp đồng manifest yêu cầu mốc có offset.")
    return parsed


def parse_time_range(text: str) -> TimeRange:
    """Đọc ``START/END`` dạng ISO 8601, ví dụ ``2025-03-17T08:00:00-04:00/2025-03-17T08:15:00-04:00``."""
    parts = text.split("/")
    if len(parts) != 2:
        raise ValueError(f"Khoảng thời gian cần dạng START/END, nhận {text!r}")
    return TimeRange(parse_timestamp(parts[0], "start"), parse_timestamp(parts[1], "end"))


def parse_manifest_text(text: str) -> List[Dict[str, Any]]:
    """Chấp nhận mảng JSON, object ``{"entries": [...]}``, một object, hoặc JSON Lines."""
    stripped = text.strip()
    if not stripped:
        return []
    try:
        data = json.loads(stripped)
    except json.JSONDecodeError:
        data = [json.loads(line) for line in stripped.splitlines() if line.strip()]
    if isinstance(data, dict):
        for key in ("entries", "datasets", "items"):
            if isinstance(data.get(key), list):
                return list(data[key])
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("Manifest không phải danh sách mục hợp lệ")


def _parse_window(item: Any) -> TimeRange:
    """Một cửa sổ đã xác nhận: ``{"window_start", "window_end"}`` hoặc chuỗi ``START/END``."""
    if isinstance(item, str):
        return parse_time_range(item)
    if isinstance(item, dict):
        start = item.get("window_start", item.get("range_start"))
        end = item.get("window_end", item.get("range_end"))
        return TimeRange(parse_timestamp(start, "window_start"), parse_timestamp(end, "window_end"))
    raise ValueError(f"confirmed_windows có phần tử không hợp lệ: {item!r}")


def to_entry(raw: Dict[str, Any]) -> ManifestEntry:
    known = {
        "dataset_role", "status", "source_id", "raw_path", "parquet_path",
        "range_start", "range_end", "coverage_complete", "confirmed_windows",
    }
    start, end = raw.get("range_start"), raw.get("range_end")
    time_range = None
    if start not in (None, "") or end not in (None, ""):
        time_range = TimeRange(parse_timestamp(start, "range_start"), parse_timestamp(end, "range_end"))
    coverage_complete = raw.get("coverage_complete")
    return ManifestEntry(
        dataset_role=str(raw.get("dataset_role", "")),
        status=str(raw.get("status", "")),
        source_id=raw.get("source_id"),
        raw_path=raw.get("raw_path"),
        parquet_path=raw.get("parquet_path"),
        time_range=time_range,
        coverage_complete=None if coverage_complete is None else bool(coverage_complete),
        confirmed_windows=tuple(_parse_window(w) for w in (raw.get("confirmed_windows") or [])),
        extra={k: v for k, v in raw.items() if k not in known},
    )


def load_manifest(spark: SparkSession, manifest_uri: str) -> List[ManifestEntry]:
    """Đọc manifest từ HDFS hoặc ổ cục bộ (bất kỳ URI nào Hadoop đọc được)."""
    files = spark.sparkContext.wholeTextFiles(manifest_uri).collect()
    if not files:
        raise FileNotFoundError(f"Không đọc được manifest tại {manifest_uri}")
    entries: List[ManifestEntry] = []
    for _, content in sorted(files):
        entries.extend(to_entry(raw) for raw in parse_manifest_text(content))
    return entries


def select_history_entries(entries: Iterable[ManifestEntry]) -> List[ManifestEntry]:
    """Mục history đã ready. Báo lỗi nếu thiếu thông tin hoặc các khoảng chồng nhau."""
    chosen = [e for e in entries if e.dataset_role == HISTORY_ROLE and e.is_ready]
    if not chosen:
        raise ValueError("Manifest không có mục history nào ở trạng thái ready")
    for e in chosen:
        if not e.parquet_path or e.time_range is None:
            raise ValueError(f"Mục history {e.source_id!r} thiếu parquet_path hoặc range_start/range_end")
        if e.coverage_complete is False:
            raise ValueError(f"Mục history {e.time_range.label()} ở trạng thái ready nhưng coverage_complete=false")
    ordered = sorted(chosen, key=lambda e: e.time_range.start_epoch)
    for prev, cur in zip(ordered, ordered[1:]):
        if cur.time_range.start_epoch < prev.time_range.end_epoch:
            raise ValueError(
                f"Hai mục history chồng thời gian: {prev.time_range.label()} và {cur.time_range.label()}"
            )
    return ordered


# ---------------------------------------------------------------------------
# Độ phủ
# ---------------------------------------------------------------------------
def covered_window_starts(coverage: Sequence[TimeRange], data_ranges: Sequence[TimeRange]) -> List[int]:
    """Epoch giây của mọi cửa sổ 15 phút đã xác nhận, nằm trọn trong dữ liệu history."""
    starts = set()
    for r in coverage:
        for epoch in (r.start_epoch, r.end_epoch):
            if epoch % P.WINDOW_SECONDS:
                raise ValueError(f"Khoảng độ phủ {r.label()} không bắt đầu/kết thúc đúng mốc 15 phút")
        starts.update(range(r.start_epoch, r.end_epoch, P.WINDOW_SECONDS))
    outside = [s for s in starts if not any(d.contains_window(s) for d in data_ranges)]
    if outside:
        first = datetime.fromtimestamp(min(outside), tz=timezone.utc).isoformat()
        raise ValueError(
            f"{len(outside)} cửa sổ độ phủ nằm ngoài các mục history ready (sớm nhất {first}). "
            "Chỉ xác nhận độ phủ cho khoảng đã tải đủ."
        )
    return sorted(starts)


def _ts_literal(epoch: int) -> Column:
    return F.timestamp_seconds(F.lit(epoch))


# ---------------------------------------------------------------------------
# Tính mức lịch sử
# ---------------------------------------------------------------------------
@dataclass
class BaselineResult:
    baseline: DataFrame
    quality: Dict[str, Any]


def compute_baseline(
    spark: SparkSession,
    entries: Sequence[ManifestEntry],
    coverage: Optional[Sequence[TimeRange]] = None,
    zones: Optional[Sequence[int]] = None,
    pickup_col: Optional[str] = None,
    zone_col: Optional[str] = None,
) -> BaselineResult:
    """Tính mức lịch sử từ các mục manifest.

    ``zones`` là danh sách vùng thuộc phạm vi so sánh. Nếu bỏ trống, dùng mọi vùng hợp lệ xuất
    hiện trong dữ liệu history. Vùng không có chuyến nào trong history sẽ không có baseline, và
    truy vấn 3.3 báo "thiếu baseline".
    """
    P.ensure_business_timezone(spark)
    history = select_history_entries(entries)
    data_ranges = [e.time_range for e in history]
    if coverage:
        coverage, coverage_source = list(coverage), "argument"
    else:
        coverage = []
        for e in history:
            coverage.extend(e.confirmed_windows or [e.time_range])
        coverage_source = "manifest_confirmed_windows" if any(e.confirmed_windows for e in history) else "manifest_range"
    window_starts = covered_window_starts(coverage, data_ranges)

    # 1–2. Đọc, chuẩn hóa và lọc theo khoảng của từng mục.
    frames = []
    for e in history:
        prepared = P.prepare_pickups(spark.read.parquet(e.parquet_path), pickup_col, zone_col)
        in_range = (
            (F.col(P.PICKUP_AT) >= _ts_literal(e.time_range.start_epoch))
            & (F.col(P.PICKUP_AT) < _ts_literal(e.time_range.end_epoch))
        )
        frames.append(
            prepared.select(
                P.PICKUP_AT,
                P.PICKUP_ZONE_ID,
                P.REJECT_REASON,
                F.coalesce(in_range, F.lit(False)).alias("in_range"),
            )
        )
    rows = frames[0]
    for f in frames[1:]:
        rows = rows.unionByName(f)
    rows = rows.cache()

    quality: Dict[str, Any] = dict(P.collect_quality(rows))
    quality["rows_read"] = quality.pop("rows_total")
    quality["rows_out_of_range"] = rows.filter(P.is_valid() & ~F.col("in_range")).count()

    valid = P.valid_pickups(rows.filter(F.col("in_range")).drop("in_range"))
    counts = valid.groupBy(P.PICKUP_ZONE_ID, P.WINDOW_START).agg(F.count(F.lit(1)).alias("trip_count"))

    # 4. Lưới vùng × cửa sổ đã xác nhận.
    windows_df = spark.createDataFrame([(s,) for s in window_starts], "epoch long").select(
        F.timestamp_seconds("epoch").alias(P.WINDOW_START)
    )
    if zones:
        zones_df = spark.createDataFrame([(int(z),) for z in sorted(set(zones))], f"{P.PICKUP_ZONE_ID} int")
    else:
        zones_df = valid.select(P.PICKUP_ZONE_ID).distinct()
    grid = zones_df.crossJoin(windows_df)
    per_window = (
        grid.join(counts, [P.PICKUP_ZONE_ID, P.WINDOW_START], "left")
        .fillna(0, subset=["trip_count"])
    )
    per_window = P.add_calendar_columns(per_window, P.WINDOW_START).cache()

    # 5. Trung bình theo vùng, thứ và khung giờ.
    history_start = min(window_starts)
    history_end = max(window_starts) + P.WINDOW_SECONDS
    baseline = (
        per_window.groupBy(P.PICKUP_ZONE_ID, P.ISO_WEEKDAY, P.SLOT_15M)
        .agg(
            F.avg("trip_count").alias("baseline_trip_count"),
            F.count(F.lit(1)).cast("int").alias("reference_windows"),
        )
        .withColumn("history_start", _ts_literal(history_start))
        .withColumn("history_end", _ts_literal(history_end))
        .select(*BASELINE_COLUMNS)
        .orderBy(P.PICKUP_ZONE_ID, P.ISO_WEEKDAY, P.SLOT_15M)
    )

    rows_in_range = rows.filter(P.is_valid() & F.col("in_range")).count()
    rows_counted = int(per_window.agg(F.sum("trip_count")).first()[0] or 0)
    quality.update(
        {
            "rows_in_range": rows_in_range,
            "rows_outside_coverage": rows_in_range - rows_counted,
            "rows_counted": rows_counted,
            "zones": zones_df.count(),
            "covered_windows": len(window_starts),
            "history_entries": [
                {"source_id": e.source_id, "parquet_path": e.parquet_path, "range": e.time_range.label()}
                for e in history
            ],
            "coverage": [r.label() for r in coverage],
            "coverage_source": coverage_source,
            "coverage_is_full_history": sorted(window_starts) == _all_window_starts(data_ranges),
        }
    )
    return BaselineResult(baseline=baseline, quality=quality)


def _all_window_starts(ranges: Sequence[TimeRange]) -> List[int]:
    starts = set()
    for r in ranges:
        first = -(-r.start_epoch // P.WINDOW_SECONDS) * P.WINDOW_SECONDS
        starts.update(s for s in range(first, r.end_epoch, P.WINDOW_SECONDS) if r.contains_window(s))
    return sorted(starts)


# ---------------------------------------------------------------------------
# Bàn giao
# ---------------------------------------------------------------------------
def baseline_for_export(baseline: DataFrame) -> DataFrame:
    """Đổi các mốc thời gian sang chuỗi ISO có offset, phù hợp CSV và ``timestamptz``."""
    return baseline.withColumn("history_start", P.format_iso(F.col("history_start"))).withColumn(
        "history_end", P.format_iso(F.col("history_end"))
    )


def write_baseline(baseline: DataFrame, output_uri: str, fmt: str = "csv") -> None:
    """Ghi đè kết quả tại ``output_uri``.

    * ``csv``: một file ``part-*.csv`` có header trong thư mục ``output_uri``.
    * ``parquet``: giữ kiểu timestamp gốc.
    """
    if fmt == "csv":
        baseline_for_export(baseline).coalesce(1).write.mode("overwrite").option("header", True).csv(output_uri)
    elif fmt == "parquet":
        baseline.write.mode("overwrite").parquet(output_uri)
    else:
        raise ValueError(f"Định dạng đầu ra không hỗ trợ: {fmt}")
