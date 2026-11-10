# Bước 2.3 | Mạnh | Đếm chuyến bằng Spark Structured Streaming.
#
# Đầu vào
# - JSON từ Kafka taxi-events, replay_id cần xử lý, checkpoint riêng của phiên và nhịp trigger.
# - Quy tắc kiểm tra và phân khung trong processing/pickup.py.
#
# Đầu ra
# - Một dòng cho mỗi vùng và cửa sổ của phiên, gồm replay_id, pickup_zone_id, window_start,
#   window_end và trip_count lũy kế. Chuyển tổng hiện tại sang database/load/replay_counts.sql.
# - Log số sự kiện hợp lệ, lỗi và lượt gửi trùng phát hiện được.
#
# Yêu cầu
# - Kiểm tra schema và loại bản ghi lỗi. Khử trùng theo replay_id và event_id trong phiên demo.
# - Gom theo pickup_at. Cấu hình nhịp trigger độc lập với cửa sổ nghiệp vụ 15 phút.
# - Dùng lại checkpoint khi tiếp tục cùng phiên. Dùng replay_id mới khi chạy lại demo từ đầu.
"""Spark Structured Streaming: số chuyến lũy kế theo phiên, vùng và cửa sổ 15 phút.

Luồng xử lý
-----------
::

    Kafka value (JSON) ─► parse_events ─► prepare_pickups (2.1) ─► observe (số liệu chất lượng)
        ─► chỉ giữ replay_id của phiên ─► withWatermark(pickup_at)
        ─► dropDuplicates(replay_id, event_id, pickup_at)
        ─► groupBy(replay_id, pickup_zone_id, window 15 phút).count()
        ─► outputMode("update") + foreachBatch ─► sink (upsert PostgreSQL / console)

* Cửa sổ tính theo **event time** ``pickup_at``, không theo lúc Spark nhận sự kiện.
* ``outputMode("update")``: mỗi micro-batch chỉ xuất các ô vừa thay đổi, kèm **tổng mới**.
  Sink ghi đè theo khóa ``(replay_id, pickup_zone_id, window_start)``, nên ghi lại một lô
  (khi Spark chạy lại lô sau sự cố) vẫn cho cùng kết quả.
* Watermark giới hạn state. Sự kiện có ``pickup_at`` trễ hơn mốc lớn nhất đã thấy quá
  ``watermark`` sẽ bị bỏ và đếm vào ``late_rows_dropped``. Replay phát theo thứ tự ``pickup_at``
  nên mặc định 1 giờ là đủ rộng.
* Khử trùng giữ ``pickup_at`` trong khóa để Spark xóa được state cũ theo watermark. Lần gửi lại
  của Chính mang cùng ``event_id`` và cùng ``pickup_at`` nên vẫn bị loại.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.streaming import StreamingQuery

from nyc_taxi_pipeline.processing import pickup as P

log = logging.getLogger(__name__)

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("replay_id", T.StringType()),
        T.StructField("event_id", T.StringType()),
        T.StructField("pickup_at", T.StringType()),
        T.StructField("pickup_zone_id", T.StringType()),
        T.StructField("source_file", T.StringType()),
    ]
)

REJECT_MALFORMED_EVENT = "malformed_event"
REJECT_MISSING_REPLAY_ID = "missing_replay_id"
REJECT_MISSING_EVENT_ID = "missing_event_id"
STREAM_REJECT_REASONS = (
    REJECT_MALFORMED_EVENT,
    REJECT_MISSING_REPLAY_ID,
    REJECT_MISSING_EVENT_ID,
) + P.PICKUP_REJECT_REASONS

QUALITY_METRICS_NAME = "replay_quality"
OUTPUT_COLUMNS = ("replay_id", P.PICKUP_ZONE_ID, P.WINDOW_START, P.WINDOW_END, "trip_count")
DEFAULT_WATERMARK = "1 hour"

Sink = Callable[[List[Dict[str, Any]], int], None]


# ---------------------------------------------------------------------------
# Đọc và kiểm tra sự kiện
# ---------------------------------------------------------------------------
def _blank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def parse_events(raw: DataFrame, value_col: str = "value") -> DataFrame:
    """Tách JSON sự kiện thành cột và gắn lý do loại cấp sự kiện vào ``reject_reason``."""
    text = F.col(value_col).cast("string")
    parsed = raw.select(text.alias("raw_value"), F.from_json(text, EVENT_SCHEMA).alias("event"))
    fields = parsed.select("raw_value", *[F.col(f"event.{f.name}").alias(f.name) for f in EVENT_SCHEMA])
    core = ["replay_id", "event_id", "pickup_at", "pickup_zone_id"]
    malformed = F.lit(True)
    for name in core:
        malformed = malformed & F.col(name).isNull()
    reason = (
        F.when(malformed, F.lit(REJECT_MALFORMED_EVENT))
        .when(_blank(F.col("replay_id")), F.lit(REJECT_MISSING_REPLAY_ID))
        .when(_blank(F.col("event_id")), F.lit(REJECT_MISSING_EVENT_ID))
    )
    return fields.withColumn(P.REJECT_REASON, reason)


def prepare_events(raw: DataFrame, replay_id: str, value_col: str = "value") -> DataFrame:
    """Sự kiện đã chuẩn hóa theo quy tắc 2.1, kèm cờ ``is_target_replay``."""
    events = parse_events(raw, value_col)
    prepared = P.prepare_pickups(events, pickup_col="pickup_at", zone_col="pickup_zone_id")
    return prepared.withColumn("is_target_replay", F.coalesce(F.trim(F.col("replay_id")) == replay_id, F.lit(False)))


def stream_quality_columns() -> List[Column]:
    reason = F.col(P.REJECT_REASON)
    target = F.col("is_target_replay")
    cols = [
        F.count(F.lit(1)).alias("rows_received"),
        F.sum(F.when(reason.isNull() & target, 1).otherwise(0)).cast("long").alias("rows_valid"),
        F.sum(F.when(reason.isNull() & ~target, 1).otherwise(0)).cast("long").alias("rows_other_replay"),
    ]
    for r in STREAM_REJECT_REASONS:
        cols.append(F.sum(F.when(reason == r, 1).otherwise(0)).cast("long").alias(P.reject_metric_name(r)))
    return cols


# ---------------------------------------------------------------------------
# Đếm lũy kế
# ---------------------------------------------------------------------------
def build_replay_counts(
    raw: DataFrame,
    replay_id: str,
    watermark: Optional[str] = DEFAULT_WATERMARK,
    value_col: str = "value",
) -> DataFrame:
    """Tạo DataFrame stream (hoặc batch) có các cột :data:`OUTPUT_COLUMNS`.

    ``raw`` cần một cột chuỗi JSON (``value``). Đó có thể là Kafka, file JSON Lines hoặc
    DataFrame tĩnh khi test.
    """
    prepared = prepare_events(raw, replay_id, value_col)
    if raw.isStreaming:
        prepared = prepared.observe(QUALITY_METRICS_NAME, *stream_quality_columns())
    valid = prepared.filter(P.is_valid() & F.col("is_target_replay")).select(
        F.trim(F.col("replay_id")).alias("replay_id"),
        F.trim(F.col("event_id")).alias("event_id"),
        P.PICKUP_AT,
        P.PICKUP_ZONE_ID,
    )
    if watermark:
        deduped = valid.withWatermark(P.PICKUP_AT, watermark).dropDuplicates(["replay_id", "event_id", P.PICKUP_AT])
    else:
        deduped = valid.dropDuplicates(["replay_id", "event_id"])
    counts = deduped.groupBy("replay_id", P.PICKUP_ZONE_ID, P.window_struct(F.col(P.PICKUP_AT)).alias("window")).agg(
        F.count(F.lit(1)).alias("trip_count")
    )
    return counts.select(
        "replay_id",
        P.PICKUP_ZONE_ID,
        F.col("window.start").alias(P.WINDOW_START),
        F.col("window.end").alias(P.WINDOW_END),
        "trip_count",
    )


def to_output_rows(batch_df: DataFrame) -> List[Dict[str, Any]]:
    """Gom một micro-batch về driver. Mốc thời gian là chuỗi ISO có offset New York.

    Mỗi lô chỉ chứa các ô vừa đổi (tối đa vài trăm vùng × vài cửa sổ) nên collect an toàn.
    """
    rows = (
        batch_df.select(
            "replay_id",
            P.PICKUP_ZONE_ID,
            P.format_iso(F.col(P.WINDOW_START)).alias(P.WINDOW_START),
            P.format_iso(F.col(P.WINDOW_END)).alias(P.WINDOW_END),
            F.col("trip_count").cast("long").alias("trip_count"),
        )
        .orderBy(P.WINDOW_START, P.PICKUP_ZONE_ID)
        .collect()
    )
    return [r.asDict() for r in rows]


# ---------------------------------------------------------------------------
# Nguồn
# ---------------------------------------------------------------------------
def read_kafka_events(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "earliest",
    max_offsets_per_trigger: Optional[int] = None,
) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
    return reader.load().select(F.col("value").cast("string").alias("value"))


def read_jsonl_events(spark: SparkSession, input_dir: str, max_files_per_trigger: int = 1) -> DataFrame:
    """Đọc thư mục JSON Lines như một stream (mỗi file là một lô). Dùng để thử khi chưa có Kafka."""
    return spark.readStream.option("maxFilesPerTrigger", str(max_files_per_trigger)).text(input_dir)


# ---------------------------------------------------------------------------
# Sink
# ---------------------------------------------------------------------------
def console_sink(rows: List[Dict[str, Any]], batch_id: int) -> None:
    if not rows:
        return
    print(f"--- micro-batch {batch_id}: {len(rows)} ô cập nhật")
    for r in rows:
        print(
            f"  {r['replay_id']} zone={r[P.PICKUP_ZONE_ID]} "
            f"[{r[P.WINDOW_START]}, {r[P.WINDOW_END]}) trip_count={r['trip_count']}"
        )


class PostgresUpsertSink:
    """Ghi theo hợp đồng ``database/load/replay_counts.sql``.

    Mỗi micro-batch chạy trong một giao dịch. Dòng mới được thêm, dòng đã có theo khóa
    ``(replay_id, pickup_zone_id, window_start)`` được gán ``trip_count`` bằng tổng mới.
    ``foreachBatch`` chạy tuần tự nên mỗi ``replay_id`` chỉ có một luồng ghi.

    Đây là cơ chế ghi tạm theo hợp đồng của Bảo, cần thay hoặc xác nhận khi Bảo bàn giao.
    """

    def __init__(self, dsn: str, table: str = "replay_pickup_counts") -> None:
        self.dsn = dsn
        self.table = table

    def sql(self) -> str:
        return (
            f"INSERT INTO {self.table} (replay_id, pickup_zone_id, window_start, window_end, trip_count) "
            "VALUES %s "
            "ON CONFLICT (replay_id, pickup_zone_id, window_start) "
            "DO UPDATE SET window_end = EXCLUDED.window_end, trip_count = EXCLUDED.trip_count"
        )

    def __call__(self, rows: List[Dict[str, Any]], batch_id: int) -> None:
        if not rows:
            return
        import psycopg2
        from psycopg2.extras import execute_values

        values = [
            (r["replay_id"], r[P.PICKUP_ZONE_ID], r[P.WINDOW_START], r[P.WINDOW_END], r["trip_count"]) for r in rows
        ]
        conn = psycopg2.connect(self.dsn)
        try:
            with conn:  # commit khi thành công, rollback khi lỗi
                with conn.cursor() as cur:
                    execute_values(cur, self.sql(), values)
        finally:
            conn.close()
        log.info("micro-batch %s: upsert %d dòng vào %s", batch_id, len(values), self.table)


# ---------------------------------------------------------------------------
# Chạy truy vấn
# ---------------------------------------------------------------------------
def start_replay_counts(
    counts: DataFrame,
    sink: Sink,
    checkpoint_location: str,
    trigger_interval: Optional[str] = "10 seconds",
    available_now: bool = False,
    query_name: Optional[str] = None,
) -> StreamingQuery:
    """Khởi động truy vấn. ``trigger_interval`` là nhịp đọc Kafka, không phải cửa sổ 15 phút."""

    def _process(batch_df: DataFrame, batch_id: int) -> None:
        sink(to_output_rows(batch_df), batch_id)

    writer = (
        counts.writeStream.outputMode("update")
        .foreachBatch(_process)
        .option("checkpointLocation", checkpoint_location)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    elif trigger_interval:
        writer = writer.trigger(processingTime=trigger_interval)
    if query_name:
        writer = writer.queryName(query_name)
    return writer.start()


def summarize_progress(progress: Dict[str, Any]) -> Dict[str, Any]:
    """Rút số liệu chất lượng từ một ``StreamingQueryProgress``."""
    summary: Dict[str, Any] = {
        "batch_id": progress.get("batchId"),
        "input_rows": progress.get("numInputRows", 0),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
    }
    observed = (progress.get("observedMetrics") or {}).get(QUALITY_METRICS_NAME) or {}
    for key, value in observed.items():
        summary[key] = int(value or 0)
    duplicates = 0
    late = 0
    # Lô không có dữ liệu (chỉ để đẩy watermark) báo lại số liệu cũ của operator, nên bỏ qua.
    operators = (progress.get("stateOperators") or []) if summary["input_rows"] else []
    for op in operators:
        late += int(op.get("numRowsDroppedByWatermark") or 0)
        if op.get("operatorName") == "dedupe":
            duplicates += int((op.get("customMetrics") or {}).get("numDroppedDuplicateRows") or 0)
    summary["duplicates_dropped"] = duplicates
    summary["late_rows_dropped"] = late
    return summary


def total_quality(summaries: List[Dict[str, Any]]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for s in summaries:
        for key, value in s.items():
            if key in ("batch_id", "watermark"):
                continue
            totals[key] = totals.get(key, 0) + int(value or 0)
    return totals


def wait_and_log(
    query: StreamingQuery,
    poll_seconds: float = 5.0,
    on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    timeout_seconds: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """Chờ truy vấn và gọi ``on_progress`` cho mỗi micro-batch mới. Trả về toàn bộ tóm tắt."""
    seen = set()
    summaries: List[Dict[str, Any]] = []
    deadline = time.monotonic() + timeout_seconds if timeout_seconds else None

    def drain() -> None:
        for progress in query.recentProgress:
            key = (progress.get("runId"), progress.get("batchId"))
            if key in seen:
                continue
            seen.add(key)
            summary = summarize_progress(progress)
            summaries.append(summary)
            if on_progress:
                on_progress(summary)

    try:
        while query.isActive:
            query.awaitTermination(poll_seconds)
            drain()
            if deadline is not None and time.monotonic() >= deadline:
                query.stop()
    finally:
        drain()
    if query.exception() is not None:
        raise RuntimeError(f"Truy vấn stream dừng vì lỗi: {query.exception()}")
    return summaries
