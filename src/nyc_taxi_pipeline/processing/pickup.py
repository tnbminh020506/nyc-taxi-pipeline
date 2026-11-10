# Bước 2.1 | Mạnh | Quy tắc kiểm tra và phân khung dùng chung.
#
# Đầu vào
# - Thời điểm đón khách và mã vùng từ Parquet hoặc JSON Kafka, kèm quy ước thời gian nguồn.
# - Độ dài cửa sổ 15 phút và múi giờ nghiệp vụ America/New_York.
#
# Đầu ra
# - Dòng hợp lệ có pickup_at với múi giờ, pickup_zone_id kiểu số nguyên, window_start, window_end,
#   iso_weekday từ 1 (thứ Hai) đến 7 (Chủ nhật) và slot_15m từ 0 đến 95 theo giờ địa phương.
# - Lý do loại nếu thiếu thời điểm, thời điểm không đọc được hoặc mã vùng không hợp lệ.
#
# Yêu cầu
# - Dùng cùng một quy tắc cho batch và stream. Kiểm tra mã vùng là số nguyên dương.
# - Cửa sổ dạng [bắt đầu, kết thúc). Mốc 08:15 vào khung 08:15–08:30.
# - Kiểm tra mã hợp lệ độc lập với việc Zone Lookup có tên vùng. Fixture đầu tiên tránh ngày đổi giờ.
"""Quy tắc chung cho Spark Batch (2.2) và Spark Structured Streaming (2.3).

Quy ước thời gian
-----------------
* Phiên Spark luôn đặt ``spark.sql.session.timeZone = America/New_York``. Mọi phép tính
  giờ, phút, thứ trong tuần đều dựa trên múi giờ phiên, nên ``iso_weekday`` và ``slot_15m``
  là giờ New York. Hàm :func:`ensure_business_timezone` chặn mọi phiên đặt sai múi giờ.
* Chuỗi thời gian **không có offset** (ví dụ ``2025-03-17T08:01:00.000`` của SODA, kiểu
  floating timestamp) được hiểu là giờ New York. Chuỗi **có offset** hoặc ``Z`` được đổi đúng
  theo offset đó. Cột Parquet kiểu ``timestamp`` được giữ nguyên thời điểm. Cột
  ``timestamp_ntz`` được hiểu là giờ New York.
* Cửa sổ 15 phút là ``[window_start, window_end)``, căn theo epoch như ``F.window``. Offset
  của New York luôn là số giờ nguyên nên mốc cửa sổ trùng mốc 00/15/30/45 giờ địa phương.

Quy ước mã vùng
---------------
Mã vùng hợp lệ là số nguyên dương vừa kiểu ``int``. Chuỗi phải chỉ gồm chữ số, ví dụ
``"161"``. Số thực chỉ hợp lệ khi không có phần lẻ, ví dụ ``161.0``. Việc kiểm tra không
phụ thuộc vào Zone Lookup.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BUSINESS_TZ = "America/New_York"
WINDOW_DURATION = "15 minutes"
WINDOW_SECONDS = 15 * 60
SLOTS_PER_DAY = 24 * 60 * 60 // WINDOW_SECONDS  # 96
MAX_ZONE_ID = 2**31 - 1
ISO_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"

# Tên cột chuẩn sau khi chuẩn hóa.
PICKUP_AT = "pickup_at"
PICKUP_ZONE_ID = "pickup_zone_id"
WINDOW_START = "window_start"
WINDOW_END = "window_end"
ISO_WEEKDAY = "iso_weekday"
SLOT_15M = "slot_15m"
REJECT_REASON = "reject_reason"

# Tên cột nguồn được nhận diện tự động, theo thứ tự ưu tiên.
PICKUP_AT_CANDIDATES = (PICKUP_AT, "tpep_pickup_datetime")
ZONE_ID_CANDIDATES = (PICKUP_ZONE_ID, "pulocationid")

# Cột lỗi do ingestion (Chính) đánh dấu. Dòng lỗi vẫn nằm trong Parquet để đối chiếu số lượng.
SOURCE_VALIDATION_COL = "validation_error"

# Lý do loại bản ghi.
REJECT_SOURCE_VALIDATION = "source_validation_error"
REJECT_MISSING_PICKUP_AT = "missing_pickup_at"
REJECT_UNPARSEABLE_PICKUP_AT = "unparseable_pickup_at"
REJECT_MISSING_ZONE_ID = "missing_pickup_zone_id"
REJECT_INVALID_ZONE_ID = "invalid_pickup_zone_id"
PICKUP_REJECT_REASONS = (
    REJECT_SOURCE_VALIDATION,
    REJECT_MISSING_PICKUP_AT,
    REJECT_UNPARSEABLE_PICKUP_AT,
    REJECT_MISSING_ZONE_ID,
    REJECT_INVALID_ZONE_ID,
)

_INTEGRAL_TYPES = (T.ByteType, T.ShortType, T.IntegerType, T.LongType)
_FRACTIONAL_TYPES = (T.FloatType, T.DoubleType, T.DecimalType)
_TIMESTAMP_NTZ_TYPE = getattr(T, "TimestampNTZType", None)


# ---------------------------------------------------------------------------
# Múi giờ của phiên Spark
# ---------------------------------------------------------------------------
def configure_session(spark: SparkSession) -> None:
    """Đặt múi giờ nghiệp vụ cho phiên Spark."""
    spark.conf.set("spark.sql.session.timeZone", BUSINESS_TZ)


def ensure_business_timezone(spark: SparkSession) -> None:
    """Báo lỗi nếu phiên Spark không dùng giờ New York. Khi đó thứ/khung giờ sẽ bị lệch."""
    current = spark.conf.get("spark.sql.session.timeZone")
    if current != BUSINESS_TZ:
        raise RuntimeError(
            f"spark.sql.session.timeZone đang là {current!r}, cần {BUSINESS_TZ!r}. "
            "Gọi configure_session(spark) hoặc tạo phiên bằng build_spark()."
        )


# ---------------------------------------------------------------------------
# Chuẩn hóa và kiểm tra hợp lệ
# ---------------------------------------------------------------------------
def resolve_column(df: DataFrame, preferred: Optional[str], candidates: Sequence[str], label: str) -> str:
    """Tìm tên cột nguồn: dùng ``preferred`` nếu có, nếu không thì thử ``candidates``."""
    if preferred:
        if preferred not in df.columns:
            raise ValueError(f"Không có cột {label} {preferred!r}. Các cột hiện có: {df.columns}")
        return preferred
    by_lower = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in by_lower:
            return by_lower[candidate.lower()]
    raise ValueError(f"Không tìm thấy cột {label}. Đã thử {list(candidates)}, các cột hiện có: {df.columns}")


def _pickup_at_expr(df: DataFrame, name: str) -> Tuple[Column, Column]:
    """Trả về (thời điểm đón kiểu timestamp, lý do loại hoặc NULL)."""
    src = df[name]
    dtype = df.schema[name].dataType
    if isinstance(dtype, T.TimestampType):
        missing = src.isNull()
        parsed = src
    elif _TIMESTAMP_NTZ_TYPE is not None and isinstance(dtype, _TIMESTAMP_NTZ_TYPE):
        missing = src.isNull()
        parsed = src.cast("timestamp")  # Giờ địa phương không offset → hiểu theo múi giờ phiên.
    elif isinstance(dtype, T.DateType):
        missing = src.isNull()
        parsed = src.cast("timestamp")
    else:
        text = F.trim(src.cast("string"))
        missing = src.isNull() | (text == "")
        parsed = F.to_timestamp(text)  # Không có offset → múi giờ phiên (New York).
    reason = (
        F.when(missing, F.lit(REJECT_MISSING_PICKUP_AT))
        .when(parsed.isNull(), F.lit(REJECT_UNPARSEABLE_PICKUP_AT))
    )
    return parsed, reason


def _zone_id_expr(df: DataFrame, name: str) -> Tuple[Column, Column]:
    """Trả về (mã vùng kiểu int hoặc NULL, lý do loại hoặc NULL)."""
    src = df[name]
    dtype = df.schema[name].dataType
    if isinstance(dtype, _INTEGRAL_TYPES):
        missing = src.isNull()
        well_formed = F.lit(True)
        as_long = src.cast("long")
    elif isinstance(dtype, _FRACTIONAL_TYPES):
        missing = src.isNull()
        if isinstance(dtype, (T.FloatType, T.DoubleType)):
            missing = missing | F.isnan(src)
        well_formed = src == F.floor(src)
        as_long = src.cast("long")
    else:
        text = F.trim(src.cast("string"))
        missing = src.isNull() | (text == "")
        well_formed = text.rlike(r"^[0-9]{1,10}$")
        as_long = F.when(well_formed, text.cast("long"))
    valid = F.coalesce(well_formed & (as_long > 0) & (as_long <= MAX_ZONE_ID), F.lit(False))
    reason = (
        F.when(missing, F.lit(REJECT_MISSING_ZONE_ID))
        .when(~valid, F.lit(REJECT_INVALID_ZONE_ID))
    )
    return F.when(valid, as_long.cast("int")), reason


def prepare_pickups(
    df: DataFrame,
    pickup_col: Optional[str] = None,
    zone_col: Optional[str] = None,
) -> DataFrame:
    """Chuẩn hóa thời điểm đón và mã vùng, gắn lý do loại cho từng dòng.

    Kết quả giữ các cột khác của ``df`` và có thêm:

    * ``pickup_at`` (timestamp, NULL nếu không đọc được),
    * ``pickup_zone_id`` (int, NULL nếu không hợp lệ),
    * ``reject_reason`` (NULL khi dòng hợp lệ, ngược lại là một giá trị trong
      :data:`PICKUP_REJECT_REASONS`). Lỗi thời điểm được báo trước lỗi mã vùng.

    Thứ tự ưu tiên lý do: ``reject_reason`` có sẵn (ví dụ lỗi JSON của stream), rồi cột
    ``validation_error`` của ingestion nếu có, rồi lỗi thời điểm, cuối cùng là lỗi mã vùng.
    """
    ensure_business_timezone(df.sparkSession)
    pickup_name = resolve_column(df, pickup_col, PICKUP_AT_CANDIDATES, "thời điểm đón")
    zone_name = resolve_column(df, zone_col, ZONE_ID_CANDIDATES, "mã vùng đón")

    pickup_at, pickup_reason = _pickup_at_expr(df, pickup_name)
    zone_id, zone_reason = _zone_id_expr(df, zone_name)
    reasons = [pickup_reason, zone_reason]
    if SOURCE_VALIDATION_COL in df.columns:
        flagged = F.coalesce(F.trim(df[SOURCE_VALIDATION_COL].cast("string")) != "", F.lit(False))
        reasons.insert(0, F.when(flagged, F.lit(REJECT_SOURCE_VALIDATION)))
    if REJECT_REASON in df.columns:
        reasons.insert(0, df[REJECT_REASON])

    replaced = {PICKUP_AT, PICKUP_ZONE_ID, REJECT_REASON}
    kept = [df[c] for c in df.columns if c not in replaced]
    return df.select(
        *kept,
        pickup_at.alias(PICKUP_AT),
        zone_id.alias(PICKUP_ZONE_ID),
        F.coalesce(*reasons).alias(REJECT_REASON),
    )


def is_valid() -> Column:
    return F.col(REJECT_REASON).isNull()


def valid_pickups(prepared: DataFrame) -> DataFrame:
    """Các dòng hợp lệ của :func:`prepare_pickups`, kèm cửa sổ, thứ và khung giờ."""
    return add_window_columns(prepared.filter(is_valid()).drop(REJECT_REASON))


def rejected_pickups(prepared: DataFrame) -> DataFrame:
    return prepared.filter(~is_valid())


# ---------------------------------------------------------------------------
# Cửa sổ 15 phút, thứ và khung giờ
# ---------------------------------------------------------------------------
def window_struct(ts: Column) -> Column:
    """Cửa sổ 15 phút ``[start, end)`` của một thời điểm. Batch và stream đều dùng hàm này."""
    return F.window(ts, WINDOW_DURATION)


def iso_weekday(ts: Column) -> Column:
    """1 = thứ Hai … 7 = Chủ nhật, theo giờ của phiên (New York).

    ``F.dayofweek`` trả 1 = Chủ nhật … 7 = thứ Bảy nên cần đổi sang chuẩn ISO.
    """
    return (((F.dayofweek(ts) + 5) % 7) + 1).cast("int")


def slot_15m(ts: Column) -> Column:
    """Số thứ tự khung 15 phút trong ngày địa phương: 0 = 00:00, 32 = 08:00, 95 = 23:45."""
    return (F.hour(ts) * 4 + F.floor(F.minute(ts) / 15)).cast("int")


def add_calendar_columns(df: DataFrame, start_col: str = WINDOW_START) -> DataFrame:
    """Thêm ``iso_weekday`` và ``slot_15m`` tính từ mốc đầu cửa sổ."""
    ensure_business_timezone(df.sparkSession)
    start = F.col(start_col)
    return df.withColumn(ISO_WEEKDAY, iso_weekday(start)).withColumn(SLOT_15M, slot_15m(start))


def add_window_columns(df: DataFrame, ts_col: str = PICKUP_AT) -> DataFrame:
    """Thêm ``window_start``, ``window_end``, ``iso_weekday`` và ``slot_15m``."""
    tmp = "__window"
    with_window = (
        df.withColumn(tmp, window_struct(F.col(ts_col)))
        .withColumn(WINDOW_START, F.col(f"{tmp}.start"))
        .withColumn(WINDOW_END, F.col(f"{tmp}.end"))
        .drop(tmp)
    )
    return add_calendar_columns(with_window, WINDOW_START)


def format_iso(col: Column) -> Column:
    """Chuỗi ISO 8601 có offset New York, ví dụ ``2025-03-31T08:00:00-04:00``.

    Dùng khi bàn giao timestamp ra ngoài Spark (CSV, PostgreSQL) để không phụ thuộc
    múi giờ của máy chạy Python.
    """
    return F.date_format(col, ISO_TIMESTAMP_FORMAT)


# ---------------------------------------------------------------------------
# Số liệu chất lượng
# ---------------------------------------------------------------------------
def reject_metric_name(reason: str) -> str:
    return f"rejected_{reason}"


def quality_metric_columns(reasons: Sequence[str] = PICKUP_REJECT_REASONS) -> List[Column]:
    """Biểu thức đếm: tổng dòng, dòng hợp lệ và số dòng bị loại theo từng lý do."""
    reason = F.col(REJECT_REASON)
    cols = [
        F.count(F.lit(1)).alias("rows_total"),
        F.sum(F.when(reason.isNull(), 1).otherwise(0)).cast("long").alias("rows_valid"),
    ]
    for r in reasons:
        cols.append(F.sum(F.when(reason == r, 1).otherwise(0)).cast("long").alias(reject_metric_name(r)))
    return cols


def collect_quality(prepared: DataFrame, reasons: Sequence[str] = PICKUP_REJECT_REASONS) -> Dict[str, int]:
    """Đếm chất lượng trên DataFrame batch đã qua :func:`prepare_pickups`."""
    row = prepared.agg(*quality_metric_columns(reasons)).first()
    return {k: int(v or 0) for k, v in row.asDict().items()}
