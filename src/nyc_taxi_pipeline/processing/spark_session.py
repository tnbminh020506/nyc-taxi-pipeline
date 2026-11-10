# Bước 2.1–2.3 | Mạnh | Tạo phiên Spark dùng chung cho batch và stream.
#
# Đầu vào
# - Tên ứng dụng, master (mặc định local[*] hoặc giá trị của spark-submit) và cấu hình bổ sung.
#
# Đầu ra
# - SparkSession có spark.sql.session.timeZone = America/New_York.
"""Tạo SparkSession với múi giờ nghiệp vụ đã đặt sẵn."""
from __future__ import annotations

from typing import Dict, Iterable, Mapping, Optional

from pyspark.sql import SparkSession

from nyc_taxi_pipeline.processing.pickup import BUSINESS_TZ, configure_session

DEFAULT_CONF = {
    # Dữ liệu demo nhỏ. 200 partition mặc định làm stream có state chạy chậm.
    # Lưu ý: số partition của một truy vấn stream được chốt trong checkpoint lần đầu chạy.
    "spark.sql.shuffle.partitions": "8",
}


def parse_conf(items: Iterable[str]) -> Dict[str, str]:
    """Đọc các cặp ``KEY=VALUE`` từ dòng lệnh."""
    conf: Dict[str, str] = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key.strip():
            raise ValueError(f"--conf cần dạng KEY=VALUE, nhận {item!r}")
        conf[key.strip()] = value.strip()
    return conf


def build_spark(
    app_name: str,
    master: Optional[str] = None,
    conf: Optional[Mapping[str, str]] = None,
    log_level: str = "WARN",
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    merged = dict(DEFAULT_CONF)
    merged.update(conf or {})
    merged["spark.sql.session.timeZone"] = BUSINESS_TZ
    for key, value in merged.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    configure_session(spark)
    spark.sparkContext.setLogLevel(log_level)
    return spark
