# Bước 2.2 | Mạnh | Điểm chạy Spark Batch.
#
# Đầu vào
# - Cấu hình Spark, manifest history, các cửa sổ tham chiếu đã xác nhận và đích xuất kết quả.
#
# Đầu ra
# - Tập mức lịch sử để Bảo nạp vào PostgreSQL, kèm log chất lượng và kỳ tham chiếu.
#
# Yêu cầu
# - Gọi pipelines/historical_baseline.py. Chốt định dạng và vị trí đầu ra với Bảo trong cấu hình demo.
"""Tính mức lịch sử từ manifest và ghi kết quả cho bước 3.2.

Ví dụ::

    python scripts/run_batch.py \
        --manifest hdfs://namenode:9000/taxi/manifest.json \
        --output hdfs://namenode:9000/taxi/processed/baseline \
        --coverage 2025-03-17T00:00:00-04:00/2025-03-25T00:00:00-04:00

Đầu ra mặc định là CSV có header (một file ``part-*.csv`` trong thư mục ``--output``), gồm các
cột ``pickup_zone_id, iso_weekday, slot_15m, baseline_trip_count, reference_windows,
history_start, history_end``. Báo cáo chất lượng in ra dạng JSON và ghi thêm vào
``--report`` nếu có.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.pipelines import historical_baseline as HB  # noqa: E402
from nyc_taxi_pipeline.processing.spark_session import build_spark, parse_conf  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bước 2.2: mức lịch sử bằng Spark Batch")
    p.add_argument("--manifest", required=True, help="URI manifest (hdfs://... hoặc đường dẫn cục bộ)")
    p.add_argument("--output", required=True, help="Thư mục đầu ra, bị ghi đè mỗi lần chạy")
    p.add_argument("--format", choices=("csv", "parquet"), default="csv", help="Định dạng đầu ra (mặc định csv)")
    p.add_argument(
        "--coverage",
        action="append",
        default=[],
        metavar="START/END",
        help="Khoảng cửa sổ đã xác nhận độ phủ, có offset. Lặp lại được. Bỏ trống = toàn bộ mục history ready.",
    )
    p.add_argument("--zones", help="Danh sách mã vùng so sánh, cách nhau bằng dấu phẩy. Bỏ trống = vùng có trong history.")
    p.add_argument("--pickup-col", help="Tên cột thời điểm đón trong Parquet (mặc định tự nhận diện)")
    p.add_argument("--zone-col", help="Tên cột mã vùng đón trong Parquet (mặc định tự nhận diện)")
    p.add_argument("--report", help="File cục bộ để ghi báo cáo chất lượng JSON")
    p.add_argument("--master", help="Spark master, ví dụ local[*] hoặc spark://spark-master:7077")
    p.add_argument(
        "--conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Cấu hình Spark bổ sung, ví dụ spark.hadoop.dfs.replication=2 cho cụm HDFS hai DataNode",
    )
    p.add_argument("--show", type=int, default=0, help="In N dòng kết quả đầu tiên")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    log = logging.getLogger("run_batch")

    coverage = [HB.parse_time_range(c) for c in args.coverage]
    zones = [int(z) for z in args.zones.split(",") if z.strip()] if args.zones else None

    spark = build_spark("nyc-taxi-historical-baseline", master=args.master, conf=parse_conf(args.conf))
    try:
        entries = HB.load_manifest(spark, args.manifest)
        result = HB.compute_baseline(
            spark, entries, coverage=coverage, zones=zones, pickup_col=args.pickup_col, zone_col=args.zone_col
        )
        HB.write_baseline(result.baseline, args.output, args.format)
        quality = dict(result.quality)
        quality["output"] = args.output
        quality["output_format"] = args.format
        quality["output_rows"] = result.baseline.count()
        if args.show:
            HB.baseline_for_export(result.baseline).show(args.show, truncate=False)
    finally:
        spark.stop()

    report = json.dumps(quality, ensure_ascii=False, indent=2)
    print(report)
    if args.report:
        Path(args.report).write_text(report + "\n", encoding="utf-8")
    if not quality["coverage_is_full_history"]:
        log.warning("Chỉ dùng một phần kỳ lịch sử đã xác nhận độ phủ: %s", quality["coverage"])
    log.info("Đã ghi %d dòng mức lịch sử vào %s", quality["output_rows"], args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
