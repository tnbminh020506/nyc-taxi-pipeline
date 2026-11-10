# Bước 2.3 | Mạnh | Điểm chạy Spark Structured Streaming.
#
# Đầu vào
# - Cấu hình Spark và Kafka, replay_id, checkpoint, nhịp trigger và đích PostgreSQL.
#
# Đầu ra
# - Các lần cập nhật tổng số chuyến theo vùng/cửa sổ trong kho và log chất lượng stream.
#
# Yêu cầu
# - Gọi pipelines/streaming_counts.py. Mạnh nối đầu ra mỗi lần cập nhật với cơ chế ghi do Bảo bàn giao ở bước 3.2.
# - database/load/replay_counts.sql mô tả cột, khóa và quy tắc cập nhật tổng lũy kế.
"""Đếm chuyến phát lại từ Kafka và ghi tổng lũy kế cho bước 3.2.

Ví dụ (khởi động trước khi Chính phát replay)::

    export POSTGRES_DSN="postgresql://taxi:taxi@localhost:5432/taxi"
    python scripts/run_stream.py --replay-id demo-01 \
        --kafka-bootstrap localhost:9092 --topic taxi-events \
        --checkpoint-root hdfs://namenode:9000/taxi/checkpoints/replay-counts \
        --sink postgres

* Checkpoint của phiên nằm tại ``<checkpoint-root>/<replay-id>``. Chạy lại cùng ``--replay-id``
  thì đọc tiếp từ checkpoint. Muốn demo lại từ đầu thì dùng ``--replay-id`` mới.
* ``--trigger`` là nhịp đọc Kafka, độc lập với cửa sổ nghiệp vụ 15 phút.
* ``--source files --input-dir DIR`` đọc JSON Lines thay Kafka để thử cục bộ.
* ``--available-now`` xử lý hết dữ liệu hiện có rồi dừng, phù hợp khi kiểm tra trên bộ mẫu.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.pipelines import streaming_counts as SC  # noqa: E402
from nyc_taxi_pipeline.processing.spark_session import build_spark, parse_conf  # noqa: E402

REPLAY_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bước 2.3: đếm chuyến phát lại bằng Spark Structured Streaming")
    p.add_argument("--replay-id", required=True, help="Mã phiên replay đã chốt với Chính")
    p.add_argument("--source", choices=("kafka", "files"), default="kafka")
    p.add_argument("--kafka-bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default="taxi-events")
    p.add_argument("--starting-offsets", default="earliest", help="Chỉ áp dụng khi checkpoint còn trống")
    p.add_argument("--max-offsets-per-trigger", type=int, help="Giới hạn số sự kiện mỗi micro-batch")
    p.add_argument("--input-dir", help="Thư mục JSON Lines khi --source files")
    p.add_argument("--checkpoint-root", default="checkpoints/replay-counts")
    p.add_argument("--trigger", default="10 seconds", help="Nhịp micro-batch, ví dụ '5 seconds'")
    p.add_argument("--available-now", action="store_true", help="Xử lý hết dữ liệu hiện có rồi dừng")
    p.add_argument("--watermark", default=SC.DEFAULT_WATERMARK, help="Độ trễ cho phép theo pickup_at. 'none' để tắt")
    p.add_argument("--sink", choices=("postgres", "console"), default="console")
    p.add_argument("--postgres-dsn-env", default="POSTGRES_DSN", help="Tên biến môi trường chứa chuỗi kết nối")
    p.add_argument("--postgres-table", default="replay_pickup_counts")
    p.add_argument("--timeout", type=float, help="Dừng sau N giây (mặc định chạy đến khi bị ngắt)")
    p.add_argument("--report", help="File cục bộ để ghi tổng số liệu chất lượng JSON khi dừng")
    p.add_argument("--master", help="Spark master, ví dụ local[*] hoặc spark://spark-master:7077")
    p.add_argument(
        "--conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Cấu hình Spark bổ sung, ví dụ spark.hadoop.dfs.replication=2 cho cụm HDFS hai DataNode",
    )
    return p.parse_args(argv)


def build_sink(args: argparse.Namespace) -> SC.Sink:
    if args.sink == "console":
        return SC.console_sink
    dsn = os.environ.get(args.postgres_dsn_env)
    if not dsn:
        raise SystemExit(f"Thiếu biến môi trường {args.postgres_dsn_env} chứa chuỗi kết nối PostgreSQL")
    return SC.PostgresUpsertSink(dsn, table=args.postgres_table)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    log = logging.getLogger("run_stream")

    if not REPLAY_ID_PATTERN.match(args.replay_id):
        raise SystemExit("--replay-id chỉ gồm chữ, số, '.', '_' hoặc '-' (tối đa 128 ký tự)")
    if args.source == "files" and not args.input_dir:
        raise SystemExit("--source files cần --input-dir")
    watermark = None if args.watermark.lower() == "none" else args.watermark
    checkpoint = f"{args.checkpoint_root.rstrip('/')}/{args.replay_id}"
    sink = build_sink(args)

    spark = build_spark(f"nyc-taxi-replay-counts-{args.replay_id}", master=args.master, conf=parse_conf(args.conf))
    if args.source == "kafka":
        raw = SC.read_kafka_events(
            spark, args.kafka_bootstrap, args.topic, args.starting_offsets, args.max_offsets_per_trigger
        )
    else:
        raw = SC.read_jsonl_events(spark, args.input_dir)

    counts = SC.build_replay_counts(raw, args.replay_id, watermark=watermark)
    query = SC.start_replay_counts(
        counts,
        sink,
        checkpoint_location=checkpoint,
        trigger_interval=args.trigger,
        available_now=args.available_now,
        query_name=f"replay_counts_{args.replay_id}".replace("-", "_").replace(".", "_"),
    )
    log.info("Đã khởi động stream replay_id=%s, checkpoint=%s, sink=%s", args.replay_id, checkpoint, args.sink)

    def on_progress(summary):
        if summary.get("input_rows") or summary.get("duplicates_dropped") or summary.get("late_rows_dropped"):
            log.info("chất lượng micro-batch: %s", json.dumps(summary, ensure_ascii=False))

    summaries = []
    try:
        summaries = SC.wait_and_log(query, on_progress=on_progress, timeout_seconds=args.timeout)
    except KeyboardInterrupt:
        log.info("Nhận Ctrl+C, đang dừng stream. Checkpoint được giữ để chạy tiếp.")
        query.stop()
    finally:
        totals = SC.total_quality(summaries)
        totals["replay_id"] = args.replay_id
        totals["checkpoint"] = checkpoint
        report = json.dumps(totals, ensure_ascii=False, indent=2)
        print(report)
        if args.report:
            Path(args.report).write_text(report + "\n", encoding="utf-8")
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
