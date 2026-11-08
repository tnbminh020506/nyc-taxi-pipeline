# Bước 1.3–1.4 | Chính | Điểm chạy phát lại sự kiện.
#
# Đầu vào
# - Cấu hình demo, manifest, replay_id, địa chỉ Kafka, topic và tốc độ phát.
#
# Đầu ra
# - Luồng JSON pickup trên taxi-events và log số sự kiện đã gửi hoặc lỗi.
#
# Yêu cầu
# - Gọi ingestion/replay.py. Chạy sau khi Spark stream của phiên demo đã sẵn sàng.

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.ingestion.common import load_config
from nyc_taxi_pipeline.ingestion.replay import run_replay


def main():
    parser = argparse.ArgumentParser(description="Replay ready Parquet as pickup events")
    parser.add_argument("--config", default="configs/demo.example.yaml")
    parser.add_argument("--replay-id", help="Reuse for retries; change for a fresh demo")
    parser.add_argument("--dry-run", action="store_true", help="Write JSONL without connecting to Kafka")
    parser.add_argument("--output", help="JSONL destination for --dry-run, otherwise stdout")
    parser.add_argument("--create-topic", action="store_true", help="Create the single-broker demo topic if absent")
    args = parser.parse_args()
    if args.output and not args.dry_run:
        parser.error("--output requires --dry-run")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run_replay(load_config(args.config), args.replay_id, args.dry_run, args.output, args.create_topic)
        return 0
    except Exception as error:
        logging.error("Replay failed: %s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
