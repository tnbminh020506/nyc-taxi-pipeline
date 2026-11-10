# Bước 3.1–3.2 | Bảo | Điểm chạy khởi tạo schema và nạp dữ liệu vào PostgreSQL.
#
# Đầu vào
# - Chuỗi kết nối PostgreSQL (--dsn hoặc biến môi trường POSTGRES_DSN).
# - Đường dẫn CSV/thư mục cho zone_lookup, baseline và replay_counts.
#
# Đầu ra
# - Bảng PostgreSQL được khởi tạo và nạp dữ liệu, kèm báo cáo JSON số dòng đã nạp và lỗi.
"""Khởi tạo bảng PostgreSQL và nạp dữ liệu từ Zone Lookup, Spark Batch Baseline, Spark Stream.

Ví dụ::

    python scripts/load_warehouse.py --init-schema \
        --zone-lookup data/raw/taxi_zone_lookup.csv \
        --baseline data/processed/baseline \
        --replay-counts data/processed/replay_counts.csv
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.warehouse import (  # noqa: E402
    get_dsn,
    init_schema,
    load_baseline,
    load_zone_lookup,
    upsert_replay_counts,
)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bước 3.1–3.2: Khởi tạo schema và nạp kho dữ liệu PostgreSQL")
    p.add_argument("--dsn", help="Chuỗi kết nối PostgreSQL (mặc định đọc biến môi trường POSTGRES_DSN)")
    p.add_argument("--init-schema", action="store_true", help="Chạy database/schema.sql để tạo 3 bảng và index")
    p.add_argument("--zone-lookup", help="Đường dẫn file CSV Taxi Zone Lookup")
    p.add_argument("--manifest", help="Đường dẫn file manifest JSON để tự động tìm raw_path của zone_lookup")
    p.add_argument("--baseline", help="Đường dẫn file CSV hoặc thư mục part-*.csv đầu ra của Spark Batch")
    p.add_argument("--replay-counts", help="Đường dẫn file CSV/JSON/JSONL chứa số đếm của Spark Stream")
    p.add_argument("--report", help="Đường dẫn file JSON để lưu báo cáo kết quả nạp")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    dsn = get_dsn(args.dsn)

    report: Dict[str, Any] = {}
    if args.init_schema:
        init_schema(dsn)
        report["schema_initialized"] = True

    if args.zone_lookup or args.manifest:
        report["zone_lookup"] = load_zone_lookup(dsn, source=args.zone_lookup, manifest_path=args.manifest)

    if args.baseline:
        report["baseline"] = load_baseline(dsn, source=args.baseline)

    if args.replay_counts:
        report["replay_counts"] = upsert_replay_counts(dsn, source=args.replay_counts)

    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.report:
        out = Path(args.report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
