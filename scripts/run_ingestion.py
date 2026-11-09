# Bước 1.1–1.2 | Chính | Điểm chạy nhập dữ liệu.
#
# Đầu vào
# - Cấu hình NYC Open Data API, nguồn CSV lookup, hai kỳ dữ liệu và đường dẫn raw/Parquet/manifest.
#
# Đầu ra
# - Phản hồi JSON và CSV lookup nguyên bản, Parquet dẫn xuất, manifest và log số trang, số dòng, lỗi.
#
# Yêu cầu
# - Nối ingestion/ingest.py với ingestion/manifest.py. Tóm tắt kết quả theo file cho nhóm.

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.ingestion.common import load_config
from nyc_taxi_pipeline.ingestion.ingest import run_ingestion


def main():
    parser = argparse.ArgumentParser(description="Fetch taxi API pages, build Parquet and publish a manifest")
    parser.add_argument("--config", default="configs/demo.example.yaml")
    parser.add_argument("--role", choices=("all", "history", "replay", "zone_lookup"), default="all")
    parser.add_argument("--no-resume", action="store_true", help="Fetch every page again, ignoring partial attempts")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        roles = ("history", "replay", "zone_lookup") if args.role == "all" else (args.role,)
        config = load_config(args.config)
        if args.no_resume:
            config["api"]["resume"] = False
        results = run_ingestion(config, roles)
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 0 if all(result["status"] == "ready" for result in results) else 1
    except Exception as error:
        logging.error("Ingestion failed: %s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
