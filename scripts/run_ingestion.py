# Bước 1.1–1.2 | Chính | Điểm chạy nhập dữ liệu.
#
# Đầu vào
# - Cấu hình DuckDB/TLC (hoặc API cũ), nguồn CSV lookup, hai kỳ và đường dẫn raw/Parquet/manifest.
#
# Đầu ra
# - Provenance nguồn TLC, CSV lookup, Parquet, manifest và log số dòng/lỗi (API cũ giữ JSON).
#
# Yêu cầu
# - Chọn backend tlc.py/API trong ingest.py và công bố qua manifest.py.

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.ingestion.common import load_config
from nyc_taxi_pipeline.ingestion.ingest import run_ingestion


def main():
    parser = argparse.ArgumentParser(description="Read TLC Parquet with DuckDB and publish a manifest (legacy API supported)")
    parser.add_argument("--config", default="configs/demo.example.yaml")
    parser.add_argument("--role", choices=("all", "history", "replay", "zone_lookup"), default="all")
    parser.add_argument("--no-resume", action="store_true", help="Fetch every page again, ignoring partial attempts")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        roles = ("history", "replay", "zone_lookup") if args.role == "all" else (args.role,)
        config = load_config(args.config)
        if args.no_resume:
            config.setdefault("api", {})["resume"] = False
        results = run_ingestion(config, roles)
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return 0 if all(result["status"] == "ready" for result in results) else 1
    except Exception as error:
        logging.error("Ingestion failed: %s", error)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
