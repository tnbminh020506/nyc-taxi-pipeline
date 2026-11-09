"""Run only when explicitly requested: makes real network calls and saves evidence."""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.ingestion.common import load_config
from nyc_taxi_pipeline.ingestion.live_smoke import run_live_smoke


def main():
    parser = argparse.ArgumentParser(description="Accept a small real NYC API download, not fixtures")
    parser.add_argument("--config", default="configs/api-smoke.yaml")
    parser.add_argument("--report", default="output/api-smoke/report.json")
    parser.add_argument("--api-version", type=int, choices=(2, 3), help="Explicitly select the official NYC endpoint")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = load_config(args.config)
    if args.api_version:
        config["api"]["version"] = args.api_version
        dataset = config["api"]["dataset_id"]
        config["api"]["endpoint"] = (
            f"https://data.cityofnewyork.us/resource/{dataset}.json" if args.api_version == 2
            else f"https://data.cityofnewyork.us/api/v3/views/{dataset}/query.json"
        )
    report = run_live_smoke(config, args.report)
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
