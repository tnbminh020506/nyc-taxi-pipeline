import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from nyc_taxi_pipeline.ingestion.common import write_json
from nyc_taxi_pipeline.ingestion.reliability_report import aggregate


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combine all completed batches, including failures")
    parser.add_argument("artifact_directory", type=Path)
    parser.add_argument("--expected-runs", type=int, default=20)
    parser.add_argument("--output", type=Path, default=Path("output/api-reliability/summary.json"))
    args = parser.parse_args()
    paths = sorted(args.artifact_directory.rglob("acceptance.json"))
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    result = aggregate(reports, args.expected_runs)
    result["source_reports"] = [str(path) for path in paths]
    write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result["status"] == "passed" else 1)
