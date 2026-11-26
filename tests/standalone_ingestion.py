"""Standalone checks for Minh: cloud source coverage or offline ingestion v2."""

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from nyc_taxi_pipeline.ingestion.common import atomic_write, write_json
from nyc_taxi_pipeline.ingestion.coverage import audit_months, coverage_markdown


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offline", action="store_true", help="Run fixture/lifecycle tests without external services")
    parser.add_argument("--years", nargs="+", type=int, default=[2025])
    parser.add_argument("--months", nargs="+", type=int, default=list(range(1, 13)))
    parser.add_argument("--local-directory", type=Path, help="Inspect files already present instead of HTTPS")
    parser.add_argument("--report", type=Path, default=ROOT / "output/standalone-ingestion/source-coverage.json")
    parser.add_argument("--markdown", type=Path, default=ROOT / "output/standalone-ingestion/source-coverage.md")
    args = parser.parse_args()
    if args.offline:
        return subprocess.call([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
                                str(ROOT / "tests/test_ingestion_v2.py"), str(ROOT / "tests/test_ingestion_workers.py"),
                                str(ROOT / "tests/test_source_coverage.py")],
                               cwd=ROOT)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    report = audit_months(args.years, args.months, root=ROOT, source_directory=args.local_directory)
    write_json(args.report, report)
    atomic_write(args.markdown, coverage_markdown(report).encode("utf-8"))
    print("MONTH    STATUS             SOURCE ROWS   IN-MONTH ROWS   OUTSIDE/INVALID")
    for item in report["checks"]:
        print(f"{item['month']}  {item['status']:<18} {item.get('source_rows', 0):>11,} "
              f"{item.get('expected_month_rows', 0):>15,} {item.get('other_month_or_invalid_rows', 0):>17,}")
    print(json.dumps({key: report[key] for key in ("success", "present_month_count", "requested_month_count",
                                                  "total_source_rows", "dataset_files_written")}))
    print(f"JSON {args.report.resolve()}\nMarkdown {args.markdown.resolve()}")
    return 0 if report["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
