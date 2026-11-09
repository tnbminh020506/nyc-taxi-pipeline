"""Fresh sequential API-to-Parquet acceptance runs, with predeclared thresholds."""

import argparse
import copy
import json
import math
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from nyc_taxi_pipeline.ingestion.common import load_config, resolve_path, write_json
from nyc_taxi_pipeline.ingestion.live_smoke import run_live_smoke


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)]


def run(config, rounds, output, start_index=0, total_rounds=None):
    total_rounds = total_rounds or rounds
    target = resolve_path(config, output)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "runs": [],
              "criteria": {"required_successful_runs": rounds, "max_small_run_seconds": 180,
                           "max_multipage_run_seconds": 600, "cache_allowed": False,
                           "first_attempt_rate": "reported separately; provider availability is not under client control"},
              "api_version": config["api"]["version"], "endpoint": config["api"]["endpoint"],
              "query_style": config["api"].get("query_style", "query")}
    write_json(target, report)
    for index in range(start_index, start_index + rounds):
        candidate = copy.deepcopy(config)
        # Vary windows as well as connections, so the evidence is not solely one
        # repeatedly queried page. Final two runs use 15-minute, multi-page data.
        duration = 15 if index >= total_rounds - 2 else 1
        for section in candidate["datasets"].values():
            start = datetime.fromisoformat(section["range_start"]) + timedelta(minutes=index % 5)
            section["range_start"] = start.isoformat()
            section["range_end"] = (start + timedelta(minutes=duration)).isoformat()
        candidate["api"].update(page_size=100, max_pages=100, resume=False)
        run_dir = target.parent / f"run-{index + 1:03d}"
        candidate["_root"] = run_dir
        result = run_live_smoke(candidate, str(run_dir / "report.json"))
        result["run_number"] = index + 1
        result["duration_minutes"] = duration
        result["time_budget_seconds"] = 600 if duration == 15 else 180
        report["runs"].append(result)
        write_json(target, report)
        print(json.dumps({"run": index + 1, "status": result["status"], "seconds": result["seconds"],
                          "error": result.get("error"), "checks": result["checks"]}), flush=True)
        time.sleep(2)
    calls = [call for result in report["runs"] for call in result["http_requests"]]
    successes = sum(result["status"] == "passed" for result in report["runs"])
    api_calls = [call for call in calls if call["kind"] == "api"]
    first = sum(call["first_attempt_success"] for call in api_calls)
    statuses = Counter(str(status) for call in calls for status in (call.get("attempt_statuses") or ["unknown_transport_failure"]))
    durations = [result["seconds"] for result in report["runs"]]
    report["summary"] = {
        "runs": rounds, "successful_runs": successes, "logical_http_calls": len(calls),
        "successful_logical_calls": sum(call["success"] for call in calls),
        "api_logical_calls": len(api_calls),
        "first_attempt_success_rate": first / len(api_calls) if api_calls else 0,
        "first_attempt_successes": first, "observed_attempt_statuses": dict(statuses),
        "median_run_seconds": round(statistics.median(durations), 3),
        "p95_run_seconds": percentile(durations, .95),
        "median_logical_call_seconds": round(statistics.median(call["seconds"] for call in calls), 3) if calls else None,
        "p95_logical_call_seconds": percentile([call["seconds"] for call in calls], .95) if calls else None,
    }
    report["status"] = "passed" if (successes == rounds and calls and all(call["success"] for call in calls)
                                    and all(result["seconds"] <= result["time_budget_seconds"]
                                            for result in report["runs"])) else "failed"
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_json(target, report)
    print(json.dumps({"status": report["status"], "summary": report["summary"]}, indent=2))
    return report["status"] == "passed"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/api-smoke.yaml")
    parser.add_argument("--rounds", type=int, choices=range(3, 51), default=20)
    parser.add_argument("--output", default="output/api-reliability/acceptance.json")
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--total-rounds", type=int)
    args = parser.parse_args()
    raise SystemExit(0 if run(load_config(args.config), args.rounds, args.output,
                            args.start_index, args.total_rounds) else 1)
