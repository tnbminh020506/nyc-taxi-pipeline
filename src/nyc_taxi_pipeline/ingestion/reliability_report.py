"""Aggregate all measurement batches without counting lookup calls as taxi API calls."""

import math
import re
import statistics
from collections import Counter


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def aggregate(reports, expected_runs=20):
    runs = [run for report in reports for run in report["runs"]]
    numbers = []
    for run in runs:
        if "run_number" in run:
            numbers.append(run["run_number"])
        else:
            raw = run["datasets"][0]["raw_path"] if run.get("datasets") else ""
            match = re.search(r"[/\\]run-(\d+)[/\\]", raw)
            numbers.append(int(match.group(1)) if match else None)
    coverage = sorted(number for number in numbers if number is not None) == list(range(1, expected_runs + 1))
    calls = [call for run in runs for call in run["http_requests"]]
    api = [call for call in calls if call["kind"] == "api"]
    first = sum(call["first_attempt_success"] for call in api)
    final = sum(call["success"] for call in api)
    statuses = Counter(str(status) for call in api
                       for status in (call.get("attempt_statuses") or ["unknown_transport_failure"]))
    successful_runs = sum(run["status"] == "passed" for run in runs)
    no_cache = all(entry.get("pages_reused", 0) == 0 for run in runs for entry in run["datasets"])
    time_ok = all(run["seconds"] <= run["time_budget_seconds"] for run in runs)
    rows = sum(check["parquet_rows"] for run in runs for check in run["checks"])
    summary = {
        "status": "passed" if (coverage and successful_runs == expected_runs and api
                               and final == len(api) and no_cache and time_ok) else "failed",
        "expected_runs": expected_runs, "recorded_runs": len(runs), "coverage_complete": coverage,
        "successful_runs": successful_runs, "fresh_downloads_only": no_cache,
        "api_logical_calls": len(api), "api_first_attempt_successes": first,
        "api_first_attempt_success_rate": first / len(api) if api else 0,
        "api_final_successes": final, "api_final_success_rate": final / len(api) if api else 0,
        "api_observed_attempt_statuses": dict(statuses),
        "lookup_logical_calls": len(calls) - len(api), "verified_parquet_rows": rows,
        "median_api_call_seconds": statistics.median(call["seconds"] for call in api) if api else None,
        "p95_api_call_seconds": percentile([call["seconds"] for call in api], .95),
        "median_run_seconds": statistics.median(run["seconds"] for run in runs) if runs else None,
        "p95_run_seconds": percentile([run["seconds"] for run in runs], .95),
        "runs": [{"number": number, "status": run["status"], "seconds": run["seconds"],
                  "duration_minutes": run["duration_minutes"], "checks": run["checks"],
                  "error": run.get("error")} for number, run in zip(numbers, runs)],
    }
    return summary
