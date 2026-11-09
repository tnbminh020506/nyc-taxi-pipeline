import copy

from nyc_taxi_pipeline.ingestion.reliability_report import aggregate


def test_lookup_success_does_not_inflate_taxi_api_success_rate():
    runs = []
    for number in (1, 2):
        runs.append({"run_number": number, "status": "passed", "seconds": 10,
                     "time_budget_seconds": 180, "duration_minutes": 1,
                     "datasets": [{"pages_reused": 0}], "checks": [{"parquet_rows": 10}],
                     "http_requests": [
                         {"kind": "api", "first_attempt_success": number == 1, "success": True,
                          "attempt_statuses": [200] if number == 1 else [503, 200], "seconds": 5},
                         {"kind": "lookup", "first_attempt_success": True, "success": True,
                          "attempt_statuses": [200], "seconds": 1}]})
    result = aggregate([{"runs": runs}], expected_runs=2)
    assert result["status"] == "passed" and result["api_first_attempt_success_rate"] == .5
    assert result["api_final_success_rate"] == 1 and result["lookup_logical_calls"] == 2
    assert result["api_observed_attempt_statuses"] == {"200": 2, "503": 1}
    # Duplicating a successful measurement cannot replace a missing run.
    duplicated = [runs[0], copy.deepcopy(runs[0])]
    assert aggregate([{"runs": duplicated}], expected_runs=2)["status"] == "failed"
