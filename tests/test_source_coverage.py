"""Detect missing timestamp periods independently of Parquet filenames."""

from datetime import datetime

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi_pipeline.ingestion.coverage import audit_months, inspect_month


def _parquet(path, pickups):
    pq.write_table(pa.table({
        "tpep_pickup_datetime": pickups,
        "tpep_dropoff_datetime": pickups,
        "PULocationID": [1] * len(pickups), "DOLocationID": [2] * len(pickups),
    }), path)


def test_timestamp_scan_reports_records_outside_the_file_month(tmp_path):
    path = tmp_path / "yellow_tripdata_2025-01.parquet"
    _parquet(path, [datetime(2025, 1, 1), datetime(2025, 1, 31, 23, 59),
                    datetime(2024, 12, 31, 23, 59), None])
    with duckdb.connect() as connection:
        result = inspect_month(connection, str(path), "2025-01")
        wrong_month = inspect_month(connection, str(path), "2025-02")
    assert result["status"] == "present"
    assert (result["source_rows"], result["expected_month_rows"], result["other_month_or_invalid_rows"]) == (4, 2, 2)
    assert len(result["samples"]) == 2
    assert wrong_month["status"] == "missing_records" and wrong_month["samples"] == []


def test_audit_keeps_missing_month_error_and_does_not_create_a_dataset(tmp_path):
    path = tmp_path / "yellow_tripdata_2025-01.parquet"
    _parquet(path, [datetime(2025, 1, 1)])
    report = audit_months([2025], [1, 2], root=tmp_path, source_directory=tmp_path)
    assert not report["success"]
    assert report["present_month_count"] == 1
    assert [item["status"] for item in report["checks"]] == ["present", "error"]
    assert list(tmp_path.glob("*.parquet")) == [path]


def test_missing_dropoff_schema_fails_source_handoff_check(tmp_path):
    path = tmp_path / "invalid.parquet"
    pq.write_table(pa.table({"tpep_pickup_datetime": [datetime(2025, 1, 1)]}), path)
    with duckdb.connect() as connection, pytest.raises(ValueError, match="Missing source columns"):
        inspect_month(connection, str(path), "2025-01")
