"""Offline Parquet contract tests; real-volume evidence lives in docs/evidence."""

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi_pipeline.ingestion.ingest import TRIP_SCHEMA, run_ingestion
from nyc_taxi_pipeline.ingestion.manifest import read_manifest
from nyc_taxi_pipeline.ingestion.replay import iter_events
from nyc_taxi_pipeline.ingestion.split import balanced_ranges
from nyc_taxi_pipeline.ingestion.tlc import sources_for_range


def local_source(config, month, rows):
    config["source"] = config.get("source", {"kind": "tlc_parquet", "files": {}})
    path = config["_root"] / f"yellow_tripdata_{month}.parquet"
    pq.write_table(pa.table({
        "tpep_pickup_datetime": pa.array([datetime.fromisoformat(t) for t, _ in rows], type=pa.timestamp("us")),
        "PULocationID": pa.array([z for _, z in rows], type=pa.int64()),
    }), path, row_group_size=2)
    config["source"]["files"][month] = str(path)
    config.pop("api", None)
    return config


def test_boundaries_duplicates_invalid_and_replay(config):
    local_source(config, "2025-01", [
        ("2025-01-20T07:59:59", 161),
        ("2025-01-20T08:00:00", 161),
        ("2025-01-20T08:00:00", 161),
        ("2025-01-20T08:01:00", None),
        ("2025-01-20T08:02:00", -1),
        ("2025-01-20T08:30:00", 161),
    ])
    # Fixture replay ends 08:30: this exact boundary must be excluded.
    config["datasets"]["replay"]["range_end"] = "2025-01-20T08:30:00-05:00"
    entry, lookup = run_ingestion(config, ("replay", "zone_lookup"))
    assert entry["status"] == lookup["status"] == "ready"
    assert (entry["row_count"], entry["invalid_row_count"]) == (4, 2)
    schema = pq.read_schema(entry["parquet_path"])
    # DuckDB writes the Parquet UTC annotation as Etc/UTC; Arrow considers it
    # equivalent in meaning but its timezone name differs from API's UTC.
    assert schema.names == TRIP_SCHEMA.names
    assert schema.field("pickup_at").type.unit == "us"
    assert schema.field("pickup_at").type.tz in ("UTC", "Etc/UTC")
    first = list(iter_events(config, "tlc-test"))
    assert len(first) == 2 and len({r["event_id"] for r in first}) == 2
    assert first == list(iter_events(config, "tlc-test"))
    rerun = run_ingestion(config, ("replay",))[0]
    assert rerun["parquet_sha256"] == entry["parquet_sha256"]
    # source_file deliberately points to a new immutable attempt; IDs stay stable.
    assert [e["event_id"] for e in iter_events(config, "tlc-test")] == [e["event_id"] for e in first]
    assert len(read_manifest(config["_root"] / config["paths"]["manifest"])["datasets"]) == 2
    metadata = json.loads((Path(rerun["raw_path"]) / "source.json").read_text())
    assert metadata["source_files"][0]["file_row_count"] == 6
    assert metadata["original_files_retained"] is False


def test_multimonth_and_missing_month_fail_closed(config):
    local_source(config, "2025-01", [("2025-01-31T23:59:59", 1)])
    local_source(config, "2025-02", [("2025-02-01T00:00:00", 2), ("2025-02-02T00:00:00", 3)])
    config["datasets"]["history"] = {"range_start": "2025-01-31T00:00:00-05:00",
                                      "range_end": "2025-02-02T00:00:00-05:00"}
    first = run_ingestion(config, ("history",))[0]
    assert first["status"] == "ready" and first["row_count"] == 2
    assert first["source_file_count"] == 2
    del config["source"]["files"]["2025-02"]
    failed = run_ingestion(config, ("history",))[0]
    assert failed["status"] == "failed" and not failed["coverage_complete"]
    assert "Missing TLC" in failed["error"]
    assert Path(first["parquet_path"]).is_file()


@pytest.mark.parametrize("month,day,label", [("2025-03", "2025-03-09T02:30:00", "nonexistent"),
                                                ("2025-11", "2025-11-02T01:30:00", "ambiguous")])
def test_dst_uncertainty_is_flagged(config, month, day, label):
    local_source(config, month, [(day, 161)])
    start_offset, end_offset = (("-05:00", "-04:00") if month.endswith("03") else ("-04:00", "-05:00"))
    config["datasets"]["history"] = {"range_start": f"{month}-01T00:00:00{start_offset}",
                                      "range_end": f"{month}-28T00:00:00{end_offset}"}
    entry = run_ingestion(config, ("history",))[0]
    assert entry["status"] == "ready", entry
    row = pq.read_table(entry["parquet_path"]).to_pylist()[0]
    assert row["pickup_at"] is None
    assert row["validation_error"] == label + "_pickup_time"


def test_ambiguous_endpoint_rejected(config):
    local_source(config, "2025-11", [("2025-11-02T01:30:00", 161)])
    config["datasets"]["history"] = {"range_start": "2025-11-02T01:30:00-04:00",
                                      "range_end": "2025-11-03T00:00:00-05:00"}
    with pytest.raises(ValueError, match="Ambiguous"):
        sources_for_range(config, "history")


def test_bad_schema_is_not_ready(config):
    local_source(config, "2025-01", [("2025-01-20T08:00:00", 161)])
    pq.write_table(pa.table({"wrong": [1]}), config["source"]["files"]["2025-01"])
    entry = run_ingestion(config, ("replay",))[0]
    assert entry["status"] == "failed" and not entry["coverage_complete"]


def test_one_bad_month_cannot_hide_behind_union_by_name(config):
    local_source(config, "2025-01", [("2025-01-31T23:59:59", 1)])
    local_source(config, "2025-02", [("2025-02-01T00:00:00", 2)])
    pq.write_table(pa.table({"wrong": [1]}), config["source"]["files"]["2025-02"])
    config["datasets"]["history"] = {"range_start": "2025-01-31T00:00:00-05:00",
                                      "range_end": "2025-02-02T00:00:00-05:00"}
    assert run_ingestion(config, ("history",))[0]["status"] == "failed"


def test_connection_failure_does_not_publish_ready(config, monkeypatch):
    local_source(config, "2025-01", [("2025-01-20T08:00:00", 161)])
    def failure(*args, **kwargs):
        raise OSError("simulated connection failure")
    monkeypatch.setattr("nyc_taxi_pipeline.ingestion.tlc.connect", failure)
    entry = run_ingestion(config, ("replay",))[0]
    assert entry["status"] == "failed" and not Path(entry["parquet_path"]).exists()


def test_record_split_keeps_time_ties_windows_and_excludes_invalid_zones(config):
    local_source(config, "2025-01", [("2025-01-06T08:00:00", 161)] * 4
                 + [("2025-01-06T08:15:00", 161)] * 2
                 + [("2025-01-06T08:30:00", 161)] * 4
                 + [("2025-01-06T08:00:00", None)])
    ranges, plan = balanced_ranges(config, list(config["source"]["files"].values()),
                                    "2025-01-06T08:00:00-05:00", "2025-01-06T09:00:00-05:00")
    config["datasets"] = ranges
    assert ranges["history"]["range_end"] == ranges["replay"]["range_start"] == "2025-01-06T08:30:00-05:00"
    assert plan["estimated_history_share"] == 0.6
    history, replay = run_ingestion(config, ("history", "replay"))
    assert (history["row_count"], history["invalid_row_count"], replay["row_count"]) == (7, 1, 4)
    assert len(list(iter_events(config, "split-test"))) == 4


def test_split_refuses_remote_or_a_single_indivisible_window(config):
    with pytest.raises(ValueError, match="local Parquet"):
        balanced_ranges(config, ["https://example.com/source.parquet"],
                        "2025-01-01T00:00:00-05:00", "2025-02-01T00:00:00-05:00")
    local_source(config, "2025-01", [("2025-01-06T08:00:00", 161)] * 10)
    with pytest.raises(ValueError, match="both sides"):
        balanced_ranges(config, list(config["source"]["files"].values()),
                        "2025-01-01T00:00:00-05:00", "2025-02-01T00:00:00-05:00")
