# Bước 2.1–2.3 và 4.1 | Mạnh | Kiểm tra phần Data Processing trên bộ mẫu.
#
# Đầu vào
# - Bộ mẫu theo data/fixtures/README.md, dựng ngay trong thư mục tạm của pytest:
#   vùng A = 161, ba thứ Hai 2025-03-17, 2025-03-24 (history) và 2025-03-31 (replay), giờ EDT.
#
# Đầu ra
# - Batch: A, iso_weekday 1, slot_15m 32, baseline 2, reference_windows 2.
# - Stream: A có 3 chuyến trong 08:00–08:15 và 1 chuyến trong 08:15–08:30 dù có gửi trùng.
#
# Cách chạy
#   docker run --rm -v "$PWD":/workspace nyc-taxi-spark:dev python3 -m pytest -p no:cacheprovider tests/test_processing.py
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from nyc_taxi_pipeline.pipelines import historical_baseline as HB  # noqa: E402
from nyc_taxi_pipeline.pipelines import streaming_counts as SC  # noqa: E402
from nyc_taxi_pipeline.processing import pickup as P  # noqa: E402
from nyc_taxi_pipeline.processing.spark_session import build_spark  # noqa: E402

ZONE_A = 161
ZONE_B = 230
REPLAY_ID = "fixture-demo-01"
HISTORY_START = "2025-03-17T00:00:00-04:00"
HISTORY_END = "2025-03-25T00:00:00-04:00"


@pytest.fixture(scope="session")
def spark():
    session = build_spark(
        "test-processing",
        master="local[2]",
        conf={"spark.sql.shuffle.partitions": "2", "spark.ui.enabled": "false"},
        log_level="ERROR",
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# 2.1 Quy tắc chung
# ---------------------------------------------------------------------------
def _prepare(spark, rows, schema="tpep_pickup_datetime string, pulocationid string"):
    return P.prepare_pickups(spark.createDataFrame(rows, schema))


def test_window_slot_and_weekday_in_new_york_time(spark):
    rows = [
        ("2025-03-17T08:00:00.000", "161"),  # đầu khung 08:00
        ("2025-03-17T08:14:59.999", "161"),  # cuối khung 08:00
        ("2025-03-17T08:15:00.000", "161"),  # đúng 08:15 → khung sau
        ("2025-03-17T12:01:00Z", "161"),  # UTC 12:01 = 08:01 EDT
        ("2025-03-23T23:59:00", "161"),  # Chủ nhật, khung cuối ngày
    ]
    out = P.valid_pickups(_prepare(spark, rows)).select(
        P.format_iso(F.col(P.WINDOW_START)).alias("ws"),
        P.format_iso(F.col(P.WINDOW_END)).alias("we"),
        P.ISO_WEEKDAY,
        P.SLOT_15M,
    ).collect()
    assert [tuple(r) for r in out] == [
        ("2025-03-17T08:00:00-04:00", "2025-03-17T08:15:00-04:00", 1, 32),
        ("2025-03-17T08:00:00-04:00", "2025-03-17T08:15:00-04:00", 1, 32),
        ("2025-03-17T08:15:00-04:00", "2025-03-17T08:30:00-04:00", 1, 33),
        ("2025-03-17T08:00:00-04:00", "2025-03-17T08:15:00-04:00", 1, 32),
        ("2025-03-23T23:45:00-04:00", "2025-03-24T00:00:00-04:00", 7, 95),
    ]


def test_reject_reasons(spark):
    rows = [
        (None, "161"),
        ("   ", "161"),
        ("not-a-time", "161"),
        ("2025-03-17T08:01:00", None),
        ("2025-03-17T08:01:00", "abc"),
        ("2025-03-17T08:01:00", "0"),
        ("2025-03-17T08:01:00", "-5"),
        ("2025-03-17T08:01:00", "99999999999"),
        ("2025-03-17T08:01:00", " 7 "),
        (None, "abc"),  # lỗi thời điểm được báo trước
    ]
    prepared = _prepare(spark, rows)
    reasons = [r[P.REJECT_REASON] for r in prepared.collect()]
    assert reasons == [
        P.REJECT_MISSING_PICKUP_AT,
        P.REJECT_MISSING_PICKUP_AT,
        P.REJECT_UNPARSEABLE_PICKUP_AT,
        P.REJECT_MISSING_ZONE_ID,
        P.REJECT_INVALID_ZONE_ID,
        P.REJECT_INVALID_ZONE_ID,
        P.REJECT_INVALID_ZONE_ID,
        P.REJECT_INVALID_ZONE_ID,
        None,
        P.REJECT_MISSING_PICKUP_AT,
    ]
    assert prepared.filter(P.is_valid()).first()[P.PICKUP_ZONE_ID] == 7
    quality = P.collect_quality(prepared)
    assert quality["rows_total"] == 10 and quality["rows_valid"] == 1
    assert quality["rejected_invalid_pickup_zone_id"] == 4


def test_numeric_and_timestamp_source_types(spark):
    df = spark.createDataFrame(
        [("2025-03-17 08:01:00", 161.0), ("2025-03-17 08:02:00", 161.5), ("2025-03-17 08:03:00", float("nan"))],
        "s string, pulocationid double",
    ).select(F.to_timestamp("s").alias("pickup_at"), "pulocationid")
    reasons = [r[P.REJECT_REASON] for r in P.prepare_pickups(df).collect()]
    assert reasons == [None, P.REJECT_INVALID_ZONE_ID, P.REJECT_MISSING_ZONE_ID]


def test_wrong_session_timezone_is_refused(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    try:
        with pytest.raises(RuntimeError):
            _prepare(spark, [("2025-03-17T08:01:00", "161")])
    finally:
        P.configure_session(spark)


# ---------------------------------------------------------------------------
# 2.2 Spark Batch
# ---------------------------------------------------------------------------
def _write_history(spark, path, extra_rows=()):
    rows = [
        # Thứ Hai 1: A có 1 chuyến trong 08:00–08:15.
        ("2025-03-17T08:05:00.000", "161"),
        # Thứ Hai 2: A có 3 chuyến trong 08:00–08:15, B có 1 chuyến trong 08:15–08:30.
        ("2025-03-24T08:00:00.000", "161"),
        ("2025-03-24T08:07:00.000", "161"),
        ("2025-03-24T08:14:00.000", "161"),
        ("2025-03-24T08:20:00.000", "230"),
        # Ngoài kỳ history: bị loại trước khi đếm.
        ("2025-03-10T08:05:00.000", "161"),
        ("2025-03-25T08:05:00.000", "161"),
        # Bản ghi lỗi.
        (None, "161"),
        ("2025-03-17T08:06:00.000", "abc"),
    ] + list(extra_rows)
    spark.createDataFrame(rows, "tpep_pickup_datetime string, pulocationid string").write.mode("overwrite").parquet(
        str(path)
    )


def _write_manifest(path, history_parquet):
    entries = [
        {
            "dataset_role": "history",
            "source_id": "irx2-vgm4",
            "raw_path": "hdfs://namenode/taxi/raw/history",
            "parquet_path": str(history_parquet),
            "status": "ready",
            "range_start": HISTORY_START,
            "range_end": HISTORY_END,
        },
        {  # Chưa ready: phải bị bỏ qua dù đường dẫn không tồn tại.
            "dataset_role": "history",
            "source_id": "irx2-vgm4",
            "parquet_path": str(path.parent / "missing"),
            "status": "loading",
            "range_start": "2025-03-03T00:00:00-05:00",
            "range_end": "2025-03-10T00:00:00-04:00",
        },
        {
            "dataset_role": "replay",
            "source_id": "irx2-vgm4",
            "parquet_path": "hdfs://namenode/taxi/parquet/replay",
            "status": "ready",
            "range_start": "2025-03-31T00:00:00-04:00",
            "range_end": "2025-04-01T00:00:00-04:00",
        },
        {"dataset_role": "zone_lookup", "source_id": "taxi_zone_lookup", "raw_path": "x.csv", "status": "ready"},
    ]
    path.write_text(json.dumps(entries), encoding="utf-8")


def _baseline_rows(result):
    return {
        (r[P.PICKUP_ZONE_ID], r[P.ISO_WEEKDAY], r[P.SLOT_15M]): (r["baseline_trip_count"], r["reference_windows"])
        for r in result.baseline.collect()
    }


def test_batch_fixture_baseline(spark, tmp_path):
    _write_history(spark, tmp_path / "history")
    _write_manifest(tmp_path / "manifest.json", tmp_path / "history")
    entries = HB.load_manifest(spark, str(tmp_path / "manifest.json"))
    result = HB.compute_baseline(spark, entries)
    rows = _baseline_rows(result)

    assert rows[(ZONE_A, 1, 32)] == (2.0, 2)  # (1 + 3) / 2
    assert rows[(ZONE_B, 1, 33)] == (0.5, 2)  # (0 + 1) / 2, cửa sổ 0 chuyến vẫn vào mẫu số
    assert rows[(ZONE_A, 2, 32)] == (0.0, 1)  # thứ Ba chỉ có một lần trong kỳ
    # Đủ 2 vùng × 7 thứ × 96 khung.
    assert len(rows) == 2 * 7 * 96

    first = result.baseline.filter(F.col(P.PICKUP_ZONE_ID) == ZONE_A).first()
    q = result.quality
    assert q["rows_read"] == 9
    assert q["rows_valid"] == 7
    assert q["rejected_missing_pickup_at"] == 1
    assert q["rejected_invalid_pickup_zone_id"] == 1
    assert q["rows_out_of_range"] == 2
    assert q["rows_counted"] == 5
    assert q["covered_windows"] == 8 * 96
    assert q["coverage_is_full_history"] is True
    assert first is not None


def test_batch_partial_coverage_and_zero_trip_window(spark, tmp_path):
    # Thứ Hai 2 không có chuyến nào của A trong 08:00–08:15 nhưng vẫn được xác nhận độ phủ.
    rows = [("2025-03-17T08:05:00.000", "161"), ("2025-03-24T09:00:00.000", "161")]
    spark.createDataFrame(rows, "tpep_pickup_datetime string, pulocationid string").write.parquet(
        str(tmp_path / "history")
    )
    _write_manifest(tmp_path / "manifest.json", tmp_path / "history")
    entries = HB.load_manifest(spark, str(tmp_path / "manifest.json"))
    coverage = [
        HB.parse_time_range("2025-03-17T08:00:00-04:00/2025-03-17T08:15:00-04:00"),
        HB.parse_time_range("2025-03-24T08:00:00-04:00/2025-03-24T08:15:00-04:00"),
    ]
    result = HB.compute_baseline(spark, entries, coverage=coverage, zones=[ZONE_A, ZONE_B])
    rows = _baseline_rows(result)
    assert rows == {(ZONE_A, 1, 32): (0.5, 2), (ZONE_B, 1, 32): (0.0, 2)}
    assert result.quality["rows_outside_coverage"] == 1  # chuyến 09:00 không thuộc cửa sổ đã xác nhận
    assert result.quality["coverage_is_full_history"] is False


def test_batch_export_csv(spark, tmp_path):
    _write_history(spark, tmp_path / "history")
    _write_manifest(tmp_path / "manifest.json", tmp_path / "history")
    result = HB.compute_baseline(spark, HB.load_manifest(spark, str(tmp_path / "manifest.json")))
    HB.write_baseline(result.baseline, str(tmp_path / "out"), "csv")
    csv_files = list((tmp_path / "out").glob("part-*.csv"))
    assert len(csv_files) == 1
    lines = csv_files[0].read_text(encoding="utf-8").splitlines()
    assert lines[0] == ",".join(HB.BASELINE_COLUMNS)
    assert "161,1,32,2.0,2,2025-03-17T00:00:00-04:00,2025-03-25T00:00:00-04:00" in lines


def test_batch_reads_ingestion_contract(spark, tmp_path):
    """Định dạng của nhánh demo/ingestion: manifest version 1, Parquet có pickup_at UTC,
    pickup_zone_id int32, validation_error và confirmed_windows trong mục history."""
    rows = [
        ("h-1", 0, "2025-01-06T08:01:00.000", "161", "2025-01-06T13:01:00Z", 161, None),
        ("h-2", 0, "2025-01-13T08:01:00.000", "161", "2025-01-13T13:01:00Z", 161, None),
        ("h-3", 1, "2025-01-13T08:05:00.000", "161", "2025-01-13T13:05:00Z", 161, None),
        ("h-4", 2, "2025-01-13T08:14:00.000", "161", "2025-01-13T13:14:00Z", 161, None),
        # Ingestion đã đánh dấu lỗi: phải bị loại dù các cột chuẩn trông hợp lệ.
        ("h-5", 3, "2025-01-13T08:06:00.000", "161", "2025-01-13T13:06:00Z", 161, "invalid_pickup_zone"),
        # Ngoài cửa sổ đã xác nhận: không được tính.
        ("h-6", 4, "2025-01-08T09:00:00.000", "161", "2025-01-08T14:00:00Z", 161, None),
    ]
    (
        spark.createDataFrame(
            rows,
            "source_record_id string, source_row long, tpep_pickup_datetime string, pulocationid string, "
            "p string, pickup_zone_id int, validation_error string",
        )
        .withColumn("pickup_at", F.to_timestamp("p"))
        .drop("p")
        .coalesce(1)
        .write.parquet(str(tmp_path / "history"))
    )
    parquet_file = next((tmp_path / "history").glob("part-*.parquet"))
    manifest = {
        "version": 1,
        "datasets": [
            {
                "dataset_role": "history",
                "source_id": "irx2-vgm4",
                "range_start": "2025-01-06T13:00:00+00:00",
                "range_end": "2025-01-13T13:15:00+00:00",
                "parquet_path": str(parquet_file),
                "status": "ready",
                "coverage_complete": True,
                "parquet_sha256": "0" * 64,
                "confirmed_windows": [
                    {"window_start": "2025-01-06T08:00:00-05:00", "window_end": "2025-01-06T08:15:00-05:00"},
                    {"window_start": "2025-01-13T08:00:00-05:00", "window_end": "2025-01-13T08:15:00-05:00"},
                ],
            }
        ],
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    result = HB.compute_baseline(spark, HB.load_manifest(spark, str(tmp_path / "manifest.json")))

    assert _baseline_rows(result) == {(ZONE_A, 1, 32): (2.0, 2)}
    q = result.quality
    assert q["coverage_source"] == "manifest_confirmed_windows"
    assert q["covered_windows"] == 2
    assert q["rejected_source_validation_error"] == 1
    assert q["rows_outside_coverage"] == 1

    not_complete = dict(manifest["datasets"][0], coverage_complete=False)
    with pytest.raises(ValueError, match="coverage_complete"):
        HB.select_history_entries([HB.to_entry(not_complete)])


def test_manifest_contract_errors(spark, tmp_path):
    with pytest.raises(ValueError, match="thiếu múi giờ"):
        HB.to_entry({"dataset_role": "history", "status": "ready", "range_start": "2025-03-17T00:00:00",
                     "range_end": "2025-03-18T00:00:00-04:00"})
    with pytest.raises(ValueError, match="không có mục history"):
        HB.select_history_entries([HB.to_entry({"dataset_role": "replay", "status": "ready"})])
    a = HB.to_entry({"dataset_role": "history", "status": "ready", "parquet_path": "a",
                     "range_start": "2025-03-17T00:00:00-04:00", "range_end": "2025-03-19T00:00:00-04:00"})
    b = HB.to_entry({"dataset_role": "history", "status": "ready", "parquet_path": "b",
                     "range_start": "2025-03-18T00:00:00-04:00", "range_end": "2025-03-20T00:00:00-04:00"})
    with pytest.raises(ValueError, match="chồng"):
        HB.select_history_entries([a, b])
    with pytest.raises(ValueError, match="nằm ngoài"):
        HB.covered_window_starts([HB.parse_time_range("2025-03-16T08:00:00-04:00/2025-03-16T08:15:00-04:00")],
                                 [a.time_range])
    jsonl = "\n".join(json.dumps({"dataset_role": "history", "status": "ready"}) for _ in range(2))
    assert len(HB.parse_manifest_text(jsonl)) == 2
    assert len(HB.parse_manifest_text(json.dumps({"entries": [{"dataset_role": "x"}]}))) == 1


# ---------------------------------------------------------------------------
# 2.3 Spark Structured Streaming
# ---------------------------------------------------------------------------
def _event(event_id, pickup_at, zone=ZONE_A, replay_id=REPLAY_ID):
    return json.dumps(
        {
            "replay_id": replay_id,
            "event_id": event_id,
            "pickup_at": pickup_at,
            "pickup_zone_id": zone,
            "source_file": "hdfs://namenode/taxi/parquet/replay/part-00000.parquet",
        }
    )


def _write_batch(directory, name, lines):
    directory.mkdir(parents=True, exist_ok=True)
    tmp = directory / f".{name}.tmp"
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.replace(tmp, directory / name)


FIXTURE_BATCHES = [
    [_event("replay#0", "2025-03-31T08:01:00-04:00"), _event("replay#1", "2025-03-31T08:05:00-04:00")],
    [
        _event("replay#2", "2025-03-31T08:14:00-04:00"),
        _event("replay#1", "2025-03-31T08:05:00-04:00"),  # gửi trùng
        "{not json",
        json.dumps({"replay_id": REPLAY_ID, "pickup_at": "2025-03-31T08:03:00-04:00", "pickup_zone_id": 161}),
        _event("replay#9", "2025-03-31T08:06:00-04:00", zone=0),
        _event("other#0", "2025-03-31T08:02:00-04:00", replay_id="another-session"),
    ],
    [_event("replay#3", "2025-03-31T12:15:00Z")],  # 08:15 EDT, gửi dạng UTC
]


def _run_stream(spark, input_dir, checkpoint):
    updates = []

    def sink(rows, batch_id):
        updates.append((batch_id, rows))

    raw = SC.read_jsonl_events(spark, str(input_dir))
    counts = SC.build_replay_counts(raw, REPLAY_ID)
    query = SC.start_replay_counts(counts, sink, str(checkpoint), available_now=True)
    summaries = SC.wait_and_log(query, poll_seconds=1)
    return updates, summaries


def _latest(updates):
    state = {}
    for _, rows in updates:
        for r in rows:
            state[(r["replay_id"], r[P.PICKUP_ZONE_ID], r[P.WINDOW_START], r[P.WINDOW_END])] = r["trip_count"]
    return state


def test_stream_fixture_counts_with_duplicate_and_bad_events(spark, tmp_path):
    for i, batch in enumerate(FIXTURE_BATCHES):
        _write_batch(tmp_path / "events", f"batch-{i}.jsonl", batch)
    updates, summaries = _run_stream(spark, tmp_path / "events", tmp_path / "checkpoint")

    assert _latest(updates) == {
        (REPLAY_ID, ZONE_A, "2025-03-31T08:00:00-04:00", "2025-03-31T08:15:00-04:00"): 3,
        (REPLAY_ID, ZONE_A, "2025-03-31T08:15:00-04:00", "2025-03-31T08:30:00-04:00"): 1,
    }
    # Lô đầu thấy 2 chuyến, lô sau gửi tổng mới là 3 (không phải +1).
    first_window = [
        r["trip_count"] for _, rows in updates for r in rows if r[P.WINDOW_START] == "2025-03-31T08:00:00-04:00"
    ]
    assert first_window == [2, 3]

    totals = SC.total_quality(summaries)
    assert totals["rows_received"] == 9
    assert totals["rows_valid"] == 5  # gồm cả lần gửi trùng, bị khử ở bước sau
    assert totals["duplicates_dropped"] == 1
    assert totals["rows_other_replay"] == 1
    assert totals["rejected_malformed_event"] == 1
    assert totals["rejected_missing_event_id"] == 1
    assert totals["rejected_invalid_pickup_zone_id"] == 1


def test_stream_restart_reuses_checkpoint(spark, tmp_path):
    events = tmp_path / "events"
    for i, batch in enumerate(FIXTURE_BATCHES):
        _write_batch(events, f"batch-{i}.jsonl", batch)
    _run_stream(spark, events, tmp_path / "checkpoint")

    # Khởi động lại cùng checkpoint: một sự kiện cũ bị gửi lại, một sự kiện mới đến.
    _write_batch(
        events,
        "batch-3.jsonl",
        [_event("replay#0", "2025-03-31T08:01:00-04:00"), _event("replay#4", "2025-03-31T08:10:00-04:00")],
    )
    updates, summaries = _run_stream(spark, events, tmp_path / "checkpoint")
    assert _latest(updates) == {
        (REPLAY_ID, ZONE_A, "2025-03-31T08:00:00-04:00", "2025-03-31T08:15:00-04:00"): 4,
    }
    assert SC.total_quality(summaries)["duplicates_dropped"] == 1


def test_progress_summary_ignores_no_data_batches():
    op = {"operatorName": "dedupe", "numRowsDroppedByWatermark": 5, "customMetrics": {"numDroppedDuplicateRows": 7}}
    with_data = SC.summarize_progress({"batchId": 1, "numInputRows": 12, "stateOperators": [op]})
    no_data = SC.summarize_progress({"batchId": 2, "numInputRows": 0, "stateOperators": [op]})
    assert (with_data["duplicates_dropped"], with_data["late_rows_dropped"]) == (7, 5)
    assert (no_data["duplicates_dropped"], no_data["late_rows_dropped"]) == (0, 0)


def test_batch_and_stream_share_window_rules(spark):
    """Cùng một chuyến cho cùng window_start/slot dù đi qua Parquet hay JSON Kafka."""
    batch_df = P.valid_pickups(_prepare(spark, [("2025-03-31T08:15:00.000", "161")]))
    stream_df = SC.build_replay_counts(
        spark.createDataFrame([Row(value=_event("e", "2025-03-31T08:15:00-04:00"))]), REPLAY_ID
    )
    b = batch_df.select(P.format_iso(F.col(P.WINDOW_START))).first()[0]
    s = stream_df.select(P.format_iso(F.col(P.WINDOW_START))).first()[0]
    assert b == s == "2025-03-31T08:15:00-04:00"
