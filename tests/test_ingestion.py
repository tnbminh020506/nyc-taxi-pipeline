import copy
import json
import threading
from collections import Counter
from datetime import timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from nyc_taxi_pipeline.ingestion.common import load_config, source_time
from nyc_taxi_pipeline.ingestion.ingest import run_ingestion
from nyc_taxi_pipeline.ingestion.ingest import query_for_range
from nyc_taxi_pipeline.ingestion.manifest import read_manifest
from nyc_taxi_pipeline.ingestion.replay import iter_events, run_replay, send_events

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def server():
    state = {"requests": [], "responses": []}

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            state["requests"].append({"body": body, "headers": dict(self.headers), "path": self.path})
            status, payload = state["responses"].pop(0)
            content = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self):
            state["requests"].append({"path": self.path, "headers": dict(self.headers)})
            status, payload = state["responses"].pop(0)
            content = json.dumps(payload).encode()
            self.send_response(status)
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    state["endpoint"] = f"http://127.0.0.1:{httpd.server_port}/query.json"
    try:
        yield state
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


def row(at="2025-01-20T08:01:00.000", zone="161"):
    return {"source_record_id": "api-row", "tpep_pickup_datetime": at, "pulocationid": zone}


def use_api(config, server, monkeypatch, size=2, max_pages=5):
    del config["datasets"]["replay"]["fixture_pages"]
    config["api"].update(endpoint=server["endpoint"], page_size=size, max_pages=max_pages)
    monkeypatch.setenv("SOCRATA_APP_TOKEN", "test-token")


def test_fixture_handoffs_and_expected_windows(config):
    results = run_ingestion(config)
    assert [entry["status"] for entry in results] == ["ready"] * 3
    assert [entry["row_count"] for entry in results] == [4, 4, 1]
    history = pq.read_table(results[0]["parquet_path"])
    assert history.schema.field("pickup_at").type.tz == "UTC"
    assert Counter(at.date().isoformat() for at in history["pickup_at"].to_pylist()) == {
        "2025-01-06": 1, "2025-01-13": 3,
    }
    events = list(iter_events(config, "fixture-session"))
    assert len(events) == 4
    assert [event["pickup_at"][11:16] for event in events] == ["13:01", "13:05", "13:14", "13:15"]
    assert all(set(event) == {"replay_id", "event_id", "pickup_at", "pickup_zone_id", "source_file"}
               for event in events)
    assert Counter(int(event["pickup_at"][14:16]) // 15 for event in events) == {0: 3, 1: 1}


def test_rerun_upserts_and_preserves_ids(config):
    run_ingestion(config)
    first = list(iter_events(config, "same-session"))
    first_path = first[0]["source_file"]
    run_ingestion(config)
    second = list(iter_events(config, "same-session"))
    assert len(read_manifest(config["_root"] / config["paths"]["manifest"])["datasets"]) == 3
    assert [event["event_id"] for event in first] == [event["event_id"] for event in second]
    assert second[0]["source_file"] != first_path
    assert Path(first_path).is_file()  # immutable files remain valid for existing readers


def test_v3_pagination_and_raw_provenance(config, server, monkeypatch):
    use_api(config, server, monkeypatch)
    server["responses"] = [(200, [row(), row("2025-01-20T08:05:00")]),
                           (200, [row("2025-01-20T08:15:00")])]
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "ready" and result["row_count"] == 3 and result["page_count"] == 2
    requests = server["requests"]
    assert [request["body"]["page"] for request in requests] == [
        {"pageNumber": 1, "pageSize": 2}, {"pageNumber": 2, "pageSize": 2}]
    assert requests[0]["headers"]["X-App-Token"] == "test-token"
    query = requests[0]["body"]["query"]
    assert ">= '2025-01-20T08:00:00'" in query and "< '2025-01-20T08:30:00'" in query
    assert "ORDER BY tpep_pickup_datetime ASC, :id ASC" in query
    metadata = json.loads((Path(result["raw_path"]) / "request.json").read_text())
    assert metadata["query"] == query
    assert "test-token" not in json.dumps(metadata)
    assert len(list(Path(result["raw_path"]).glob("page-*.json"))) == 2


def test_full_last_page_without_eof_is_incomplete(config, server, monkeypatch):
    use_api(config, server, monkeypatch, max_pages=1)
    server["responses"] = [(200, [row(), row()])]
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "incomplete" and not result["coverage_complete"]
    assert not Path(result["parquet_path"]).exists()
    with pytest.raises(ValueError, match="No ready"):
        list(iter_events(config, "session"))


def test_empty_final_page_confirms_exact_multiple(config, server, monkeypatch):
    use_api(config, server, monkeypatch)
    server["responses"] = [(200, [row(), row()]), (200, [])]
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "ready" and result["row_count"] == 2 and result["page_count"] == 2


def test_missing_page_invalidates_old_ready_entry(config, server, monkeypatch):
    run_ingestion(config, ("replay",))
    use_api(config, server, monkeypatch)
    server["responses"] = [(200, [row(), row()]), (403, {"message": "denied"})]
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "failed" and result["row_count"] == 2
    entries = read_manifest(config["_root"] / config["paths"]["manifest"])["datasets"]
    assert len(entries) == 1 and entries[0]["status"] == "failed"


def test_rate_limit_is_retried(config, server, monkeypatch):
    use_api(config, server, monkeypatch)
    server["responses"] = [(429, {"message": "try again"}), (200, [row()])]
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "ready" and len(server["requests"]) == 2


def test_missing_app_token_fails_without_http(config, server, monkeypatch):
    use_api(config, server, monkeypatch)
    monkeypatch.delenv("SOCRATA_APP_TOKEN")
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "failed" and "SOCRATA_APP_TOKEN" in result["error"]
    assert not server["requests"]


def test_v2_explicit_configuration(config, server, monkeypatch):
    use_api(config, server, monkeypatch)
    config["api"]["version"] = 2
    monkeypatch.delenv("SOCRATA_APP_TOKEN")
    server["responses"] = [(200, [row()])]
    assert run_ingestion(config, ("replay",))[0]["status"] == "ready"
    from urllib.parse import parse_qs, urlsplit
    query = parse_qs(urlsplit(server["requests"][0]["path"]).query)["$query"][0]
    assert query.endswith("LIMIT 2 OFFSET 0")


def test_empty_dataset_has_readable_parquet(config, server, monkeypatch):
    use_api(config, server, monkeypatch)
    server["responses"] = [(200, [])]
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "ready"
    assert pq.read_table(result["parquet_path"]).num_rows == 0
    assert list(iter_events(config, "empty")) == []


def test_invalid_records_preserved_but_not_replayed(config, tmp_path):
    path = tmp_path / "bad-page.json"
    path.write_text(json.dumps([row(), row(zone="bad"), row(at=None), row(zone="0")]))
    config["datasets"]["replay"]["fixture_pages"] = [str(path)]
    result, = run_ingestion(config, ("replay",))
    assert result["row_count"] == 4 and result["invalid_row_count"] == 3
    assert pq.read_table(result["parquet_path"]).num_rows == 4
    stats = {}
    assert len(list(iter_events(config, "session", stats))) == 1
    assert stats["invalid_rows"] == 3


def test_duplicates_remain_distinct_and_sorted(config, tmp_path):
    path = tmp_path / "duplicates.json"
    path.write_text(json.dumps([row("2025-01-20T08:14:00"), row(), row()]))
    config["datasets"]["replay"]["fixture_pages"] = [str(path)]
    run_ingestion(config, ("replay",))
    events = list(iter_events(config, "session"))
    assert [event["pickup_at"][11:16] for event in events] == ["13:01", "13:01", "13:14"]
    assert len({event["event_id"] for event in events}) == 3
    assert list(iter_events(config, "session")) == events


def test_upper_bound_is_exclusive(config, tmp_path):
    path = tmp_path / "outside.json"
    path.write_text(json.dumps([row("2025-01-20T08:30:00")]))
    config["datasets"]["replay"]["fixture_pages"] = [str(path)]
    result, = run_ingestion(config, ("replay",))
    assert result["status"] == "failed" and "outside" in result["error"]


def test_modified_parquet_cannot_be_replayed(config):
    result, = run_ingestion(config, ("replay",))
    with Path(result["parquet_path"]).open("ab") as handle:
        handle.write(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        list(iter_events(config, "session"))


@pytest.mark.parametrize("at", ["2025-03-09T02:30:00", "2025-11-02T01:30:00"])
def test_dst_ambiguity_requires_explicit_offset(at):
    with pytest.raises(ValueError, match="Ambiguous or nonexistent"):
        source_time(at, "America/New_York")
    assert source_time("2025-11-02T01:30:00-04:00", "America/New_York").tzinfo == timezone.utc


@pytest.mark.parametrize("mutation, message", [
    (lambda c: c["datasets"].update(replay=copy.deepcopy(c["datasets"]["history"])), "overlap"),
    (lambda c: c["datasets"]["history"].update(range_start="2025-01-01T00:00:00"), "offset"),
    (lambda c: c["api"].update(page_size=0), "positive integer"),
    (lambda c: c["replay"].update(events_per_second=-1), "nonnegative"),
])
def test_invalid_config_is_rejected(tmp_path, mutation, message):
    config = yaml.safe_load((ROOT / "configs/fixture.yaml").read_text(encoding="utf-8"))
    mutation(config)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    with pytest.raises(ValueError, match=message):
        load_config(path)


def test_bad_lookup_does_not_become_ready(config, tmp_path):
    lookup = tmp_path / "lookup.csv"
    lookup.write_text("LocationID,Zone,Borough\n161,Midtown,Manhattan\n161,Again,Manhattan\n")
    config["zone_lookup"]["source"] = str(lookup)
    result, = run_ingestion(config, ("zone_lookup",))
    assert result["status"] == "failed"


def test_dry_run_writes_jsonl(config):
    run_ingestion(config)
    stats = run_replay(config, dry_run=True, output="output/events.jsonl")
    lines = (config["_root"] / "output/events.jsonl").read_text().splitlines()
    assert len(lines) == 4 and stats["mode"] == "dry-run"
    assert json.loads(lines[0])["replay_id"] == "taxi-fixture-001"


def test_kafka_send_requires_ack_and_preserves_duplicate_identity(config):
    run_ingestion(config, ("replay",))
    events = list(iter_events(config, "session"))
    messages = []

    class Producer:
        def send(self, topic, key, value):
            messages.append((topic, key, json.loads(value)))
            return self

        def get(self, timeout):
            assert timeout == 30

        def flush(self, timeout):
            pass

    stats = send_events(events + [events[1]], Producer(), "taxi-events", 0)
    assert stats["events_sent"] == 5
    assert messages[-1][2]["event_id"] == messages[1][2]["event_id"]
    assert len({message[2]["event_id"] for message in messages}) == 4


def test_kafka_send_failure_propagates():
    class FailedProducer:
        def send(self, *args, **kwargs):
            return self

        def get(self, timeout):
            raise TimeoutError("broker did not acknowledge")

    event = {"replay_id": "s", "pickup_zone_id": 161}
    stats = {}
    with pytest.raises(TimeoutError):
        send_events([event], FailedProducer(), "taxi-events", 0, stats)
    assert stats["events_sent"] == 0


@pytest.mark.parametrize("start,end", [
    ("2025-03-08T00:00:00-05:00", "2025-03-10T00:00:00-04:00"),
    ("2025-01-01T00:00:00-05:00", "2025-12-01T00:00:00-05:00"),
])
def test_api_ranges_crossing_dst_are_rejected(start, end):
    with pytest.raises(ValueError, match="DST"):
        query_for_range({"range_start": start, "range_end": end}, "America/New_York")


def test_cli_from_another_working_directory(config, tmp_path):
    import subprocess
    import sys

    config = dict(config)
    config.pop("_root")
    config["workspace_root"] = str(tmp_path)
    config_path = tmp_path / "demo.yaml"
    config_path.write_text(yaml.safe_dump(config))
    ingestion = subprocess.run([sys.executable, str(ROOT / "scripts/run_ingestion.py"),
                                "--config", str(config_path)], cwd=tmp_path, capture_output=True, text=True)
    assert ingestion.returncode == 0, ingestion.stderr
    assert len(json.loads(ingestion.stdout)) == 3
    replay = subprocess.run([sys.executable, str(ROOT / "scripts/replay_taxi_stream.py"),
                             "--config", str(config_path), "--dry-run"],
                            cwd=tmp_path, capture_output=True, text=True)
    assert replay.returncode == 0, replay.stderr
    assert len(replay.stdout.splitlines()) == 4
    config["datasets"]["replay"]["fixture_pages"] = [str(tmp_path / "missing.json")]
    config_path.write_text(yaml.safe_dump(config))
    failed = subprocess.run([sys.executable, str(ROOT / "scripts/run_ingestion.py"),
                             "--config", str(config_path), "--role", "replay"],
                            cwd=tmp_path, capture_output=True, text=True)
    assert failed.returncode == 1 and json.loads(failed.stdout)[0]["status"] == "failed"
