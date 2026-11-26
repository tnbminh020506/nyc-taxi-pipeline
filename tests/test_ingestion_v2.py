"""Source, WebHDFS, manifest and replay contracts exercised over HTTP."""

import json
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlencode, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from shapely.geometry import Point

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.ingestion.coordinates import load_boundaries
from nyc_taxi_pipeline.ingestion.hdfs_store import WebHDFS
from nyc_taxi_pipeline.ingestion.kafka_publisher import event_key
from nyc_taxi_pipeline.ingestion.manifest import publish_datasets
from nyc_taxi_pipeline.ingestion.replay import iter_requests


def _zone(identifier, lon):
    shell = [[lon, 40], [lon + 0.1, 40], [lon + 0.1, 40.1], [lon, 40.1], [lon, 40]]
    return {"type": "Feature", "properties": {"LocationID": identifier},
            "geometry": {"type": "Polygon", "coordinates": [shell]}}


def _trip(identifier, pickup, dropoff="2025-01-20T08:10:00", destination="2"):
    return {"source_record_id": identifier, "pickup_at": pickup, "dropoff_at": dropoff,
            "pickup_zone_id": "1", "dropoff_zone_id": destination}


class TestServer(ThreadingHTTPServer):
    __test__ = False

    def __init__(self):
        super().__init__(("127.0.0.1", 0), Handler)
        self.origin = f"http://127.0.0.1:{self.server_port}"
        self.files, self.committed = {}, []
        self.replicas = 2
        self.page_fault = None
        self.rows = {
            "history": [_trip("history-1", "2025-01-06T08:00:00", "2025-01-06T08:05:00")],
            "replay": [_trip("request-1", "2025-01-20T08:00:00"),
                       _trip("request-2", "2025-01-20T08:00:00"),
                       _trip("request-3", "2025-01-20T08:02:00", destination=None),
                       _trip("request-4", "2025-01-20T08:04:00")],
        }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def _send(self, status, payload=None, redirect=None):
        self.send_response(status)
        if redirect:
            self.send_header("Location", redirect)
        if isinstance(payload, bytes):
            data = payload
            self.send_header("Content-Type", "application/octet-stream")
        else:
            data = json.dumps(payload).encode()
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _query(self, query, limit, offset):
        rows = self.server.rows["history" if "2025-01-06" in query else "replay"]
        if "count(*)" in query:
            self._send(200, [{"row_count": str(len(rows))}])
            return
        assert "ORDER BY tpep_pickup_datetime ASC, :id ASC" in query
        page = rows[offset:offset + limit]
        if len(rows) > 1 and offset > 0 and self.server.page_fault:
            page = page[:-1] if self.server.page_fault == "short" else [rows[0], *page[1:]]
        self._send(200, page)

    def _parts(self):
        url = urlsplit(self.path)
        return url.path, {key: values[0] for key, values in parse_qs(url.query).items()}

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        payload = json.loads(self.rfile.read(length))
        page = payload["page"]
        size = page["pageSize"]
        self._query(payload["query"], size, (page["pageNumber"] - 1) * size)

    def do_GET(self):
        path, args = self._parts()
        if path == "/api":
            query, limit, offset = re.fullmatch(r"(.+) LIMIT (\d+) OFFSET (\d+)", args["$query"]).groups()
            self._query(query, int(limit), int(offset))
        elif path == "/data":
            self._send(200, self.server.files[args["path"]])
        elif path.startswith("/webhdfs/v1/"):
            name = path[len("/webhdfs/v1"):]
            value = self.server.files.get(name)
            if value is None:
                self._send(404, {"message": "File missing"})
            elif args["op"] == "OPEN":
                self._send(307, redirect=f"{self.server.origin}/data?{urlencode({'path': name})}")
            elif args["op"] == "GETFILESTATUS":
                self._send(200, {"FileStatus": {"length": len(value), "replication": 2}})
            elif args["op"] == "GETFILEBLOCKLOCATIONS":
                self._send(200, {"BlockLocations": {"BlockLocation": [
                    {"corrupt": False, "names": [f"replica-{i}" for i in range(self.server.replicas)]},
                ]}})

    def do_PUT(self):
        path, args = self._parts()
        if path == "/data":
            self.server.files[args["path"]] = self.rfile.read(int(self.headers["Content-Length"]))
            self._send(201)
            return
        name = path[len("/webhdfs/v1"):]
        if args["op"] == "MKDIRS":
            self._send(200, {"boolean": True})
        elif args["op"] == "CREATE":
            self._send(307, redirect=f"{self.server.origin}/data?{urlencode({'path': name})}")
        elif args["op"] == "RENAME":
            destination = args["destination"]
            if destination in self.server.files:
                self._send(200, {"boolean": False})
                return
            self.server.files[destination] = self.server.files.pop(name)
            self.server.committed.append(destination)
            self._send(200, {"boolean": True})

    def do_DELETE(self):
        path, args = self._parts()
        assert args["recursive"] == "false"
        self.server.files.pop(path[len("/webhdfs/v1"):], None)
        self._send(200, {"boolean": True})


@contextmanager
def source_server():
    with TestServer() as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join()


def ingestion_config(tmp_path, server):
    lookup, geo = tmp_path / "lookup.csv", tmp_path / "zones.geojson"
    lookup.write_text("LocationID,Zone,Borough\n1,A,NYC\n2,B,NYC\n", encoding="utf-8")
    geo.write_text(json.dumps({"type": "FeatureCollection", "features": [_zone(1, -74.1), _zone(2, -73.9)]}),
                   encoding="utf-8")
    return {
        "_root": tmp_path, "workspace_root": ".", "business_timezone": "America/New_York",
        "api": {"endpoint": server.origin + "/api", "dataset_id": "fixture", "source_version": "1", "page_size": 2},
        "datasets": {
            "history": {"range_start": "2025-01-06T08:00:00-05:00", "range_end": "2025-01-06T10:00:00-05:00"},
            "replay": {"range_start": "2025-01-20T08:00:00-05:00", "range_end": "2025-01-20T10:00:00-05:00"},
        },
        "storage": {"fs_uri": "hdfs://namenode:8020", "webhdfs_url": server.origin,
                    "root": "/demo", "replication_wait_seconds": 0},
        "zones": {
            "zone_lookup": {"source": str(lookup), "source_id": "lookup", "source_version": "1"},
            "zone_boundaries": {"source": str(geo), "source_id": "zones", "source_version": "1"},
        },
    }


def test_api_to_hdfs_to_replay_through_cli(tmp_path):
    with source_server() as server:
        config = ingestion_config(tmp_path, server)
        config_file = tmp_path / "ingestion.yaml"
        config_file.write_text(yaml.safe_dump({key: value for key, value in config.items() if key != "_root"}),
                               encoding="utf-8")
        status = tmp_path / "status.json"
        subprocess.run([sys.executable, "scripts/run_ingestion.py", "--config", str(config_file),
                        "--status-file", str(status)], check=True, capture_output=True)
        result = json.loads(status.read_bytes())
        datasets = {entry["dataset_role"]: entry for entry in result["datasets"]}
        assert result["version"] == 2 and datasets["history"]["row_count"] == 1
        assert datasets["replay"]["row_count"] == 4
        assert server.committed[-1] == urlsplit(result["manifest_uri"]).path
        count = len(server.committed)
        assert publish_datasets(config)["manifest_uri"] == result["manifest_uri"]
        assert len(server.committed) == count
        output = tmp_path / "events.jsonl"
        subprocess.run([sys.executable, "scripts/replay_taxi_stream.py", "--config", str(config_file),
                        "--manifest", result["manifest_uri"], "--scope-id", "s", "--replay-id", "r",
                        "--dry-run", "--output", str(output)], check=True, capture_output=True)
        events = [json.loads(row) for row in output.read_bytes().splitlines()]
        with WebHDFS(config) as store:
            assert events == list(iter_requests(store, result["manifest_uri"], "r", "s"))
        assert len({event["event_id"] for event in events}) == 4
        assert events[0]["source_file"].startswith("hdfs://") and events[0]["source_duration_seconds"] == 600
        polygons = load_boundaries(Path(config["zones"]["zone_boundaries"]["source"]))
        for event in events:
            assert event_key(event) and event["event_type"] == "ride_requested"
            assert polygons[1].contains(Point(event["pickup_lon"], event["pickup_lat"]))
        assert events[2]["dropoff_zone_id"] is None and "missing_dropoff_coordinates" in events[2]["matching_error"]


@pytest.mark.parametrize(("fault", "message"), [("short", "incomplete"), ("duplicate", "Duplicate source ID")])
def test_pages_fail_without_publishing_ready(tmp_path, fault, message):
    with source_server() as server:
        server.page_fault = fault
        status = tmp_path / "failed.json"
        with pytest.raises(ValueError, match=message):
            publish_datasets(ingestion_config(tmp_path, server), ["replay"], status_path=status)
        assert json.loads(status.read_bytes())["status"] == "failed"
        assert not any(path.startswith("/demo/manifests/") for path in server.committed)


def test_replication_failure_does_not_publish_or_leave_temporary_file(tmp_path):
    with source_server() as server:
        server.replicas = 1
        with pytest.raises(ValueError, match="not replicated"):
            publish_datasets(ingestion_config(tmp_path, server), ["zone_lookup"])
        assert not server.committed and not server.files


def test_soda_v3_and_token_from_environment(tmp_path, monkeypatch):
    with source_server() as server:
        config = ingestion_config(tmp_path, server)
        config["api"].update(version=3, app_token_env="FIXTURE_SODA_TOKEN")
        monkeypatch.setenv("FIXTURE_SODA_TOKEN", "test-only")
        assert publish_datasets(config, ["history"])["datasets"][0]["row_count"] == 1


def test_tlc_uses_original_file_and_normalizes_pickup_and_dropoff(tmp_path):
    with source_server() as server:
        config = ingestion_config(tmp_path, server)
        original = tmp_path / "2025-01.parquet"
        cached = os.environ.get("TEST_INGESTION_TLC_JANUARY")
        if cached:
            original = Path(cached).resolve()
        else:
            pq.write_table(pa.table({
                "tpep_pickup_datetime": [datetime(2025, 1, 6, 8), datetime(2025, 1, 20, 8)],
                "tpep_dropoff_datetime": [datetime(2025, 1, 6, 8, 3), datetime(2025, 1, 20, 8, 16)],
                "PULocationID": [1, 1], "DOLocationID": [2, 2],
            }), original)
        config["source"] = {"kind": "tlc_parquet", "source_id": "tlc", "source_version": "01",
                            "files": {"2025-01": str(original)}}
        result = publish_datasets(config)
        entries = {entry["dataset_role"]: entry for entry in result["datasets"]}
        assert (entries["history"]["row_count"], entries["replay"]["row_count"]) == ((8461, 5651) if cached else (1, 1))
        with WebHDFS(config) as store:
            rows = list(iter_requests(store, result["manifest_uri"], "r", "s"))
        assert len(rows) == (5651 if cached else 1)
        assert rows[0]["source_file"].endswith(original.name)
        assert rows[0]["dropoff_zone_id"] is not None
        if not cached:
            assert rows[0]["source_duration_seconds"] == 960
