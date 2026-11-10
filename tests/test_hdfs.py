"""Offline failure/contract tests. A separate CI job uses an actual HDFS cluster."""

import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from nyc_taxi_pipeline.ingestion.hdfs import WebHDFS, hdfs_manifest, receipt_path
from nyc_taxi_pipeline.ingestion.ingest import run_ingestion
from nyc_taxi_pipeline.ingestion.replay import iter_events


@pytest.fixture
def hdfs_config(config):
    original = config["_root"] / "yellow_tripdata_2025-01.parquet"
    pq.write_table(pa.table({"tpep_pickup_datetime": [datetime(2025, 1, 6, 8, 1)] * 6
                             + [datetime(2025, 1, 20, 8, 1)] * 4,
                            "PULocationID": [161] * 10}), original)
    config["source"] = {"kind": "tlc_parquet", "source_id": "tlc-test", "files": {"2025-01": str(original)}}
    config["storage"] = {"kind": "hdfs", "fs_uri": "hdfs://namenode:8020",
                         "webhdfs_url": "http://127.0.0.1:9870", "replication": 2,
                         "root": "/nyc-test", "replication_wait_seconds": 0}
    config["datasets"] = {"history": {"range_start": "2025-01-06T08:00:00-05:00", "range_end": "2025-01-06T08:15:00-05:00"},
                          "replay": {"range_start": "2025-01-20T08:00:00-05:00", "range_end": "2025-01-20T08:15:00-05:00"}}
    return config


class FakeHDFS:
    """Only test publication ordering/failures here; this is not HDFS evidence."""
    files = None
    fail_replay = False
    replication = 2

    def __init__(self, config):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def uri(self, path):
        return path if path.startswith("hdfs://") else "hdfs://namenode:8020" + path

    def put(self, local, uri):
        if self.fail_replay and "/replay/" in uri:
            raise OSError("DataNode write failed")
        content = Path(local).read_bytes()
        uri = self.uri(uri)
        self.files[uri] = content
        return {"uri": uri, "sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content), "replication": 2}

    def sha256(self, uri):
        return hashlib.sha256(self.files[uri]).hexdigest()

    def json(self, uri):
        return json.loads(self.files[uri])

    def download(self, uri, target, checksum):
        if self.sha256(uri) != checksum:
            raise ValueError("HDFS download SHA-256 differs from manifest")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(self.files[uri])
        return target


@pytest.fixture
def fake_hdfs(monkeypatch):
    FakeHDFS.files = {}
    FakeHDFS.fail_replay = False
    monkeypatch.setattr("nyc_taxi_pipeline.ingestion.hdfs.WebHDFS", FakeHDFS)
    return FakeHDFS


def test_publication_keeps_originals_and_replay_reads_shared_manifest(hdfs_config, fake_hdfs):
    results = run_ingestion(hdfs_config)
    assert all(e["status"] == "ready" and e["hdfs_verified"] for e in results)
    receipt = json.loads(receipt_path(hdfs_config).read_text())
    assert receipt["status"] == "ready"
    raw_uri = receipt["original_sources"][0]["uri"]
    assert fake_hdfs.files[raw_uri] == Path(hdfs_config["source"]["files"]["2025-01"]).read_bytes()
    first = list(iter_events(hdfs_config, "hdfs-test"))
    assert len(first) == 4 and all(e["source_file"].startswith("hdfs://") for e in first)
    # Remove all staging outputs: HDFS, not the producer's local manifest/path,
    # supplies the source for this replay.
    local_manifest = hdfs_config["_root"] / hdfs_config["paths"]["manifest"]
    local_entries = json.loads(local_manifest.read_text())["datasets"]
    for entry in local_entries:
        if entry.get("parquet_path"):
            Path(entry["parquet_path"]).unlink()
    local_manifest.unlink()
    assert list(iter_events(hdfs_config, "hdfs-test")) == first
    # A new consumer can pin just the shared manifest URI, without a receipt.
    pinned = copy.deepcopy(hdfs_config)
    pinned["storage"]["manifest_uri"] = receipt["manifest_uri"]
    receipt_path(hdfs_config).unlink()
    assert list(iter_events(pinned, "hdfs-test")) == first


def test_failed_upload_has_no_ready_manifest_or_local_fallback(hdfs_config, fake_hdfs):
    fake_hdfs.fail_replay = True
    assert all(e["status"] == "failed" for e in run_ingestion(hdfs_config))
    assert not any("/manifests/" in path for path in fake_hdfs.files)
    with pytest.raises(ValueError, match="not ready"):
        list(iter_events(hdfs_config, "failed"))


def test_tampered_hdfs_manifest_is_rejected(hdfs_config, fake_hdfs):
    run_ingestion(hdfs_config)
    receipt = json.loads(receipt_path(hdfs_config).read_text())
    fake_hdfs.files[receipt["manifest_uri"]] += b" "
    with pytest.raises(ValueError, match="manifest checksum"):
        hdfs_manifest(hdfs_config)


def test_webhdfs_paths_redirect_mapping_and_insufficient_replicas(hdfs_config, monkeypatch):
    hdfs_config["storage"]["data_node_map"] = {"datanode-2:9865": "http://127.0.0.1:9865"}
    with WebHDFS(hdfs_config) as client:
        for path in ["hdfs://other:8020/x", "/a/../b", "/a/%2e%2e/b", "/file?op=DELETE"]:
            with pytest.raises(ValueError):
                client.path(path)
        response = requests.Response()
        response.status_code = 307
        response.headers["Location"] = "http://172.20.0.3:9865/webhdfs/v1/a?op=OPEN"
        assert client.data_url(response) == "http://127.0.0.1:9865/webhdfs/v1/a?op=OPEN"
        monkeypatch.setattr(client, "status", lambda _: {"replication": 2, "length": 10})
        monkeypatch.setattr(client, "blocks", lambda _: [{"names": ["dn1:9866"], "corrupt": False}])
        with pytest.raises(ValueError, match="not replicated"):
            client.wait_replicas("/a")


def test_replay_rejects_malformed_checksum_before_building_cache_path(hdfs_config, fake_hdfs):
    run_ingestion(hdfs_config)
    receipt = json.loads(receipt_path(hdfs_config).read_text())
    document = json.loads(fake_hdfs.files[receipt["manifest_uri"]])
    for entry in document["datasets"]:
        if entry["dataset_role"] == "replay":
            entry["parquet_sha256"] = "../../outside"
    fake_hdfs.files[receipt["manifest_uri"]] = json.dumps(document).encode()
    hdfs_config["storage"]["manifest_uri"] = receipt["manifest_uri"]
    with pytest.raises(ValueError, match="SHA-256 hex"):
        list(iter_events(hdfs_config, "invalid-checksum"))
