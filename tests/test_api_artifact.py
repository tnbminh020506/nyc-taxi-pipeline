import json
from pathlib import Path

import pytest

from nyc_taxi_pipeline.ingestion.artifact import import_artifact
from nyc_taxi_pipeline.ingestion.common import load_config, write_json
from nyc_taxi_pipeline.ingestion.ingest import run_ingestion
from nyc_taxi_pipeline.ingestion.manifest import read_manifest
from nyc_taxi_pipeline.ingestion.replay import iter_events


def package(config):
    run_ingestion(config)
    manifest = config["_root"] / config["paths"]["manifest"]
    document = read_manifest(manifest)
    for entry in document["datasets"]:
        if entry["dataset_role"] != "zone_lookup":
            entry["input_mode"] = "api"  # simulate the downloaded artifact format, not a live test
        for key in ("raw_path", "parquet_path"):
            if entry[key]:
                relative = Path(entry[key]).relative_to(config["_root"])
                entry[key] = "/home/runner/project/" + relative.as_posix()
    write_json(manifest, document)
    write_json(config["_root"] / "output/api-smoke/report.json", {
        "status": "passed", "dataset_id": config["api"]["dataset_id"],
        "api_version": 3, "endpoint": config["api"]["endpoint"]})
    return manifest, document


def test_import_rebases_paths_without_network_or_changing_event_ids(config, monkeypatch):
    run_ingestion(config)
    expected = [event["event_id"] for event in iter_events(config, "session")]
    manifest, _ = package(config)
    original = manifest.read_bytes()
    def no_network(*args, **kwargs):
        raise AssertionError("Artifact import must be offline")
    monkeypatch.setattr("requests.Session.request", no_network)
    imported = import_artifact(manifest)
    local = load_config(imported["config"])
    actual = [event["event_id"] for event in iter_events(local, "session")]
    assert actual == expected and imported["rows"] == {"history": 4, "replay": 4, "zone_lookup": 1}
    assert manifest.read_bytes() == original


def test_import_rejects_changed_parquet_before_publishing(config):
    manifest, document = package(config)
    relative = document["datasets"][0]["parquet_path"].split("/project/")[1]
    with (config["_root"] / relative).open("ab") as handle:
        handle.write(b"changed")
    with pytest.raises(ValueError, match="checksum"):
        import_artifact(manifest)
    assert not (manifest.parent / "manifest.local.json").exists()
