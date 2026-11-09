"""Verify a successful Actions data artifact and create a portable local manifest."""

import copy
import csv
import json
import re
import uuid
from pathlib import Path, PurePosixPath

import pyarrow.parquet as pq
import yaml

from .common import atomic_write, write_json
from .ingest import file_sha256
from .manifest import read_manifest


def import_artifact(path):
    path = Path(path).resolve()
    if path.is_dir():
        manifests = [item for item in path.rglob("*manifest.json") if item.parent.name == "data"]
        if len(manifests) != 1:
            raise ValueError("Select one artifact/run containing exactly one source manifest")
        source = manifests[0]
    else:
        source = path
    root = source.parent.parent.resolve()
    reports = [item for item in (root / "output/api-smoke/report.json", root / "report.json") if item.is_file()]
    if len(reports) != 1:
        raise ValueError("Missing or ambiguous API acceptance report")
    report = json.loads(reports[0].read_text(encoding="utf-8"))
    if report.get("status") != "passed":
        raise ValueError("Only a successful API acceptance artifact can be imported")
    document = copy.deepcopy(read_manifest(source))
    entries = document["datasets"]
    if sorted(entry["dataset_role"] for entry in entries) != ["history", "replay", "zone_lookup"]:
        raise ValueError("Select a single run with one history, replay and lookup entry")

    def local_path(original):
        parts = PurePosixPath(original.replace("\\", "/")).parts
        indices = [index for index, part in enumerate(parts) if part == "data"]
        if not indices:
            raise ValueError("Expected the standard artifact data/ layout")
        target = root.joinpath(*parts[indices[-1]:]).resolve()
        if not target.is_relative_to(root):
            raise ValueError("Artifact path escapes the selected package")
        return target

    for entry in entries:
        if entry["status"] != "ready":
            raise ValueError("All imported entries must be ready")
        raw = local_path(entry["raw_path"])
        entry["original_raw_path"] = entry["raw_path"]
        entry["raw_path"] = str(raw)
        if entry["dataset_role"] == "zone_lookup":
            if not raw.is_file() or file_sha256(raw) != entry["sha256"]:
                raise ValueError("Lookup checksum mismatch")
            with raw.open(encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                if not {"LocationID", "Zone", "Borough"}.issubset(reader.fieldnames or []):
                    raise ValueError("Lookup columns do not match the contract")
                if sum(1 for _ in reader) != entry["row_count"]:
                    raise ValueError("Lookup row count mismatch")
            continue
        if entry.get("input_mode") != "api" or not entry.get("coverage_complete"):
            raise ValueError("Expected complete API input, not a fixture or partial download")
        if entry["source_id"] != report["dataset_id"]:
            raise ValueError("Dataset differs from the acceptance report")
        parquet = local_path(entry["parquet_path"])
        if not parquet.is_file() or file_sha256(parquet) != entry["parquet_sha256"]:
            raise ValueError("Parquet checksum mismatch")
        if pq.ParquetFile(parquet).metadata.num_rows != entry["row_count"]:
            raise ValueError("Parquet row count mismatch")
        metadata = json.loads((raw / "request.json").read_text(encoding="utf-8"))
        raw_rows = 0
        for page in metadata["pages"]:
            if not re.fullmatch(r"page-[0-9]{6}\.json", page["file"]):
                raise ValueError("Invalid raw page filename")
            page_path = (raw / page["file"]).resolve()
            if not page_path.is_relative_to(root):
                raise ValueError("Raw page escapes the package")
            if page.get("sha256") and file_sha256(page_path) != page["sha256"]:
                raise ValueError("Raw page checksum mismatch")
            rows = json.loads(page_path.read_text(encoding="utf-8"))
            if not isinstance(rows, list) or len(rows) != page["rows"]:
                raise ValueError("Raw page row count mismatch")
            raw_rows += len(rows)
        if raw_rows != entry["row_count"]:
            raise ValueError("Raw/Parquet row count mismatch")
        entry["original_parquet_path"] = entry["parquet_path"]
        entry["parquet_path"] = str(parquet)
    local_manifest = source.parent / "manifest.local.json"
    write_json(local_manifest, document)
    by_role = {entry["dataset_role"]: entry for entry in entries}
    config = {
        "workspace_root": str(root), "source_timezone": by_role["history"]["source_timezone"],
        "api": {"version": report["api_version"], "dataset_id": report["dataset_id"],
                "endpoint": report["endpoint"]},
        "datasets": {role: {key: by_role[role][key] for key in ("range_start", "range_end")}
                     for role in ("history", "replay")},
        "zone_lookup": {"source_id": by_role["zone_lookup"]["source_id"], "source": by_role["zone_lookup"]["raw_path"]},
        "paths": {"raw": "data/raw", "parquet": "data/processed", "manifest": str(local_manifest)},
        "kafka": {"bootstrap_servers": "localhost:9092", "topic": "taxi-events", "partitions": 1},
        "replay": {"replay_id": f"taxi-import-{uuid.uuid4().hex[:12]}", "events_per_second": 0},
    }
    config_path = root / "imported.local.yaml"
    atomic_write(config_path, yaml.safe_dump(config, sort_keys=False).encode("utf-8"))
    return {"manifest": str(local_manifest), "config": str(config_path),
            "rows": {role: entry["row_count"] for role, entry in by_role.items()}}
