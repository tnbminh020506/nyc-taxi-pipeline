"""Publish complete source datasets as one immutable HDFS manifest."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import source_api, tlc
from .common import file_sha256, stable_id, write_json
from .config import dataset_ranges, hdfs_root
from .hdfs_store import WebHDFS, publish_json
from .trip_rows import TRIP_SCHEMA
from .zone_sources import stage_zone_source

LOG = logging.getLogger(__name__)
ROLES = ("history", "replay", "zone_lookup", "zone_boundaries")


def _source_info(settings, default_id=None):
    source_id = settings.get("source_id", default_id)
    source_version = settings.get("source_version")
    if not all(isinstance(value, str) and value.strip() for value in (source_id, source_version)):
        raise ValueError("Configure source_id and source_version for each source")
    return {"source_id": source_id, "source_version": source_version}


def _invalid_row_count(path):
    invalid = 0
    with path.open("rb") as handle:
        for batch in pq.ParquetFile(handle).iter_batches(columns=["validation_error"]):
            invalid += pc.count(batch.column(0)).as_py()
    return invalid


def _publish_trips(config, role, window, stage, store, root):
    kind = config.get("source", {}).get("kind", "api")
    if kind == "api":
        source = _source_info(config["api"], config["api"]["dataset_id"])
        pages, metadata = source_api.collect_pages(config, window, stage)
        paths = sorted(stage.glob("*.json"))
    elif kind == "tlc_parquet":
        source = _source_info(config["source"])
        monthly = tlc.monthly_files(config, window)
        paths = list(dict.fromkeys(monthly.values()))
        metadata = {"method": "original_monthly_parquet_scan", "months": list(monthly)}
    else:
        raise ValueError("source.kind must be api or tlc_parquet")
    digests = [(path.name, file_sha256(path)) for path in paths]
    ranges = [stamp.isoformat() for stamp in window]
    dataset_id = stable_id(role, source, ranges, digests, metadata, 2)
    raw_root = f"{root}/raw/trips/{dataset_id}"
    raw_files, uris = [], {}
    # Restore provenance to the original JSON page or Parquet file, not the derived file.
    for path, (_, digest) in zip(paths, digests):
        artifact = store.put(path, f"{root}/raw/trips/files/{digest}/{path.name}")
        raw_files.append(artifact)
        uris[path] = artifact["uri"]
    raw_index = publish_json(store, f"{raw_root}/index.json", {**metadata, "files": raw_files})
    output = stage / "trips.parquet"
    if kind == "api":
        source_api.write_parquet(pages, uris, output, config["business_timezone"], window)
    else:
        tlc.write_parquet(monthly, uris, output, window)
    with output.open("rb") as handle:
        parquet = pq.ParquetFile(handle)
        count = parquet.metadata.num_rows
        if parquet.schema_arrow != TRIP_SCHEMA or (kind == "api" and count != metadata["row_count"]):
            raise ValueError("Derived Parquet does not match the expected schema/row count")
    invalid = _invalid_row_count(output)
    artifact = store.put(output, f"{root}/parquet/{dataset_id}/{file_sha256(output)}.parquet")
    entry = {
        **source, "range_start": ranges[0], "range_end": ranges[1], "raw_path": raw_index["uri"],
        "parquet_path": artifact["uri"], "parquet_sha256": artifact["sha256"], "row_count": count,
        "invalid_row_count": invalid, "coverage": {
            "status": "confirmed", "range_start": ranges[0], "range_end": ranges[1],
            "zone_scope": "all_zones_in_source", "basis": metadata["method"],
            "invalid_pickup_rows": invalid,
            "dst_policy": "reject_ambiguous_or_nonexistent_local_timestamps",
        },
    }
    return entry


def _publish_zones(config, role, stage, store, root):
    source = _source_info(config["zones"][role])
    path, identifiers = stage_zone_source(config, role, stage)
    digest = file_sha256(path)
    artifact = store.put(path, f"{root}/raw/{role}/{digest}/{path.name}")
    return {
        **source, "range_start": None, "range_end": None,
        "raw_path": artifact["uri"], "raw_sha256": artifact["sha256"],
        "parquet_path": None, "row_count": len(identifiers), "coverage": {
            "status": "confirmed", "zone_ids": identifiers, "basis": "validated_source_file",
        },
        **({"crs": "EPSG:4326"} if role == "zone_boundaries" else {}),
    }


def publish_datasets(config, roles=ROLES, *, store=None, status_path=None):
    roles = tuple(dict.fromkeys(roles))
    if not roles or set(roles) - set(ROLES):
        raise ValueError("Select one or more supported dataset roles")
    root = hdfs_root(config)
    windows = dataset_ranges(config) if set(roles) & {"history", "replay"} else {}
    if store is None:
        with WebHDFS(config) as store:
            return publish_datasets(config, roles, store=store, status_path=status_path)
    if status_path:
        write_json(status_path, {"status": "publishing", "roles": list(roles)})
    active = "initialization"
    datasets = []
    try:
        with TemporaryDirectory(prefix="taxi-ingestion-") as directory:
            for active in roles:
                stage = Path(directory) / active
                stage.mkdir()
                if active in windows:
                    entry = _publish_trips(config, active, windows[active], stage, store, root)
                else:
                    entry = _publish_zones(config, active, stage, store, root)
                datasets.append({"dataset_role": active, "status": "ready", **entry})
                LOG.info("Verified %s: %s rows", active, entry["row_count"])
        active = "manifest"
        datasets.sort(key=lambda entry: entry["dataset_role"])
        publication_id = stable_id(datasets)
        manifest = {"version": 2, "publication_id": publication_id, "datasets": datasets}
        published = publish_json(store, f"{root}/manifests/{publication_id}.json", manifest)
    except Exception as error:
        if status_path:
            write_json(status_path, {"status": "failed", "role": active, "error": str(error)})
        raise
    result = {"status": "ready", "manifest_uri": published["uri"], **manifest}
    if status_path:
        write_json(status_path, result)
    return result


def ready_entry(manifest, role):
    if manifest.get("version") != 2:
        raise ValueError("Replay requires a v2 ingestion manifest")
    entries = [entry for entry in manifest.get("datasets", ())
               if entry.get("dataset_role") == role and entry.get("status") == "ready"]
    if len(entries) != 1 or entries[0].get("coverage", {}).get("status") != "confirmed":
        raise ValueError(f"Need exactly one ready {role} dataset with confirmed coverage")
    return entries[0]
