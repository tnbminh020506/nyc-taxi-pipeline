"""Create v2 requests from the published, normalized replay dataset."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow.parquet as pq

from .common import aware_time, stable_id
from .coordinates import load_boundaries, simulated_point
from .manifest import ready_entry
from .trip_rows import TRIP_SCHEMA

LOG = logging.getLogger(__name__)


def _request(row, entry, polygons, replay_id, scope_id, seed):
    source_id, record_id = entry["source_id"], row["source_record_id"]
    request_id = stable_id(replay_id, source_id, entry["source_version"], record_id)
    timestamp = row["pickup_at"].isoformat()
    event = {
        "schema_version": 2, "event_type": "ride_requested", "source_kind": "nyc_replay",
        "replay_id": replay_id, "scope_id": scope_id, "request_id": request_id,
        "event_id": stable_id("ride_requested", request_id),
        "event_at": timestamp, "requested_at": timestamp,
        **{key: row[key] for key in ("source_file", "source_record_id", "pickup_zone_id",
                                     "dropoff_zone_id", "source_duration_seconds")},
    }
    simulated = False
    errors = row["matching_error"].split(";") if row["matching_error"] else []
    for leg in ("pickup", "dropoff"):
        lat, lon = row[f"{leg}_lat"], row[f"{leg}_lon"]
        if lat is None or lon is None:
            lat, lon = simulated_point(polygons, row[f"{leg}_zone_id"], seed, source_id, record_id, leg)
            simulated |= lat is not None
        if lat is None:
            errors.append(f"missing_{leg}_coordinates")
        event[f"{leg}_lat"], event[f"{leg}_lon"] = lat, lon
    event["coordinates_simulated"] = simulated
    if errors:
        event["matching_error"] = ";".join(errors)
    return event


def iter_requests(store, manifest_uri, replay_id, scope_id, *, seed=0):
    if not all(isinstance(value, str) and value.strip() for value in (replay_id, scope_id)):
        raise ValueError("replay_id and scope_id are required")
    manifest = store.json(manifest_uri)
    entry, boundaries = (ready_entry(manifest, role) for role in ("replay", "zone_boundaries"))
    lower, upper = (aware_time(entry[key]) for key in ("range_start", "range_end"))
    emitted = rejected = matching_errors = 0
    previous = None
    with TemporaryDirectory(prefix="taxi-replay-") as directory:
        path = Path(directory)
        store.download(entry["parquet_path"], path / "replay.parquet", entry["parquet_sha256"])
        store.download(boundaries["raw_path"], path / "zones.geojson", boundaries["raw_sha256"])
        polygons = load_boundaries(path / "zones.geojson")
        with (path / "replay.parquet").open("rb") as parquet_file:
            replay = pq.ParquetFile(parquet_file)
            if replay.schema_arrow != TRIP_SCHEMA or replay.metadata.num_rows != entry["row_count"]:
                raise ValueError("Replay Parquet schema or row count differs from the manifest")
            for batch in replay.iter_batches(batch_size=8192):
                for row in batch.to_pylist():
                    timestamp = row["pickup_at"]
                    if row["validation_error"] or timestamp is None or row["pickup_zone_id"] is None:
                        rejected += 1
                        continue
                    if not lower <= timestamp < upper or (previous is not None and timestamp < previous):
                        raise ValueError("Replay Parquet timestamps are out of range or order")
                    previous = timestamp
                    event = _request(row, entry, polygons, replay_id, scope_id, seed)
                    matching_errors += "matching_error" in event
                    emitted += 1
                    yield event
    LOG.info("Replay prepared=%d rejected_pickup=%d incomplete_matching=%d", emitted, rejected, matching_errors)


def publish_replay(events, publisher, clock):
    count = 0
    for event in events:
        clock.wait_until(event["event_at"])
        publisher.publish(event)
        count += 1
    return count
