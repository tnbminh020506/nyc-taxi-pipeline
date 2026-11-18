"""Archive raw Kafka records and warehouse journals as immutable HDFS inputs."""

import base64
import json
import logging
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol

from kafka.structs import OffsetAndMetadata

from .common import aware_time, file_sha256, json_bytes, stable_id
from .hdfs_store import publish_json

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class JournalLease:
    lease_token: str
    entry: dict


class ArchiveRepository(Protocol):
    def claim_journal(self, *, limit: int, lease_seconds: int) -> list[JournalLease]: ...

    def acknowledge_journal(self, entries: list[tuple[str, str]], receipt: dict) -> None: ...

    def record_event_archive(self, receipt: dict) -> None: ...


def _encoded(value):
    return None if value is None else base64.b64encode(value).decode("ascii")


def _raw_record(record):
    event = None
    try:
        value = json.loads(record.value)
        if isinstance(value, dict) and value.get("schema_version") == 2:
            event = value
    except (ValueError, TypeError, RecursionError):
        pass
    identity = {}
    if event:
        identity = {key: event[key] for key in ("event_id", "event_type", "replay_id", "scope_id", "event_at")
                    if isinstance(event.get(key), str) and event[key]}
    return {
        "topic": record.topic, "partition": record.partition, "offset": record.offset,
        "kafka_timestamp_ms": record.timestamp, "key_base64": _encoded(record.key),
        "value_base64": _encoded(record.value),
        "headers": [[name, _encoded(value)] for name, value in (record.headers or ())],
        **identity,
    }


def _write_segment(store, root, kind, rows, **metadata):
    with TemporaryDirectory(prefix="taxi-archive-") as directory:
        path = Path(directory) / "archive.jsonl"
        with path.open("wb") as handle:
            for row in rows:
                handle.write(json_bytes(row) + b"\n")
        digest = file_sha256(path)
        archive_id = stable_id(kind, digest, metadata)
        prefix = f"{root}/archive/{kind}/{archive_id}"
        data = store.put(path, f"{prefix}.jsonl")
    receipt = {
        "archive_id": archive_id, "kind": kind, "status": "committed", "hdfs_path": data["uri"],
        "sha256": digest, "row_count": len(rows), **metadata,
    }
    published = publish_json(store, f"{prefix}.receipt.json", receipt)
    return {**receipt, "receipt_uri": published["uri"]}


def _contiguous(records):
    group = []
    for record in records:
        if group and record.offset != group[-1].offset + 1:
            if record.offset <= group[-1].offset:
                raise ValueError("Kafka poll returned non-increasing offsets")
            yield group
            group = []
        group.append(record)
    if group:
        yield group


def _event_segment(store, root, partition, records):
    rows = [_raw_record(record) for record in records]
    scopes = sorted({(row["scope_id"], row["replay_id"]) for row in rows
                     if "scope_id" in row and "replay_id" in row})
    return _write_segment(store, root, "events", rows,
                          topic=partition.topic, partition=partition.partition,
                          offset_start=records[0].offset, offset_end=records[-1].offset + 1,
                          scopes=[list(scope) for scope in scopes],
                          event_index=[{key: row.get(key) for key in
                                        ("offset", "scope_id", "replay_id", "event_id", "event_at")}
                                       for row in rows if "scope_id" in row and "replay_id" in row],
                          event_ids=sorted({row["event_id"] for row in rows if "event_id" in row}))


def archive_poll(consumer, store, root, *, repository=None, max_records=500, timeout_ms=1000):
    """Commit Kafka offsets only after the entire returned poll is durable."""
    records_by_partition = consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
    offsets, receipts = {}, []
    try:
        for partition, records in records_by_partition.items():
            for group in _contiguous(records):
                receipt = _event_segment(store, root, partition, group)
                if repository is not None:
                    repository.record_event_archive(receipt)
                receipts.append(receipt)
                offsets[partition] = OffsetAndMetadata(group[-1].offset + 1, "", -1)
        if offsets:
            consumer.commit(offsets=offsets)
    except Exception:
        try:
            assigned = consumer.assignment()
            for partition, records in records_by_partition.items():
                if records and partition in assigned:
                    consumer.seek(partition, records[0].offset)
        except Exception:
            LOG.exception("Could not rewind Kafka position; restart from the committed offsets")
        raise
    return receipts


def archive_journal(repository: ArchiveRepository, store, root, *, limit=500, lease_seconds=90):
    claims = repository.claim_journal(limit=limit, lease_seconds=lease_seconds)
    entries_by_scope = defaultdict(list)
    for claim in claims:
        entry = claim.entry
        kind = entry["kind"]
        if kind not in ("journal", "admission"):
            raise ValueError("Archive journal kind must be journal or admission")
        if not all(isinstance(entry[key], str) and entry[key] for key in ("scope_id", "replay_id", "entry_id")):
            raise ValueError("Journal entry requires scope_id, replay_id and entry_id")
        if kind == "admission":
            _validate_admission(entry)
        entries_by_scope[kind, entry["scope_id"], entry["replay_id"]].append(claim)
    receipts = []
    for (kind, scope_id, replay_id), group in entries_by_scope.items():
        group.sort(key=lambda claim: claim.entry["entry_id"])
        items = [(claim.entry["entry_id"], claim.lease_token) for claim in group]
        metadata = {"scope_id": scope_id, "replay_id": replay_id, "entry_ids": [entry_id for entry_id, _ in items]}
        receipt = _write_segment(store, root, kind, [claim.entry for claim in group], **metadata)
        repository.acknowledge_journal(items, receipt)
        receipts.append(receipt)
    return receipts


def _validate_admission(entry):
    if (not all(isinstance(entry[key], str) and entry[key] for key in ("policy_version", "topic", "reason"))
            or type(entry["partition"]) is not int or entry["partition"] < 0
            or type(entry["offset"]) is not int or entry["offset"] < 0
            or entry["decision"] not in ("accepted", "quarantined", "duplicate")
            or "event_id" not in entry or "event_at" not in entry):
        raise ValueError("Invalid admission metadata")
    aware_time(entry["ingested_at"])
    if entry["event_at"] is not None:
        aware_time(entry["event_at"])
    if entry.get("watermark_at") is not None:
        aware_time(entry["watermark_at"])


def _indexed_intervals(ranges):
    intervals = defaultdict(list)
    for item in ranges:
        partition, begin, end = (item[key] for key in ("partition", "offset_start", "offset_end"))
        if (not isinstance(item["topic"], str) or not item["topic"]
                or any(type(value) is not int for value in (partition, begin, end))
                or partition < 0 or not 0 <= begin < end):
            raise ValueError("Invalid Kafka offset interval; offset_end is exclusive")
        intervals[item["topic"], partition].append((begin, end))
    return intervals


def _covers(intervals, start, end):
    cursor = start
    for left, right in sorted(intervals):
        if left > cursor:
            break
        cursor = max(cursor, right)
        if cursor >= end:
            return True
    return False


def publish_snapshot(store, root, spec):
    """Freeze receipts at an explicit cutoff and verify the requested Kafka ranges."""
    scope_id, replay_id, policy = (spec[key] for key in ("scope_id", "replay_id", "admission_policy_version"))
    if not all(isinstance(value, str) and value for value in (scope_id, replay_id, policy)):
        raise ValueError("Snapshot needs a scope, replay and admission policy version")
    cutoff = aware_time(spec["cutoff"]).isoformat()
    coverage = spec["coverage"]
    if not isinstance(coverage, dict) or coverage.get("status") not in ("pending", "confirmed"):
        raise ValueError("Snapshot requires a coverage statement from processing")
    required = _indexed_intervals(spec["archive_offset_ranges"])
    receipts, expected, decisions = [], {}, {}
    for uri in sorted(set(spec["receipt_uris"])):
        receipt = store.json(uri)
        kind = receipt["kind"]
        if receipt.get("status") != "committed" or kind not in ("events", "journal", "admission"):
            raise ValueError(f"Invalid archive receipt: {uri}")
        if kind != "events" and (receipt["scope_id"], receipt["replay_id"]) != (scope_id, replay_id):
            raise ValueError(f"Archive receipt has the wrong scope/replay: {uri}")
        if kind == "admission":
            for entry in _read_admission(store, receipt, policy):
                partition = (entry["topic"], entry["partition"])
                offset = entry["offset"]
                if not any(begin <= offset < end for begin, end in required.get(partition, ())):
                    continue
                key = (*partition, offset)
                existing = decisions.setdefault(key, entry)
                if existing != entry:
                    raise ValueError(f"Conflicting admission metadata for {key}")
        if kind == "events":
            partition = (receipt["topic"], receipt["partition"])
            for entry in receipt["event_index"]:
                offset = entry["offset"]
                if ((entry["scope_id"], entry["replay_id"]) == (scope_id, replay_id)
                        and any(begin <= offset < end for begin, end in required.get(partition, ()))):
                    key = (*partition, offset)
                    existing = expected.setdefault(key, entry)
                    if existing != entry:
                        raise ValueError(f"Conflicting archived events at {key}")
        # Keep the snapshot manifest compact. The per-event indexes stay in the receipts.
        pointer = {key: value for key, value in receipt.items()
                   if key not in ("event_ids", "event_index", "entry_ids")}
        receipts.append({**pointer, "receipt_uri": uri})
    available = _indexed_intervals(receipt for receipt in receipts if receipt["kind"] == "events")
    for key, ranges in required.items():
        for begin, end in ranges:
            if not _covers(available[key], begin, end):
                raise ValueError(f"Archive has missing offsets for {key} in [{begin}, {end})")
    if coverage["status"] == "confirmed":
        for key, entry in expected.items():
            decision = decisions.get(key)
            if decision is None or any(decision[field] != entry[field] for field in ("event_id", "event_at")):
                raise ValueError(f"Confirmed coverage needs matching admission metadata at {key}")
    snapshot = {
        "version": 1, "scope_id": scope_id, "replay_id": replay_id, "cutoff": cutoff,
        "admission_policy_version": policy, "coverage": coverage,
        "archive_offset_ranges": spec["archive_offset_ranges"], "receipts": receipts,
    }
    identifier = stable_id(snapshot)
    snapshot["input_snapshot_id"] = identifier
    artifact = publish_json(store, f"{root}/archive/snapshots/{identifier}.json", snapshot)
    return {**snapshot, "snapshot_uri": artifact["uri"]}


def _read_admission(store, receipt, policy):
    with TemporaryDirectory(prefix="taxi-admission-") as directory:
        path = Path(directory) / "admission.jsonl"
        store.download(receipt["hdfs_path"], path, receipt["sha256"])
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                entry = json.loads(line)
                _validate_admission(entry)
                if entry["policy_version"] != policy:
                    raise ValueError("Admission ledger mixes policy versions")
                yield entry
