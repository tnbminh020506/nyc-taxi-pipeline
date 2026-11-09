# Bước 1.3–1.4 | Chính | Tạo sự kiện đón khách và phát qua Kafka.
#
# Đầu vào
# - Các mục ready có dataset_role = replay trong manifest và Parquet tại parquet_path.
# - replay_id, Kafka bootstrap servers, topic taxi-events và tốc độ replay.
#
# Đầu ra
# - JSON trên taxi-events gồm replay_id, event_id, pickup_at, pickup_zone_id và source_file.
# - source_file nhận diện Parquet replay, tra ngược về provenance TLC/JSON API qua manifest.
# - Log số dòng đã đọc, số sự kiện đã gửi và lỗi theo file hoặc dòng.
#
# Yêu cầu
# - Lọc pickup theo [range_start, range_end) của từng dòng manifest, rồi phát theo thứ tự pickup_at.
# - Giữ event_id ổn định cho cùng dòng của bộ mẫu cố định, kể cả khi gửi lại.
# - Phân biệt hai dòng nguồn hợp lệ có cùng giá trị. Dùng định danh file và vị trí dòng ổn định.
# - pickup_at theo ISO 8601 có offset hoặc UTC. Ghi rõ quy ước chuyển thời gian nguồn.
# - Payload chỉ chứa thông tin có tại thời điểm đón khách.
# - Python điều tiết tốc độ phát. Kiểm tra Spark đọc được JSON trên topic đã thống nhất.

import hashlib
import json
import logging
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

import pyarrow.parquet as pq

from .common import aware_time, dataset_source_id, resolve_path
from .ingest import file_sha256
from .manifest import entry_key, read_manifest

LOG = logging.getLogger(__name__)


def replay_entries(config):
    section = config["datasets"]["replay"]
    key = entry_key({"dataset_role": "replay", "source_id": dataset_source_id(config), **section})
    manifest = read_manifest(resolve_path(config, config["paths"]["manifest"]))
    entries = [entry for entry in manifest["datasets"]
               if entry["dataset_role"] == "replay" and entry_key(entry) == key and entry["status"] == "ready"]
    if not entries:
        raise ValueError("No ready replay dataset matches the source and range in this config")
    return entries


def iter_events(config, replay_id, stats=None):
    """Sort on disk, so unsorted/multiple Parquet row groups do not exhaust RAM."""
    if not replay_id or not replay_id.strip():
        raise ValueError("replay_id must be nonempty")
    stats = stats if stats is not None else {}
    stats.update(rows_read=0, invalid_rows=0, outside_range=0, events_prepared=0)
    entries = replay_entries(config)
    with tempfile.TemporaryDirectory(prefix="taxi-replay-") as temporary:
        with closing(sqlite3.connect(str(Path(temporary) / "events.sqlite"))) as connection:
            connection.execute("CREATE TABLE events (pickup INTEGER, event_id TEXT, payload TEXT)")
            for entry in entries:
                path = Path(entry["parquet_path"])
                checksum = file_sha256(path)
                if entry.get("parquet_sha256") != checksum:
                    raise ValueError(f"Parquet checksum differs from manifest: {path}")
                if not entry.get("coverage_complete"):
                    raise ValueError("Replay requires confirmed coverage")
                lower, upper = aware_time(entry["range_start"]), aware_time(entry["range_end"])
                identity = json.dumps([entry["source_id"], lower.isoformat(), upper.isoformat(), checksum])
                row_number = 0
                for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
                    insertions = []
                    for row in batch.to_pylist():
                        current_row = row_number
                        row_number += 1
                        stats["rows_read"] += 1
                        pickup, zone = row.get("pickup_at"), row.get("pickup_zone_id")
                        if (row.get("validation_error") or pickup is None or pickup.tzinfo is None
                                or isinstance(zone, bool) or not isinstance(zone, int) or zone <= 0):
                            stats["invalid_rows"] += 1
                            if stats["invalid_rows"] <= 20:
                                LOG.warning("Invalid row file=%s row=%d reason=%s", path, current_row,
                                            row.get("validation_error") or "invalid pickup time/zone")
                            continue
                        if not lower <= pickup < upper:
                            stats["outside_range"] += 1
                            continue
                        event_id = hashlib.sha256(f"{identity}:{current_row}".encode()).hexdigest()
                        payload = {"replay_id": replay_id, "event_id": event_id,
                                   "pickup_at": pickup.isoformat(), "pickup_zone_id": zone,
                                   "source_file": str(path)}
                        insertions.append((int(pickup.timestamp() * 1_000_000), event_id,
                                           json.dumps(payload, ensure_ascii=False)))
                    connection.executemany("INSERT INTO events VALUES (?, ?, ?)", insertions)
                connection.commit()
            connection.execute("CREATE INDEX event_order ON events(pickup, event_id)")
            for (payload,) in connection.execute("SELECT payload FROM events ORDER BY pickup, event_id"):
                stats["events_prepared"] += 1
                yield json.loads(payload)


def ensure_topic(bootstrap_servers, topic, partitions=1):
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="taxi-ingestion-admin")
    try:
        try:
            admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


def send_events(events, producer, topic, events_per_second, stats=None):
    import math
    rate = float(events_per_second)
    if not math.isfinite(rate) or rate < 0:
        raise ValueError("events_per_second must be finite and nonnegative")
    stats = stats if stats is not None else {}
    stats["events_sent"] = 0
    deadline = time.monotonic()
    for event in events:
        if rate:
            time.sleep(max(0, deadline - time.monotonic()))
        # Acknowledge every event: a failed send stops the command with nonzero
        # exit status. Replaying the same input retains IDs for downstream dedup.
        producer.send(topic, key=f"{event['replay_id']}:{event['pickup_zone_id']}".encode(),
                      value=json.dumps(event, ensure_ascii=False).encode("utf-8")).get(timeout=30)
        stats["events_sent"] += 1
        if rate:
            deadline = max(deadline + 1 / rate, time.monotonic())
    producer.flush(timeout=30)
    return stats


def run_replay(config, replay_id=None, dry_run=False, output=None, create_topic=False):
    replay = config["replay"]
    replay_id = replay_id or replay["replay_id"]
    stats = {}
    events = iter_events(config, replay_id, stats)
    if dry_run:
        import sys
        handle = None
        try:
            if output:
                path = resolve_path(config, output)
                path.parent.mkdir(parents=True, exist_ok=True)
                handle = path.open("w", encoding="utf-8", newline="\n")
            for event in events:
                print(json.dumps(event, ensure_ascii=False), file=handle or sys.stdout)
        finally:
            if handle:
                handle.close()
            events.close()
        stats["mode"] = "dry-run"
    else:
        from kafka import KafkaProducer
        kafka = config["kafka"]
        if create_topic:
            ensure_topic(kafka["bootstrap_servers"], kafka["topic"], kafka.get("partitions", 1))
        producer = KafkaProducer(bootstrap_servers=kafka["bootstrap_servers"], acks="all", retries=3,
                                 max_in_flight_requests_per_connection=1, client_id="taxi-replay")
        try:
            send_events(events, producer, kafka["topic"], replay["events_per_second"], stats)
        finally:
            events.close()
            producer.close(timeout=30)
        stats["mode"] = "kafka"
    LOG.info("replay_id=%s summary=%s", replay_id, json.dumps(stats))
    return stats
