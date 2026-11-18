"""Exercise lifecycle gating, retries, and archive commit boundaries."""

import base64
import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from kafka.structs import TopicPartition

from test_ingestion_v2 import ingestion_config, source_server
from nyc_taxi_pipeline.ingestion.dropoff_simulator import DropoffLease, publish_dropoff
from nyc_taxi_pipeline.ingestion.event_archive import JournalLease, archive_journal, archive_poll, publish_snapshot
from nyc_taxi_pipeline.ingestion.hdfs_store import WebHDFS
from nyc_taxi_pipeline.ingestion.kafka_publisher import TaxiPublisher
from nyc_taxi_pipeline.ingestion.pickup_publisher import PickupLease, publish_pickup
from nyc_taxi_pipeline.shared.clock import SimulationClock

START = datetime(2025, 1, 20, 13, tzinfo=timezone.utc)
TOPIC = "taxi-events"
PARTITION = TopicPartition(TOPIC, 0)


def pickup():
    return {
        "event_id": "pickup-event", "schema_version": 2, "source_kind": "dispatch",
        "event_type": "pickup_completed", "scope_id": "scope", "replay_id": "demo",
        "event_at": START.isoformat(), "picked_up_at": START.isoformat(),
        "trip_id": "trip", "request_ids": ["r1", "r2"], "trip_mode": "shared",
        "pickup_zone_id": 1, "pickup_lat": 40.1, "pickup_lon": -74.1, "outbox_id": "outbox",
    }


def dropoff(request, duration=300):
    return DropoffLease(
        f"lease-{request}", {
            "event_id": f"dropoff-{request}", "scope_id": "scope", "replay_id": "demo",
            "trip_id": "trip", "request_id": request, "dropoff_zone_id": 2,
            "dropoff_lat": 40.2, "dropoff_lon": -73.9, "scheduled_event_id": f"scheduled-{request}",
        },
        START, START + timedelta(seconds=duration), duration, True,
    )


class Broker:
    def __init__(self):
        self.messages = []
        self.fail = False

    def send(self, topic, *, key, value):
        def get(*, timeout):
            if self.fail:
                raise TimeoutError("broker did not acknowledge")
            offset = len(self.messages)
            self.messages.append((key, json.loads(value)))
            return SimpleNamespace(topic=topic, partition=0, offset=offset)

        return SimpleNamespace(get=get)


class Schedule:
    def __init__(self):
        self.pickup_acked = False
        self.database_failure = False
        self.dropoffs = [dropoff("r1"), dropoff("r2", 360)]
        self.quarantined = []

    def claim_pickup(self, *, now, lease_seconds):
        return None if self.pickup_acked else PickupLease("pickup-lease", pickup())

    def acknowledge_pickup(self, outbox_id, lease_token, ack):
        assert outbox_id == "outbox" and lease_token == "pickup-lease" and ack.topic == TOPIC
        if self.database_failure:
            raise RuntimeError("worker stopped after broker ack")
        self.pickup_acked = True

    def claim_dropoff(self, *, now, lease_seconds):
        return next((item for item in self.dropoffs if self.pickup_acked and item.due_at <= now), None)

    def acknowledge_dropoff(self, scheduled_event_id, lease_token, ack):
        assert ack.topic == TOPIC
        self.dropoffs = [item for item in self.dropoffs if item.event["scheduled_event_id"] != scheduled_event_id]

    def quarantine_dropoff(self, scheduled_event_id, lease_token, reason):
        self.quarantined.append((scheduled_event_id, lease_token, reason))


def test_delivery_retries_keep_id_and_schedule_waits_for_pickup_ack():
    repository, broker = Schedule(), Broker()
    publisher = TaxiPublisher(broker, TOPIC)
    clock = SimpleNamespace(now=lambda: START + timedelta(hours=1))
    assert not publish_dropoff(repository, publisher, clock)
    broker.fail = True
    with pytest.raises(TimeoutError):
        publish_pickup(repository, publisher, clock)
    assert not repository.pickup_acked
    broker.fail, repository.database_failure = False, True
    with pytest.raises(RuntimeError):
        publish_pickup(repository, publisher, clock)
    assert not publish_dropoff(repository, publisher, clock)
    repository.database_failure = False
    assert publish_pickup(repository, publisher, clock)
    assert publish_dropoff(repository, publisher, clock)
    assert publish_dropoff(repository, publisher, clock)
    assert not publish_dropoff(repository, publisher, clock)
    assert broker.messages[0] == broker.messages[1]
    events = [event for _, event in broker.messages]
    assert len({event["event_id"] for event in events}) == 3
    assert all(key == b'["demo","trip"]' for key, _ in broker.messages)
    for duration, event in zip((300, 360), events[2:]):
        assert event["completed_at"] == (START + timedelta(seconds=duration)).isoformat()


def test_dropoff_checks_repository_gating_and_quarantines_invalid_duration():
    broker, repository = Broker(), Schedule()
    publisher = TaxiPublisher(broker, TOPIC)
    clock = SimpleNamespace(now=lambda: START + timedelta(hours=1))
    work = dropoff("r1")
    repository.claim_dropoff = lambda **kwargs: replace(work, pickup_acked=False)
    with pytest.raises(ValueError, match="before pickup ack"):
        publish_dropoff(repository, publisher, clock)
    repository.claim_dropoff = lambda **kwargs: replace(work, source_duration_seconds=-1)
    assert publish_dropoff(repository, publisher, clock)
    assert repository.quarantined[0][2] == "invalid_source_duration"
    assert broker.messages == []


def test_clock_advances_without_input_and_workers_agree_after_restart():
    elapsed = [0.0]
    monotonic = lambda: elapsed[0]
    wall_now = lambda: START + timedelta(seconds=elapsed[0])

    def sleep(seconds):
        elapsed[0] += seconds

    first = SimulationClock(START, START, 2, monotonic=monotonic, wall_now=wall_now, sleep=sleep)
    first.wait_until(START + timedelta(seconds=300))
    assert elapsed[0] == 150
    restarted = SimulationClock(START, START, 2, monotonic=monotonic, wall_now=wall_now)
    elapsed[0] += 10
    assert first.now() == restarted.now() == START + timedelta(seconds=320)


def kafka_record(offset, value):
    return SimpleNamespace(topic=TOPIC, partition=0, offset=offset, timestamp=100,
                           key=b'trip-key', value=value, headers=[("source", b"test")])


class Consumer:
    def __init__(self, *records):
        self.records = records
        self.position = 0
        self.acks = []

    def poll(self, **kwargs):
        result = {PARTITION: [record for record in self.records if record.offset >= self.position]}
        self.position = self.records[-1].offset + 1
        return result

    def commit(self, *, offsets):
        self.acks.append(offsets)

    def assignment(self):
        return {PARTITION}

    def seek(self, partition, offset):
        assert partition == PARTITION
        self.position = offset


class Journal:
    def __init__(self, *entries):
        self.entries = list(entries)
        self.acks = []
        self.recorded = []

    def claim_journal(self, **kwargs):
        return [JournalLease(f"lease-{entry['entry_id']}", entry) for entry in self.entries]

    def acknowledge_journal(self, entries, receipt):
        self.acks.append((entries, receipt))

    def record_event_archive(self, receipt):
        self.recorded.append(receipt)


def _admission(offset, event_id="pickup-event"):
    return {
        "kind": "admission", "entry_id": f"admission-{offset}", "scope_id": "scope", "replay_id": "demo",
        "policy_version": "watermark-1", "topic": TOPIC, "partition": 0, "offset": offset,
        "event_id": event_id, "event_at": START.isoformat(), "ingested_at": START.isoformat(),
        "decision": "quarantined", "reason": "after_cutoff", "watermark_at": START.isoformat(),
    }


def test_archive_preserves_raw_and_pins_snapshot_with_admission(tmp_path):
    event = json.dumps(pickup()).encode()
    consumer = Consumer(kafka_record(0, event), kafka_record(1, b'not-json'))
    journal = Journal({"kind": "journal", "entry_id": "dispatch-1", "scope_id": "scope", "replay_id": "demo"},
                      _admission(0))
    with source_server() as server, WebHDFS(ingestion_config(tmp_path, server)) as store:
        receipt, = archive_poll(consumer, store, "/demo", repository=journal)
        assert journal.recorded == [receipt]
        assert consumer.acks[-1][PARTITION].offset == 2
        payload = tmp_path / "segment.jsonl"
        store.download(receipt["hdfs_path"], payload, receipt["sha256"])
        archived = [json.loads(line) for line in payload.read_bytes().splitlines()]
        assert base64.b64decode(archived[0]["value_base64"]) == event
        assert base64.b64decode(archived[1]["value_base64"]) == b"not-json"
        spec = {
            "scope_id": "scope", "replay_id": "demo", "cutoff": START.isoformat(),
            "admission_policy_version": "watermark-1", "coverage": {"status": "confirmed"},
            "receipt_uris": [receipt["receipt_uri"]],
            "archive_offset_ranges": [{"topic": TOPIC, "partition": 0, "offset_start": 0, "offset_end": 2}],
        }
        with pytest.raises(ValueError, match="matching admission"):
            publish_snapshot(store, "/demo", spec)
        archive_uris = [item["receipt_uri"] for item in archive_journal(journal, store, "/demo")]
        spec["receipt_uris"].extend(archive_uris)
        snapshot = publish_snapshot(store, "/demo", spec)
        assert len(journal.acks) == 2
        assert snapshot == publish_snapshot(store, "/demo", spec)
        spec["archive_offset_ranges"][0]["offset_end"] = 3
        with pytest.raises(ValueError, match="missing offsets"):
            publish_snapshot(store, "/demo", spec)


def test_archive_failure_rewinds_offsets_for_retry_and_blocks_ack(tmp_path):
    consumer = Consumer(kafka_record(0, json.dumps(pickup()).encode()))
    journal = Journal(_admission(0))
    with source_server() as server, WebHDFS(ingestion_config(tmp_path, server)) as store:
        server.replicas = 1
        with pytest.raises(ValueError, match="not replicated"):
            archive_poll(consumer, store, "/demo", repository=journal)
        with pytest.raises(ValueError, match="not replicated"):
            archive_journal(journal, store, "/demo")
        assert consumer.acks == journal.acks == journal.recorded == []
        assert consumer.position == 0 and server.files == {}
        server.replicas = 2
        receipt, = archive_poll(consumer, store, "/demo", repository=journal)
        assert receipt["offset_start"] == 0 and consumer.acks[0][PARTITION].offset == 1
