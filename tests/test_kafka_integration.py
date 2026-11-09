"""Opt-in real broker round trip. Does not stand in for Spark integration."""

import json
import os
import time
import uuid
from datetime import datetime

import pytest

from nyc_taxi_pipeline.ingestion.ingest import run_ingestion
from nyc_taxi_pipeline.ingestion.replay import ensure_topic, iter_events, send_events


@pytest.mark.kafka
@pytest.mark.skipif(not os.environ.get("KAFKA_BOOTSTRAP_SERVERS"), reason="No Kafka broker configured")
@pytest.mark.parametrize("backend", ["api", "tlc_parquet"])
def test_real_kafka_roundtrip(config, backend):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient

    servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    if backend == "tlc_parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq
        path = config["_root"] / "yellow_tripdata_2025-01.parquet"
        pq.write_table(pa.table({"tpep_pickup_datetime": [datetime(2025, 1, 20, 8, minute) for minute in (1, 5, 14, 15)],
                                "PULocationID": [161] * 4}), path)
        config["source"] = {"kind": "tlc_parquet", "files": {"2025-01": str(path)}}
    topic = "taxi-ingestion-test-" + uuid.uuid4().hex
    ensure_topic(servers, topic)
    producer = KafkaProducer(bootstrap_servers=servers, acks="all")
    consumer = KafkaConsumer(bootstrap_servers=servers, enable_auto_commit=False, group_id=None)
    try:
        run_ingestion(config, ("replay",))
        events = list(iter_events(config, "kafka-integration"))
        assert len(events) == 4
        assert send_events(events + [events[1]], producer, topic, 0)["events_sent"] == 5
        consumer.assign([TopicPartition(topic, 0)])
        consumer.seek_to_beginning()
        received = []
        deadline = time.monotonic() + 20
        while len(received) < 5 and time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=1000).values():
                received.extend(json.loads(record.value) for record in batch)
        assert received == events + [events[1]]
        assert len({event["event_id"] for event in received}) == 4
    finally:
        consumer.close()
        producer.close()
        admin = KafkaAdminClient(bootstrap_servers=servers)
        try:
            admin.delete_topics([topic])
        finally:
            admin.close()
