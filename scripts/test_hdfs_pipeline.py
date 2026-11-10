"""Acceptance against a real two-DataNode HDFS cluster, with no TLC requests.

Stress input is explicitly synthetic. This tests distributed persistence,
replication, replay/Kafka and failure behavior, not NYC source availability.
"""

import argparse
import copy
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from nyc_taxi_pipeline.ingestion.common import load_config, write_json
from nyc_taxi_pipeline.ingestion.hdfs import WebHDFS, receipt_path, hdfs_manifest
from nyc_taxi_pipeline.ingestion.ingest import file_sha256, run_ingestion
from nyc_taxi_pipeline.ingestion.replay import ensure_topic, iter_events, send_events
from nyc_taxi_pipeline.ingestion.split import balanced_ranges


def kafka_roundtrip(config, events):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient
    topic = "hdfs-replay-" + uuid.uuid4().hex
    servers = config["kafka"]["bootstrap_servers"]
    ensure_topic(servers, topic)
    producer = KafkaProducer(bootstrap_servers=servers, acks="all")
    consumer = KafkaConsumer(bootstrap_servers=servers, enable_auto_commit=False, group_id=None)
    try:
        expected = events + [events[1]]
        send_events(expected, producer, topic, 0)
        consumer.assign([TopicPartition(topic, 0)])
        consumer.seek_to_beginning()
        received = []
        deadline = time.monotonic() + 30
        while len(received) < len(expected) and time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1000).values():
                received.extend(json.loads(record.value) for record in records)
        assert received == expected
        return {"messages_received": len(received), "unique_event_ids": len({e["event_id"] for e in received})}
    finally:
        producer.close()
        consumer.close()
        admin = KafkaAdminClient(bootstrap_servers=servers)
        try:
            admin.delete_topics([topic])
        finally:
            admin.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--report", default="output/hdfs-acceptance/report.json")
    parser.add_argument("--fault-test", action="store_true", help="Stop/restart datanode-2 in the taxi-hdfs demo Compose project")
    args = parser.parse_args()
    if not 10_000 <= args.rows <= 2_000_000:
        parser.error("rows must be 10,000..2,000,000 (within January synthetic timestamps)")
    root = Path(__file__).resolve().parents[1]
    workspace = root / "output/hdfs-acceptance" / uuid.uuid4().hex
    workspace.mkdir(parents=True)
    config = load_config(root / "configs/hdfs.example.yaml")
    config["paths"] = {"raw": str(workspace / "raw"), "parquet": str(workspace / "parquet"),
                       "manifest": str(workspace / "local-manifest.json")}
    config["storage"].update(root="/nyc-taxi-acceptance/" + workspace.name,
                             receipt=str(workspace / "receipt.json"), replay_cache=str(workspace / "replay-cache"),
                             block_size=1024 * 1024)
    config["source"]["source_id"] = "synthetic-hdfs-stress"
    lookup = workspace / "lookup.csv"
    lookup.write_text("LocationID,Borough,Zone\n161,Manhattan,Test zone\n", encoding="utf-8")
    config["zone_lookup"]["source"] = str(lookup)
    source = workspace / "yellow_tripdata_2025-01.parquet"
    report = {"status": "running", "input_mode": "synthetic", "tlc_requests": 0,
              "rows_requested": args.rows, "replication_required": 2, "fault_test_requested": args.fault_test}
    restarted = True
    compose = ["docker", "compose", "-p", "taxi-hdfs", "-f", str(root / "docker-compose.hdfs.yml")]
    try:
        with duckdb.connect(config={"memory_limit": "512MB", "threads": 2}) as connection:
            connection.execute("COPY (SELECT TIMESTAMP '2025-01-01' + i * INTERVAL '1 second' AS tpep_pickup_datetime, "
                               "(1 + i % 265)::INTEGER AS PULocationID, md5(i::VARCHAR) AS diagnostic_payload "
                               f"FROM range({args.rows}) AS rows(i)) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(source)])
        config["source"]["files"] = {"2025-01": str(source)}
        config["datasets"], report["split_plan"] = balanced_ranges(
            config, [source], "2025-01-01T00:00:00-05:00", "2025-02-01T00:00:00-05:00")
        started = time.monotonic()
        large = run_ingestion(config)
        assert all(e["status"] == "ready" for e in large), large
        report["large_publish_seconds"] = round(time.monotonic() - started, 3)
        report["large_entries"] = large
        report["large_manifest"] = json.loads(receipt_path(config).read_text())["manifest_uri"]
        assert sum(e["row_count"] for e in large if e["dataset_role"] != "zone_lookup") == args.rows
        with WebHDFS(config) as client:
            proof = []
            for entry in large:
                if entry.get("parquet_path"):
                    cached = client.download(entry["parquet_path"], workspace / (entry["dataset_role"] + ".parquet"), entry["parquet_sha256"])
                    assert pq.ParquetFile(cached).metadata.num_rows == entry["row_count"]
                    blocks = client.wait_replicas(entry["parquet_path"])
                    proof.append({"uri": entry["parquet_path"], "blocks": len(blocks),
                                  "minimum_live_replicas": min(len(set(b["names"])) for b in blocks),
                                  "data_nodes": sorted({name for block in blocks for name in block["names"]})})
            original = large[0]["original_sources"][0]
            assert client.sha256(original["uri"]) == file_sha256(source)
            proof.append({**original, "blocks": len(client.wait_replicas(original["uri"]))})
            report["replication_proof"] = proof
        # Independent small publication for replay/Kafka/failure tests. The large
        # publication above remains immutable and readable via its own URI.
        small = workspace / "yellow_tripdata_2025-01-small.parquet"
        pq.write_table(pa.table({"tpep_pickup_datetime": [datetime(2025, 1, 6, 8, 1)] * 6
                                 + [datetime(2025, 1, 20, 8, 1)] * 4,
                                "PULocationID": [161] * 10}), small)
        config["source"].update(source_id="synthetic-hdfs-contract", files={"2025-01": str(small)})
        config["datasets"] = {role: {"range_start": f"2025-01-{day}T08:00:00-05:00",
                                      "range_end": f"2025-01-{day}T08:15:00-05:00"}
                              for role, day in (("history", "06"), ("replay", "20"))}
        contract = run_ingestion(config)
        assert all(e["status"] == "ready" for e in contract), contract
        events = list(iter_events(config, "hdfs-acceptance"))
        assert len(events) == 4 and len({e["event_id"] for e in events}) == 4
        assert all(e["source_file"].startswith("hdfs://") for e in events)
        assert events == list(iter_events(config, "hdfs-acceptance"))
        report["kafka"] = kafka_roundtrip(config, events)
        receipt = json.loads(receipt_path(config).read_text())
        pinned = copy.deepcopy(config)
        pinned["storage"]["manifest_uri"] = receipt["manifest_uri"]
        # Remove producer staging Parquet, the local manifest/receipt and replay
        # cache. Limit all removals to this uniquely owned acceptance directory.
        for path in [*Path(config["paths"]["parquet"]).glob("*.parquet"),
                     *Path(config["storage"]["replay_cache"]).glob("*.parquet"),
                     Path(config["paths"]["manifest"]), receipt_path(config)]:
            assert path.resolve().is_relative_to(workspace.resolve())
            path.unlink(missing_ok=True)
        assert list(iter_events(pinned, "hdfs-acceptance")) == events
        report["replay_after_local_staging_removed"] = True
        report["stable_event_ids"] = True
        if args.fault_test:
            subprocess.run(compose + ["stop", "datanode-2"], check=True, timeout=60)
            restarted = False
            replay_entry = next(e for e in contract if e["dataset_role"] == "replay")
            with WebHDFS(config) as client:
                deadline = time.monotonic() + 60
                while True:
                    blocks = client.blocks(replay_entry["parquet_path"])
                    if blocks and all(len(b["names"]) == 1 for b in blocks):
                        break
                    if time.monotonic() >= deadline:
                        raise AssertionError("NameNode did not recognize stopped DataNode")
                    time.sleep(1)
            for path in Path(config["storage"]["replay_cache"]).glob("*.parquet"):
                assert path.resolve().is_relative_to(workspace.resolve())
                path.unlink()
            assert list(iter_events(pinned, "hdfs-acceptance")) == events
            report["read_with_one_datanode_down"] = True
            config["storage"]["replication_wait_seconds"] = 2
            failed = run_ingestion(config)
            assert all(e["status"] == "failed" for e in failed)
            assert "not replicated" in failed[0]["error"]
            assert json.loads(receipt_path(config).read_text())["status"] == "failed"
            try:
                hdfs_manifest(config)
                raise AssertionError("Failed publication allowed replay fallback")
            except ValueError as error:
                assert "not ready" in str(error)
            report["underreplicated_publish_rejected"] = True
            subprocess.run(compose + ["start", "datanode-2"], check=True, timeout=60)
            restarted = True
            config["storage"]["replication_wait_seconds"] = 60
            with WebHDFS(config) as client:
                client.wait_replicas(replay_entry["parquet_path"])
            report["replicas_restored_after_restart"] = True
        report["status"] = "passed"
    except Exception as error:
        import traceback
        traceback.print_exc()
        report.update(status="failed", error=str(error))
    finally:
        if not restarted:
            subprocess.run(compose + ["start", "datanode-2"], check=False, timeout=60)
        write_json(args.report, report)
    print(json.dumps({"status": report["status"], "report": args.report}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
