"""Archive Kafka events, drain the warehouse journal, or pin a batch snapshot."""

import json
import time
from pathlib import Path

from kafka import KafkaConsumer

from _ingestion_cli import config_parser
from nyc_taxi_pipeline.ingestion.config import hdfs_root, load_config, warehouse_repository
from nyc_taxi_pipeline.ingestion.event_archive import archive_journal, archive_poll, publish_snapshot
from nyc_taxi_pipeline.ingestion.hdfs_store import WebHDFS


def main():
    parser = config_parser(__doc__)
    parser.add_argument("mode", choices=("events", "journal", "snapshot"))
    parser.add_argument("--once", action="store_true", help="Process at most one batch in events or journal mode")
    parser.add_argument("--spec", type=Path, help="JSON request for snapshot mode")
    args = parser.parse_args()
    if (args.mode == "snapshot") != (args.spec is not None):
        parser.error("snapshot mode requires --spec; other modes do not use it")
    config = load_config(args.config)
    root = hdfs_root(config)
    archive = config.get("archive", {})
    limit = archive.get("max_records", 500)
    if type(limit) is not int or not 1 <= limit <= 5000:
        raise ValueError("archive.max_records must be an integer in 1..5000")
    with WebHDFS(config) as store:
        if args.mode == "snapshot":
            spec = json.loads(args.spec.read_text(encoding="utf-8"))
            print(json.dumps(publish_snapshot(store, root, spec), ensure_ascii=True), flush=True)
            return
        configured = config.get("warehouse", {}).get("archive_repository")
        repository = warehouse_repository(config, "archive_repository") if configured or args.mode == "journal" else None
        if args.mode == "events":
            kafka = config["kafka"]
            with KafkaConsumer(kafka.get("topic", "taxi-events"), bootstrap_servers=kafka["bootstrap_servers"],
                               group_id=archive.get("group_id", "taxi-event-archive-v2"),
                               enable_auto_commit=False, auto_offset_reset="earliest") as consumer:
                while True:
                    for receipt in archive_poll(consumer, store, root, repository=repository, max_records=limit):
                        print(json.dumps(receipt, ensure_ascii=True), flush=True)
                    if args.once:
                        return
        else:
            lease = archive.get("journal_lease_seconds", 90)
            if type(lease) is not int or lease <= 0:
                raise ValueError("archive.journal_lease_seconds must be positive")
            while True:
                receipts = archive_journal(repository, store, root, limit=limit, lease_seconds=lease)
                for receipt in receipts:
                    print(json.dumps(receipt, ensure_ascii=True), flush=True)
                if args.once:
                    return
                if not receipts:
                    time.sleep(1)


if __name__ == "__main__":
    main()
