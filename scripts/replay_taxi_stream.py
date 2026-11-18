"""Read a pinned HDFS source manifest and replay v2 requests."""

import json
from pathlib import Path
from tempfile import NamedTemporaryFile

from _ingestion_cli import config_parser
from nyc_taxi_pipeline.ingestion.config import hdfs_root, load_config
from nyc_taxi_pipeline.ingestion.hdfs_store import WebHDFS
from nyc_taxi_pipeline.ingestion.kafka_publisher import TaxiPublisher, event_key
from nyc_taxi_pipeline.ingestion.replay import iter_requests, publish_replay
from nyc_taxi_pipeline.shared.clock import SimulationClock


def main():
    parser = config_parser(__doc__)
    parser.add_argument("--manifest", required=True, help="Immutable HDFS manifest URI")
    parser.add_argument("--replay-id", required=True)
    parser.add_argument("--scope-id", required=True)
    parser.add_argument("--dry-run", action="store_true", help="Write JSONL without Kafka or pacing")
    parser.add_argument("--output", type=Path, help="Output file for --dry-run")
    args = parser.parse_args()
    if args.dry_run != (args.output is not None):
        parser.error("--dry-run and --output must be used together")
    config = load_config(args.config)
    hdfs_root(config)
    with WebHDFS(config) as store:
        seed = config.get("replay", {}).get("coordinate_seed", 0)
        events = iter_requests(store, args.manifest, args.replay_id, args.scope_id, seed=seed)
        if args.dry_run:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            # Do not leave a complete-looking file when a manifest fails mid-replay.
            temporary = None
            count = 0
            try:
                with NamedTemporaryFile("w", encoding="utf-8", dir=args.output.parent, delete=False) as handle:
                    temporary = Path(handle.name)
                    for event in events:
                        event_key(event)
                        handle.write(json.dumps(event, allow_nan=False) + "\n")
                        count += 1
                temporary.replace(args.output)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        else:
            clock = SimulationClock.from_config(config)
            with TaxiPublisher.from_config(config) as publisher:
                count = publish_replay(events, publisher, clock)
    print(json.dumps({"status": "ready" if args.dry_run else "acknowledged", "request_count": count}))


if __name__ == "__main__":
    main()
