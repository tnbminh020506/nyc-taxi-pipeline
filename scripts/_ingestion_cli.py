"""Configuration and logging for the ingestion entrypoints."""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def config_parser(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", required=True, type=Path)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)
    return parser


def run_scheduled_worker(worker, description):
    from nyc_taxi_pipeline.ingestion.config import load_config, warehouse_repository
    from nyc_taxi_pipeline.ingestion.kafka_publisher import TaxiPublisher
    from nyc_taxi_pipeline.shared.clock import SimulationClock

    parser = config_parser(description)
    parser.add_argument("--once", action="store_true", help="Claim and process at most one item")
    args = parser.parse_args()
    config = load_config(args.config)
    settings = config.get("schedule_worker", {})
    lease_seconds = settings.get("lease_seconds", 90)
    idle_seconds = settings.get("idle_seconds", 0.5)
    if type(lease_seconds) is not int or lease_seconds <= 0 or not 0 < idle_seconds <= 60:
        raise ValueError("Invalid schedule_worker lease_seconds or idle_seconds")
    clock = SimulationClock.from_config(config)
    repository = warehouse_repository(config, "schedule_repository")
    with TaxiPublisher.from_config(config) as publisher:
        while True:
            worked = worker(repository, publisher, clock, lease_seconds=lease_seconds)
            if args.once:
                return
            if not worked:
                time.sleep(idle_seconds)
