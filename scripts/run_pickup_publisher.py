"""Run the pickup publisher over the warehouse's committed outbox."""

from _ingestion_cli import run_scheduled_worker
from nyc_taxi_pipeline.ingestion.pickup_publisher import publish_pickup


if __name__ == "__main__":
    run_scheduled_worker(publish_pickup, __doc__)
