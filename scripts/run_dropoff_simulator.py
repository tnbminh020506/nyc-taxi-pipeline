"""Run the dropoff simulator over the warehouse's durable schedule."""

from _ingestion_cli import run_scheduled_worker
from nyc_taxi_pipeline.ingestion.dropoff_simulator import publish_dropoff


if __name__ == "__main__":
    run_scheduled_worker(publish_dropoff, __doc__)
