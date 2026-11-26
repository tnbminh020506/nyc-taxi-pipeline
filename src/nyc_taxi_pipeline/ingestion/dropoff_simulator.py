"""Publish scheduled dropoffs after warehouse-recorded pickup acknowledgments."""

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from .common import aware_time
from .kafka_publisher import event_key
from .pickup_publisher import KafkaAck

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DropoffLease:
    lease_token: str
    event: dict
    trip_started_at: datetime
    due_at: datetime
    source_duration_seconds: float
    pickup_acked: bool


class DropoffRepository(Protocol):
    def claim_dropoff(self, *, now: datetime, lease_seconds: int) -> DropoffLease | None: ...

    def acknowledge_dropoff(self, scheduled_event_id: str, lease_token: str, ack: KafkaAck) -> None: ...

    def quarantine_dropoff(self, scheduled_event_id: str, lease_token: str, reason: str) -> None: ...


def publish_dropoff(repository: DropoffRepository, publisher, clock, *, lease_seconds=90):
    now = clock.now()
    work = repository.claim_dropoff(now=now, lease_seconds=lease_seconds)
    if work is None:
        return False
    if work.pickup_acked is not True or aware_time(work.due_at) > now:
        raise ValueError("Dropoff repository claimed work before pickup ack or due time")
    scheduled_id = work.event["scheduled_event_id"]
    try:
        duration = work.source_duration_seconds
        if type(duration) not in (int, float) or not math.isfinite(duration) or duration < 0:
            raise ValueError("invalid_source_duration")
        due = aware_time(work.due_at)
        if due != aware_time(work.trip_started_at) + timedelta(seconds=duration):
            raise ValueError("due_at_differs_from_scheduled_duration")
        event = {
            **work.event, "event_type": "dropoff_completed", "source_kind": "simulator",
            "schema_version": 2, "event_at": due.isoformat(), "completed_at": due.isoformat(),
        }
        event_key(event)
    except (ValueError, KeyError, OverflowError) as error:
        repository.quarantine_dropoff(scheduled_id, work.lease_token, str(error))
        LOG.warning("Quarantined dropoff %s: %s", scheduled_id, error)
        return True
    receipt = KafkaAck.from_metadata(publisher.publish(event))
    repository.acknowledge_dropoff(scheduled_id, work.lease_token, receipt)
    return True
