"""Publish one queued pickup from the warehouse's durable outbox."""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from .common import aware_time


@dataclass(frozen=True)
class KafkaAck:
    topic: str
    partition: int
    offset: int

    @classmethod
    def from_metadata(cls, metadata):
        return cls(metadata.topic, metadata.partition, metadata.offset)


@dataclass(frozen=True)
class PickupLease:
    lease_token: str
    event: dict


class PickupRepository(Protocol):
    def claim_pickup(self, *, now: datetime, lease_seconds: int) -> PickupLease | None: ...

    def acknowledge_pickup(self, outbox_id: str, lease_token: str, ack: KafkaAck) -> None: ...


def publish_pickup(repository: PickupRepository, publisher, clock, *, lease_seconds=90):
    now = clock.now()
    work = repository.claim_pickup(now=now, lease_seconds=lease_seconds)
    if work is None:
        return False
    event = work.event
    if (event.get("event_type") != "pickup_completed" or event.get("source_kind") != "dispatch"
            or aware_time(event["picked_up_at"]) > now):
        raise ValueError("Pickup outbox must contain a committed, due dispatch event")
    receipt = KafkaAck.from_metadata(publisher.publish(event))
    repository.acknowledge_pickup(event["outbox_id"], work.lease_token, receipt)
    return True
