"""Validate the ingestion boundary and wait for Kafka delivery acknowledgments."""

import math

from kafka import KafkaProducer

from .common import aware_time, json_bytes
from .trip_rows import zone_id

EVENT_TIMES = {
    "ride_requested": "requested_at", "pickup_completed": "picked_up_at", "dropoff_completed": "completed_at",
}


def _require_ids(event, *fields):
    for key in fields:
        if not isinstance(event.get(key), str) or not event[key].strip():
            raise ValueError(f"Event requires nonempty {key}")


def _require_point(event, leg, *, zone_optional=False):
    zone, lat, lon = (event[f"{leg}_{name}"] for name in ("zone_id", "lat", "lon"))
    if not (zone_optional and zone is None) and (type(zone) is not int or zone_id(zone) is None):
        raise ValueError(f"Invalid {leg} zone")
    if lat is None and lon is None:
        return
    if (type(lat) not in (int, float) or type(lon) not in (int, float)
            or not -90 <= lat <= 90 or not -180 <= lon <= 180):
        raise ValueError(f"Invalid {leg} WGS84 coordinates")


def event_key(event):
    _require_ids(event, "event_id", "replay_id", "scope_id", "event_type")
    kind = event["event_type"]
    if (event.get("schema_version") != 2 or kind not in EVENT_TIMES
            or event.get("source_kind") not in ("nyc_replay", "simulator", "dispatch")):
        raise ValueError("Expected a v2 taxi event with a supported type and source_kind")
    if aware_time(event["event_at"]) != aware_time(event[EVENT_TIMES[kind]]):
        raise ValueError("event_at must equal the business timestamp")
    if kind == "ride_requested":
        _require_ids(event, "request_id")
        _require_point(event, "pickup")
        _require_point(event, "dropoff", zone_optional=True)
        duration = event.get("source_duration_seconds")
        if duration is not None and (type(duration) not in (int, float) or not math.isfinite(duration) or duration < 0):
            raise ValueError("Invalid source_duration_seconds")
        if type(event.get("coordinates_simulated")) is not bool:
            raise ValueError("Request requires coordinates_simulated")
        return json_bytes([event["replay_id"], event["request_id"]])
    _require_ids(event, "trip_id")
    if kind == "pickup_completed":
        _require_ids(event, "outbox_id")
        _require_point(event, "pickup")
        members = event.get("request_ids")
        if (not isinstance(members, list) or len(members) not in (1, 2)
                or any(not isinstance(member, str) or not member.strip() for member in members)
                or len(set(members)) != len(members)
                or event.get("trip_mode") != ("solo" if len(members) == 1 else "shared")):
            raise ValueError("Pickup requires the unique members of one solo or shared trip")
    else:
        _require_ids(event, "scheduled_event_id", "request_id")
        _require_point(event, "dropoff")
    return json_bytes([event["replay_id"], event["trip_id"]])


class TaxiPublisher:
    def __init__(self, producer, topic, ack_timeout_seconds=30):
        self.producer, self.topic, self.ack_timeout_seconds = producer, topic, ack_timeout_seconds

    @classmethod
    def from_config(cls, config):
        settings = config["kafka"]
        timeout = float(settings.get("ack_timeout_seconds", 30))
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Kafka ack timeout must be positive")
        producer = KafkaProducer(
            bootstrap_servers=settings["bootstrap_servers"], acks="all", enable_idempotence=True,
            max_block_ms=10000,
        )
        return cls(producer, settings.get("topic", "taxi-events"), timeout)

    def publish(self, event):
        key = event_key(event)
        payload = json_bytes(event)
        return self.producer.send(self.topic, key=key, value=payload).get(timeout=self.ack_timeout_seconds)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.producer.close(timeout=5)
