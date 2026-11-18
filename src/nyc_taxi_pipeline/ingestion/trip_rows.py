"""Normalize raw source fields to the Parquet handoff schema."""

import math

import pyarrow as pa

from .common import source_time

TRIP_SCHEMA = pa.schema([
    ("source_record_id", pa.string()),
    ("source_file", pa.string()),
    ("pickup_at", pa.timestamp("us", tz="UTC")),
    ("dropoff_at", pa.timestamp("us", tz="UTC")),
    ("pickup_zone_id", pa.int32()),
    ("dropoff_zone_id", pa.int32()),
    ("pickup_lat", pa.float64()),
    ("pickup_lon", pa.float64()),
    ("dropoff_lat", pa.float64()),
    ("dropoff_lon", pa.float64()),
    ("source_duration_seconds", pa.float64()),
    ("validation_error", pa.string()),
    ("matching_error", pa.string()),
])


def _timestamp(value, zone):
    try:
        return source_time(value, zone)
    except (ValueError, TypeError, OverflowError):
        return None


def zone_id(value):
    if isinstance(value, bool) or not str(value).isascii() or not str(value).isdigit():
        return None
    number = int(value)
    return number if 0 < number <= 2147483647 else None


def point(lat, lon):
    if isinstance(lat, bool) or isinstance(lon, bool):
        return None, None
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError, OverflowError):
        return None, None
    return (lat, lon) if -90 <= lat <= 90 and -180 <= lon <= 180 else (None, None)


def normalize_trip(row, zone, source_uri):
    pickup, dropoff = (_timestamp(row.get(key), zone) for key in ("pickup_at", "dropoff_at"))
    pu_zone, do_zone = (zone_id(row.get(key)) for key in ("pickup_zone_id", "dropoff_zone_id"))
    errors = [key for key, value in (("invalid_pickup_time", pickup), ("invalid_pickup_zone", pu_zone))
              if value is None]
    duration = (dropoff - pickup).total_seconds() if pickup is not None and dropoff is not None else None
    if row.get("source_duration_seconds") is not None:
        try:
            duration = float(row["source_duration_seconds"])
        except (TypeError, ValueError, OverflowError):
            duration = None
    if duration is not None and (not math.isfinite(duration) or duration < 0):
        duration = None
    pu_lat, pu_lon = point(row.get("pickup_lat"), row.get("pickup_lon"))
    do_lat, do_lon = point(row.get("dropoff_lat"), row.get("dropoff_lon"))
    return {
        "source_record_id": str(row["source_record_id"]), "source_file": source_uri,
        "pickup_at": pickup, "dropoff_at": dropoff, "pickup_zone_id": pu_zone, "dropoff_zone_id": do_zone,
        "pickup_lat": pu_lat, "pickup_lon": pu_lon, "dropoff_lat": do_lat, "dropoff_lon": do_lon,
        "source_duration_seconds": duration, "validation_error": ";".join(errors) or None,
        "matching_error": ";".join(key for key, value in
                                   (("missing_dropoff_zone", do_zone), ("invalid_duration", duration))
                                   if value is None) or None,
    }
