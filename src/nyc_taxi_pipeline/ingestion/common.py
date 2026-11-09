"""Configuration, atomic files and explicit source-time conversion for ingestion."""

import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yaml


def atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def write_json(path, value):
    atomic_write(path, (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


def aware_time(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Timestamp must include an offset: {value}")
    return parsed.astimezone(timezone.utc)


def source_time(value, zone_name):
    """Reject ambiguous/nonexistent local times instead of silently choosing a DST fold."""
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc)
    zone = ZoneInfo(zone_name)
    candidates = set()
    for fold in (0, 1):
        candidate = parsed.replace(tzinfo=zone, fold=fold).astimezone(timezone.utc)
        if candidate.astimezone(zone).replace(tzinfo=None) == parsed:
            candidates.add(candidate)
    if len(candidates) != 1:
        raise ValueError(f"Ambiguous or nonexistent local time in {zone_name}: {value}")
    return candidates.pop()


def resolve_path(config, value):
    path = Path(value)
    return path.resolve() if path.is_absolute() else (config["_root"] / path).resolve()


def load_config(path):
    path = Path(path).resolve()
    with path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError("Config must be a YAML mapping")
    config["_root"] = (path.parent / config.get("workspace_root", "..")).resolve()
    ZoneInfo(config.get("source_timezone", "America/New_York"))
    ranges = []
    for role in ("history", "replay"):
        section = config["datasets"][role]
        start, end = aware_time(section["range_start"]), aware_time(section["range_end"])
        if start >= end:
            raise ValueError(f"{role}: range_start must be before range_end")
        ranges.append((start, end))
    if max(ranges[0][0], ranges[1][0]) < min(ranges[0][1], ranges[1][1]):
        raise ValueError("history and replay must not overlap")
    api = config.get("api", {})
    if api.get("version", 3) not in (2, 3):
        raise ValueError("api.version must be 2 or 3")
    for name, default in (("page_size", 1000), ("max_pages", 100), ("timeout_seconds", 30)):
        value = api.get(name, default)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"api.{name} must be a positive integer")
    retries = api.get("max_retries", 3)
    if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= 10:
        raise ValueError("api.max_retries must be an integer from 0 to 10")
    for name, default in (("retry_backoff_seconds", 0.5), ("retry_backoff_max_seconds", 20)):
        value = float(api.get(name, default))
        if not math.isfinite(value) or not 0 <= value <= 120:
            raise ValueError(f"api.{name} must be finite and between 0 and 120")
        api[name] = value
    rate = float(config.get("replay", {}).get("events_per_second", 10))
    if not math.isfinite(rate) or rate < 0:
        raise ValueError("replay.events_per_second must be finite and nonnegative")
    return config
