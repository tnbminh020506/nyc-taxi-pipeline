"""Configuration, atomic files and explicit source-time conversion for ingestion."""

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


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


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_bytes(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode("utf-8")


def stable_id(*parts):
    return hashlib.sha256(json_bytes(parts)).hexdigest()
