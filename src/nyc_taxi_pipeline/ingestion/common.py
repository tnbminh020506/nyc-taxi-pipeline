"""Configuration, atomic files and explicit source-time conversion for ingestion."""

import json
import math
import os
import re
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


def dataset_source_id(config):
    if config.get("source", {}).get("kind", "api") == "tlc_parquet":
        return config["source"].get("source_id", "tlc-yellow-taxi")
    return config["api"]["dataset_id"]


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
    kind = config.get("source", {}).get("kind", "api")
    if kind not in ("api", "tlc_parquet"):
        raise ValueError("source.kind must be api or tlc_parquet")
    storage = config.get("storage", {})
    if storage.get("kind", "local") not in ("local", "hdfs"):
        raise ValueError("storage.kind must be local or hdfs")
    if storage.get("kind") == "hdfs":
        from urllib.parse import urlsplit
        fs_uri = urlsplit(storage.get("fs_uri", ""))
        endpoint = urlsplit(storage.get("webhdfs_url", ""))
        if fs_uri.scheme != "hdfs" or not fs_uri.netloc or fs_uri.path not in ("", "/"):
            raise ValueError("storage.fs_uri must be hdfs://namenode:port")
        if endpoint.scheme not in ("http", "https") or not endpoint.netloc or endpoint.path not in ("", "/"):
            raise ValueError("storage.webhdfs_url must be an HTTP(S) NameNode origin")
        replication = storage.get("replication", 2)
        if isinstance(replication, bool) or not isinstance(replication, int) or not 2 <= replication <= 10:
            raise ValueError("storage.replication must be 2..10 for distributed publication")
        root = storage.get("root", "/nyc-taxi")
        if not isinstance(root, str) or not root.startswith("/") or ".." in root.split("/") or root == "/":
            raise ValueError("storage.root must be a dedicated absolute HDFS directory")
        for name, default in (("timeout_seconds", 60), ("replication_wait_seconds", 60)):
            value = storage.get(name, default)
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 600:
                raise ValueError(f"storage.{name} must be an integer from 1 to 600")
    if kind == "tlc_parquet":
        if config.get("source_timezone", "America/New_York") != "America/New_York":
            raise ValueError("TLC timestamps must use source_timezone America/New_York")
        settings = config.get("duckdb", {})
        for key, default in (("memory_limit", "512MB"), ("max_temp_directory_size", "8GB")):
            if not re.fullmatch(r"[1-9][0-9]*(?:MB|GB)", str(settings.get(key, default))):
                raise ValueError(f"duckdb.{key} must be a positive integer in MB or GB")
        threads = settings.get("threads", 2)
        if isinstance(threads, bool) or not isinstance(threads, int) or not 1 <= threads <= 32:
            raise ValueError("duckdb.threads must be an integer from 1 to 32")
    api = config.get("api", {})
    if api.get("version", 3) not in (2, 3):
        raise ValueError("api.version must be 2 or 3")
    if not isinstance(api.get("resume", True), bool):
        raise ValueError("api.resume must be true or false")
    if not isinstance(api.get("connection_close", False), bool):
        raise ValueError("api.connection_close must be true or false")
    for name, default in (("page_size", 1000), ("max_pages", 100), ("timeout_seconds", 30)):
        value = api.get(name, default)
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"api.{name} must be a positive integer")
    retries = api.get("max_retries", 3)
    if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= 20:
        raise ValueError("api.max_retries must be an integer from 0 to 20")
    budget = float(api.get("request_budget_seconds", 120))
    if not 0 < budget <= 600:
        raise ValueError("api.request_budget_seconds must be positive and at most 600")
    api["request_budget_seconds"] = budget
    for name, default in (("retry_backoff_seconds", 0.5), ("retry_backoff_max_seconds", 20),
                          ("min_retry_delay_seconds", 0), ("retry_jitter_seconds", 0),
                          ("min_request_interval_seconds", 0)):
        value = float(api.get(name, default))
        if not math.isfinite(value) or not 0 <= value <= 120:
            raise ValueError(f"api.{name} must be finite and between 0 and 120")
        api[name] = value
    rate = float(config.get("replay", {}).get("events_per_second", 10))
    if not math.isfinite(rate) or rate < 0:
        raise ValueError("replay.events_per_second must be finite and nonnegative")
    return config
