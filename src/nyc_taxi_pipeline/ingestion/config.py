"""Entrypoint configuration shared by ingestion workers."""

from importlib import import_module
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import yaml

from .common import aware_time


def load_config(path):
    path = Path(path).resolve()
    with path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError("Config must be a YAML mapping")
    config["_root"] = (path.parent / config.get("workspace_root", "..")).resolve()
    config.setdefault("business_timezone", "America/New_York")
    ZoneInfo(config["business_timezone"])
    if config.get("schema_version", 2) != 2:
        raise ValueError("Ingestion requires Kafka schema_version 2")
    return config


def dataset_ranges(config):
    ranges = {}
    for role in ("history", "replay"):
        settings = config["datasets"][role]
        start, end = (aware_time(settings[key]) for key in ("range_start", "range_end"))
        if start >= end:
            raise ValueError(f"{role}: range_start must be before range_end")
        ranges[role] = (start, end)
    if ranges["history"][1] > ranges["replay"][0]:
        raise ValueError("History must end at or before replay starts")
    return ranges


def hdfs_root(config):
    settings = config["storage"]
    for key, schemes in (("fs_uri", {"hdfs"}), ("webhdfs_url", {"http", "https"})):
        parsed = urlsplit(settings[key])
        if (parsed.scheme not in schemes or not parsed.hostname or parsed.username
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
            raise ValueError(f"storage.{key} must be a filesystem or HTTP origin")
    root = settings.get("root", "/nyc-taxi").rstrip("/")
    if not root.startswith("/") or not root or ".." in root.split("/"):
        raise ValueError("storage.root must be a dedicated absolute HDFS path")
    replication = settings.get("replication", 2)
    if type(replication) is not int or not 2 <= replication <= 10:
        raise ValueError("storage.replication must be an integer in 2..10")
    return root


def warehouse_repository(config, name):
    reference = config.get("warehouse", {}).get(name)
    if not reference:
        raise ValueError(f"Configure warehouse.{name} with Bảo's module:factory")
    module, separator, attribute = reference.partition(":")
    if not separator or not module or not attribute:
        raise ValueError(f"warehouse.{name} must be a module:factory reference")
    return getattr(import_module(module), attribute)(config)
