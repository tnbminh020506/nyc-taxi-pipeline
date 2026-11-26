"""Validate the independently versioned lookup and polygon inputs."""

import csv
import shutil
from urllib.parse import urlsplit

import requests

from .common import atomic_write, resolve_path
from .coordinates import load_boundaries
from .source_api import fetch_bytes
from .trip_rows import zone_id


def stage_zone_source(config, role, stage):
    section = config["zones"][role]
    source = section["source"]
    target = stage / ("lookup.csv" if role == "zone_lookup" else "boundaries.geojson")
    if urlsplit(source).scheme in ("https", "http"):
        with requests.Session() as session:
            atomic_write(target, fetch_bytes(session, "GET", source, section))
    else:
        shutil.copyfile(resolve_path(config, source), target)
    if role == "zone_boundaries":
        identifiers = set(load_boundaries(target))
    else:
        identifiers = set()
        with target.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not {"LocationID", "Zone", "Borough"}.issubset(reader.fieldnames or []):
                raise ValueError("Zone Lookup needs LocationID, Zone and Borough")
            for row in reader:
                identifier = zone_id(row["LocationID"])
                if identifier is None or identifier in identifiers or not row["Zone"] or not row["Borough"]:
                    raise ValueError("Zone Lookup contains invalid or repeated LocationID/labels")
                identifiers.add(identifier)
        if not identifiers:
            raise ValueError("Zone Lookup is empty")
    return target, sorted(identifiers)
