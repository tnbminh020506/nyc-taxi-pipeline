"""Deterministic WGS84 simulation inside the published zone polygons."""

import json
import random
from collections import defaultdict

from shapely import get_parts, make_valid, union_all
from shapely.geometry import Point, shape

from .common import stable_id
from .trip_rows import zone_id


def load_boundaries(path):
    data = json.loads(path.read_bytes())
    if data.get("type") != "FeatureCollection":
        raise ValueError("Zone boundaries must be a GeoJSON FeatureCollection")
    crs = data.get("crs")
    if crs and crs.get("properties", {}).get("name", "").upper() not in {
        "EPSG:4326", "URN:OGC:DEF:CRS:EPSG::4326", "URN:OGC:DEF:CRS:OGC:1.3:CRS84", "OGC:CRS84",
    }:
        raise ValueError("Zone boundaries must use WGS84 longitude/latitude")
    groups = defaultdict(list)
    for feature in data["features"]:
        properties = {key.lower(): value for key, value in feature["properties"].items()}
        identifier = zone_id(properties.get("locationid"))
        if identifier is None or feature["geometry"]["type"] not in ("Polygon", "MultiPolygon"):
            raise ValueError("Each zone feature must have LocationID and polygon geometry")
        for part in get_parts(make_valid(shape(feature["geometry"]))):
            if part.geom_type in ("Polygon", "MultiPolygon") and part.area > 0:
                west, south, east, north = part.bounds
                if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
                    raise ValueError(f"Zone {identifier} coordinates are outside WGS84 bounds")
                groups[identifier].append(part)
        if not groups[identifier]:
            raise ValueError(f"Zone {identifier} has no polygon area")
    if not groups:
        raise ValueError("No zone polygons found")
    return {identifier: union_all(parts).normalize() for identifier, parts in groups.items()}


def simulated_point(polygons, identifier, seed, source_id, source_record_id, leg):
    polygon = polygons.get(identifier)
    if polygon is None:
        return None, None
    rng = random.Random(int(stable_id(seed, source_id, source_record_id, leg), 16))
    west, south, east, north = polygon.bounds
    for _ in range(128):
        lon, lat = rng.uniform(west, east), rng.uniform(south, north)
        if polygon.contains(Point(lon, lat)):
            return lat, lon
    point = polygon.representative_point()
    return point.y, point.x
