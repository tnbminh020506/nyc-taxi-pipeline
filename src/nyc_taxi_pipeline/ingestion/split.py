"""Plan chronological ~60/40 ranges from cached trips and whole 15-minute windows."""

from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from .common import aware_time, source_time
from .tlc import connect, literal


def balanced_ranges(config, files, range_start, range_end):
    """Use valid row counts, never random sampling or remote reads, to choose a cut.

    A timestamp tie/window stays together. The nearest complete window may be
    imperfect; callers must report the actual output ratio after ingestion.
    """
    paths = [str(path) for path in files]
    if not paths or any(path.startswith(("http://", "https://")) or not Path(path).is_file() for path in paths):
        raise ValueError("Split planning requires existing local Parquet files")
    zone_name = config.get("source_timezone", "America/New_York")
    zone = ZoneInfo(zone_name)
    start = aware_time(range_start).astimezone(zone).replace(tzinfo=None)
    end = aware_time(range_end).astimezone(zone).replace(tzinfo=None)
    if start >= end:
        raise ValueError("Split range must be increasing")
    sources = "[" + ",".join(literal(path) for path in paths) + "]"
    timezone = literal(zone_name)
    query = f"""
        WITH converted AS (
            SELECT tpep_pickup_datetime AS local_pickup,
                   timezone({timezone}, tpep_pickup_datetime) AS utc_pickup
            FROM read_parquet({sources}, union_by_name=true)
            WHERE tpep_pickup_datetime >= TIMESTAMP {literal(start.isoformat())}
              AND tpep_pickup_datetime < TIMESTAMP {literal(end.isoformat())}
              AND PULocationID > 0 AND PULocationID <= 2147483647
        )
        SELECT time_bucket(INTERVAL '15 minutes', local_pickup) AS window_start, count(*)
        FROM converted
        WHERE timezone({timezone}, utc_pickup) = local_pickup
          AND timezone({timezone}, utc_pickup - INTERVAL '1 hour') <> local_pickup
          AND timezone({timezone}, utc_pickup + INTERVAL '1 hour') <> local_pickup
        GROUP BY window_start ORDER BY window_start
    """
    with connect(config) as connection:
        windows = connection.execute(query).fetchall()
    total = sum(count for _, count in windows)
    cumulative = 0
    candidates = []
    for window, count in windows:
        cumulative += count
        boundary = window + timedelta(minutes=15)
        if start < boundary < end and 0 < cumulative < total:
            try:
                instant = source_time(boundary.isoformat(), zone_name)
            except ValueError:
                continue
            candidates.append((abs(cumulative / total - 0.6), instant, cumulative))
    if not candidates:
        raise ValueError("Need valid trips on both sides of a complete 15-minute split boundary")
    _, cut, history_count = min(candidates)
    split = cut.astimezone(zone).isoformat()
    return {
        "history": {"range_start": range_start, "range_end": split},
        "replay": {"range_start": split, "range_end": range_end},
    }, {"basis": "valid_records", "target_history_share": 0.6,
        "estimated_history_rows": history_count, "estimated_replay_rows": total - history_count,
        "estimated_history_share": history_count / total, "split_at": split,
        "note": "Nearest whole 15-minute boundary; actual ingestion counts are checked separately."}
