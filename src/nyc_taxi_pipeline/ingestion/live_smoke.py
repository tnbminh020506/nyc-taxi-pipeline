"""Opt-in acceptance against real API data, independently checked with COUNT(*)."""

from datetime import datetime, timezone
from pathlib import Path
import copy
import time

import pyarrow.parquet as pq

from .common import atomic_write, resolve_path, write_json
from .ingest import api_page, http_session, query_for_range, run_ingestion
from .replay import run_replay


def run_live_smoke(config, report_path="output/api-smoke/report.json"):
    config = copy.deepcopy(config)
    # A reliability/acceptance run must contact the provider rather than become
    # green by reading pages cached by a previous failed measurement.
    config["api"]["resume"] = False
    metrics = []
    config["api"]["_request_metrics"] = metrics
    started = time.monotonic()
    report_path = resolve_path(config, report_path)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "status": "failed",
              "dataset_id": config["api"]["dataset_id"], "api_version": config["api"].get("version", 3),
              "endpoint": config["api"]["endpoint"], "checks": [], "datasets": []}
    try:
        for role in ("history", "replay"):
            if "fixture_pages" in config["datasets"][role]:
                raise ValueError("Live smoke refuses fixture input")
        if not config["zone_lookup"]["source"].startswith("https://"):
            raise ValueError("Live smoke requires an HTTPS lookup source")
        results = run_ingestion(config)
        report["datasets"] = results
        if any(entry["status"] != "ready" for entry in results):
            raise ValueError("Real API ingestion did not complete; inspect datasets[].error")
        with http_session(config.get("api")) as session:
            for entry in results:
                role = entry["dataset_role"]
                if role == "zone_lookup":
                    continue
                if entry.get("input_mode") != "api" or not entry.get("coverage_complete"):
                    raise ValueError(f"{role}: expected a complete API download")
                parquet_rows = pq.ParquetFile(entry["parquet_path"]).metadata.num_rows
                source_ids = pq.read_table(entry["parquet_path"], columns=["source_record_id"])["source_record_id"].to_pylist()
                if any(not value for value in source_ids) or len(source_ids) != len(set(source_ids)):
                    raise ValueError(f"{role}: missing or duplicate API row IDs across pages")
                query = query_for_range(config["datasets"][role], config["source_timezone"])
                count_query = "SELECT count(*) AS total " + query[query.index("WHERE "):query.index("ORDER BY")].strip()
                content, count_rows = api_page(session, config["api"], count_query, 1)
                atomic_write(Path(entry["raw_path"]) / "count.json", content)
                total = int(count_rows[0]["total"])
                if not total or total != entry["row_count"] or total != parquet_rows:
                    raise ValueError(f"{role}: API count={total}, downloaded={entry['row_count']}, parquet={parquet_rows}")
                report["checks"].append({"role": role, "api_count": total, "parquet_rows": parquet_rows,
                                         "count_query": count_query, "status": "passed"})
        stats = run_replay(config, dry_run=True, output=str(report_path.parent / "replay-events.jsonl"))
        replay = next(entry for entry in results if entry["dataset_role"] == "replay")
        expected = replay["row_count"] - replay["invalid_row_count"]
        if stats["events_prepared"] != expected or expected == 0:
            raise ValueError("Replay event count does not match valid real input rows")
        report["replay"] = stats
        report["status"] = "passed"
    except Exception as error:
        report["error"] = str(error)
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report["seconds"] = round(time.monotonic() - started, 3)
    report["http_requests"] = metrics
    write_json(report_path, report)
    return report
