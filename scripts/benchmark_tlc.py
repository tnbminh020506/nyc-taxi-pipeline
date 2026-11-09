"""Real TLC volume/repeatability check; failures produce a report and exit 1.

Each large run COPYs all selected rows to Parquet. An independent, streamed
PyArrow scan hashes physical row identity, UTC pickup, zone and validity against
every exported row, so matching only a metadata count cannot pass this check.
"""

import argparse
import hashlib
import json
import logging
import os
import platform
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import duckdb
import numpy as np
import psutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nyc_taxi_pipeline.ingestion.common import load_config, write_json
from nyc_taxi_pipeline.ingestion.ingest import file_sha256, run_ingestion
from nyc_taxi_pipeline.ingestion.replay import iter_events
from nyc_taxi_pipeline.ingestion.tlc import connect, literal, sources_for_range

NULL = np.iinfo(np.int64).min


def measured(action, temporary):
    process = psutil.Process()
    stop = threading.Event()
    stats = {"baseline_rss_bytes": process.memory_info().rss, "peak_rss_bytes": 0, "peak_spill_bytes": 0}

    def sample():
        while not stop.is_set():
            stats["peak_rss_bytes"] = max(stats["peak_rss_bytes"], process.memory_info().rss)
            try:
                size = sum(p.stat().st_size for p in temporary.rglob("*") if p.is_file())
                stats["peak_spill_bytes"] = max(stats["peak_spill_bytes"], size)
            except FileNotFoundError:
                pass
            stop.wait(0.1)

    worker = threading.Thread(target=sample, daemon=True)
    started = time.monotonic()
    worker.start()
    try:
        result = action()
    finally:
        stop.set()
        worker.join()
    stats["wall_seconds"] = round(time.monotonic() - started, 3)
    return result, stats


def integers(array, null=NULL):
    return pc.fill_null(pc.cast(array, pa.int64()), null).to_numpy(zero_copy_only=False).copy()


def update_digest(digest, indices, pickup, zones, flags):
    packed = np.column_stack((indices, pickup, zones, flags)).astype("<i8", copy=False)
    digest.update(packed.tobytes())


def download_reference(files, cache):
    cache.mkdir(parents=True, exist_ok=True)
    retained = []
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1,
                      status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET",))))
        for item in files:
            path = cache / item["filename"]
            if not path.exists():
                partial = path.with_suffix(".parquet.tmp")
                with session.get(item["path"], stream=True, timeout=(10, 90)) as response:
                    response.raise_for_status()
                    if response.status_code != 200:
                        raise ValueError("Reference download requires a complete HTTP 200 response")
                    with partial.open("wb") as handle:
                        for block in response.iter_content(1024 * 1024):
                            handle.write(block)
                pq.ParquetFile(partial)  # Verify footer before publishing cache.
                os.replace(partial, path)
            retained.append({**item, "local_path": str(path), "bytes": path.stat().st_size,
                             "sha256": file_sha256(path), "rows": pq.ParquetFile(path).metadata.num_rows})
    return retained


def validate_reference(config, entry, retained):
    files, lower, upper = sources_for_range(config, entry["dataset_role"])
    expected_files = {item["filename"] for item in files}
    evidence = []
    with connect(config) as connection:
        for item in retained:
            if item["filename"] not in expected_files:
                continue
            digest = hashlib.sha256()
            selected_count, offset = 0, 0
            for batch in pq.ParquetFile(item["local_path"]).iter_batches(
                    batch_size=65536, columns=["tpep_pickup_datetime", "PULocationID"]):
                times, zones = batch.column(0), batch.column(1)
                mask = pc.fill_null(pc.and_(pc.greater_equal(times, pa.scalar(lower, type=times.type)),
                                           pc.less(times, pa.scalar(upper, type=times.type))), False)
                indices = np.arange(offset, offset + len(batch), dtype=np.int64)[mask.to_numpy(zero_copy_only=False)]
                offset += len(batch)
                times, zones = pc.filter(times, mask), pc.filter(zones, mask)
                times = pc.cast(times, pa.timestamp("us"), safe=False)
                # Arrow's two fold policies independently identify uncertainty.
                early = pc.assume_timezone(times, config["source_timezone"], ambiguous="earliest", nonexistent="earliest")
                late = pc.assume_timezone(times, config["source_timezone"], ambiguous="latest", nonexistent="latest")
                ambiguous = integers(early) != integers(late)
                utc = integers(early)
                utc[ambiguous] = NULL
                zone_values = integers(zones)
                invalid_zone = (zone_values <= 0) | (zone_values > np.iinfo(np.int32).max)
                zone_values[invalid_zone] = 0
                flags = ambiguous.astype(np.int64) + 2 * invalid_zone.astype(np.int64)
                update_digest(digest, indices, utc, zone_values, flags)
                selected_count += len(indices)
            actual = hashlib.sha256()
            query = f"""SELECT source_row, epoch_us(pickup_at)::BIGINT AS pickup,
                        pickup_zone_id::BIGINT AS zone,
                        (CASE WHEN contains(coalesce(validation_error,''),'pickup_time') THEN 1 ELSE 0 END
                         + CASE WHEN contains(coalesce(validation_error,''),'zone_id') THEN 2 ELSE 0 END)::BIGINT AS flags
                        FROM read_parquet({literal(entry['parquet_path'])})
                        WHERE starts_with(source_record_id, {literal(item['filename'] + '#')})
                        ORDER BY source_row"""
            actual_count = 0
            for batch in connection.execute(query).to_arrow_reader(65536):
                update_digest(actual, integers(batch.column(0)), integers(batch.column(1)),
                              integers(batch.column(2), 0), integers(batch.column(3)))
                actual_count += len(batch)
            if actual_count != selected_count or digest.hexdigest() != actual.hexdigest():
                raise AssertionError(f"Independent row/UTC/zone/validity mismatch: {item['filename']}")
            evidence.append({"filename": item["filename"], "rows": selected_count,
                             "row_content_sha256": digest.hexdigest(), "match": True})
        malformed = connection.execute(f"""SELECT count(*) FROM read_parquet({literal(entry['parquet_path'])})
            WHERE source_record_id NOT LIKE '%#' || source_row::VARCHAR""").fetchone()[0]
        if malformed or sum(row["rows"] for row in evidence) != entry["row_count"]:
            raise AssertionError("Unexpected/missing physical source identities")
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", type=int, default=2)
    parser.add_argument("--large-repeats", type=int, default=2)
    parser.add_argument("--small-repeats", type=int, default=5)
    parser.add_argument("--report", default="output/tlc-benchmark.json")
    parser.add_argument("--source-mode", choices=("remote", "local"), default="remote",
                        help="Explicit local mode validates cached original files; never reported as HTTPS ingestion")
    args = parser.parse_args()
    if not 2 <= args.months <= 12 or args.large_repeats < 1 or args.small_repeats < 0:
        parser.error("months must be 2..12, large-repeats >=1 and small-repeats >=0")
    if args.source_mode == "local" and args.small_repeats:
        parser.error("Use --small-repeats 0 with --source-mode local (the small test is an HTTPS acceptance test)")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "configs/demo.example.yaml")
    output = root / "output" / f"tlc-benchmark-{args.months}months"
    config["paths"] = {"raw": str(output / "raw"), "parquet": str(output / "parquet"),
                       "manifest": str(output / "manifest.json")}
    config["duckdb"]["temp_directory"] = str(output / "spill")
    temporary = output / "spill"
    config["replay"]["events_per_second"] = 0
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "platform": platform.platform(),
              "python": platform.python_version(), "duckdb": duckdb.__version__, "pyarrow": pa.__version__,
              "physical_ram_bytes": psutil.virtual_memory().total,
              "duckdb_settings": config["duckdb"], "small_runs": [], "large_runs": [], "status": "running",
              "source_mode": args.source_mode,
              "note": "Ingestion run success rate, not individual httpfs request success rate; RSS includes Python/Arrow, memory_limit is DuckDB's buffer budget."}
    try:
        if sys.platform == "win32":
            try:
                pc.assume_timezone(pa.array([datetime(2025, 1, 1)]), "America/New_York")
            except pa.ArrowInvalid:
                # Arrow's C++ library needs IANA text files on Windows; Python's
                # binary tzdata wheel alone is insufficient for this validator.
                from pyarrow.util import download_tzdata_on_windows
                logging.info("Installing IANA timezone database required by Arrow on Windows")
                download_tzdata_on_windows()
        reference_hashes = None
        for number in range(args.small_repeats):
            entries, stats = measured(lambda: run_ingestion(config), temporary)
            if any(e["status"] != "ready" for e in entries):
                raise AssertionError(entries)
            hashes = [e["parquet_sha256"] for e in entries[:2]]
            if reference_hashes is not None and hashes != reference_hashes:
                raise AssertionError("Small repeated scan changed output bytes")
            reference_hashes = hashes
            events = list(iter_events(config, "tlc-benchmark"))
            if len(events) != entries[1]["row_count"] - entries[1]["invalid_row_count"]:
                raise AssertionError("Replay dropped valid rows")
            if events != list(iter_events(config, "tlc-benchmark")):
                raise AssertionError("Replay IDs/order changed")
            report["small_runs"].append({"run": number + 1, "entries": entries, "metrics": stats,
                                         "replay_events": len(events), "repeat_replay_match": True})
            write_json(args.report, report)
        zone = ZoneInfo(config["source_timezone"])
        def boundary(month):
            return datetime(2025 + (month == 13), month if month < 13 else 1, 1, tzinfo=zone).isoformat()
        split = args.months // 2 + 1
        config["datasets"] = {"history": {"range_start": boundary(1), "range_end": boundary(split)},
                              "replay": {"range_start": boundary(split), "range_end": boundary(args.months + 1)}}
        files = [*sources_for_range(config, "history")[0], *sources_for_range(config, "replay")[0]]
        retained, transfer = measured(lambda: download_reference(files, root / "output/tlc-source-cache"), temporary)
        report["reference_sources"] = retained
        report["reference_download_metrics"] = transfer
        if args.source_mode == "local":
            config["source"]["files"] = {item["month"]: item["local_path"] for item in retained}
        hashes = None
        for number in range(args.large_repeats):
            entries, stats = measured(lambda: run_ingestion(config, ("history", "replay")), temporary)
            if any(e["status"] != "ready" for e in entries):
                raise AssertionError(entries)
            current_hashes = [e["parquet_sha256"] for e in entries]
            if hashes is not None and current_hashes != hashes:
                raise AssertionError("Large repeated scan changed output bytes")
            hashes = current_hashes
            run = {"run": number + 1, "entries": entries, "metrics": stats, "match": False}
            report["large_runs"].append(run)
            write_json(args.report, report)
            validation, validation_stats = measured(
                lambda: [validate_reference(config, e, retained) for e in entries], temporary)
            run.update(independent_validation=validation, validation_metrics=validation_stats, match=True)
            write_json(args.report, report)
        report["status"] = "passed"
    except Exception as error:
        logging.exception("Benchmark failed")
        report.update(status="failed", error=str(error))
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_json(args.report, report)
    print(json.dumps({"status": report["status"], "report": args.report,
                      "small_runs": len(report["small_runs"]), "large_runs": len(report["large_runs"])}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
