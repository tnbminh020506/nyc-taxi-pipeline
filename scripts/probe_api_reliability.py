"""Small, serial, uncached-by-client probes; never substitute fixtures for failures."""

import argparse
import hashlib
import json
import os
import random
import statistics
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from nyc_taxi_pipeline.ingestion.common import load_config, resolve_path, write_json
from nyc_taxi_pipeline.ingestion.ingest import query_for_range


def split_params(query, size, offset=0):
    selection, predicate = query.removeprefix("SELECT ").split(" WHERE ", 1)
    if " ORDER BY " in predicate:
        predicate, ordering = predicate.rsplit(" ORDER BY ", 1)
    else:
        ordering = None
    params = {"$select": selection, "$where": predicate, "$limit": size, "$offset": offset}
    if ordering:
        params["$order"] = ordering
    return params


def probe(session, method, endpoint, label, **kwargs):
    started = time.monotonic()
    result = {"label": label, "method": method, "at": datetime.now(timezone.utc).isoformat()}
    try:
        response = session.request(method, endpoint, timeout=(8, 25), **kwargs)
        result.update(status=response.status_code, content_type=response.headers.get("Content-Type"),
                      bytes=len(response.content), server=response.headers.get("Server"),
                      retry_after=response.headers.get("Retry-After"),
                      request_id=response.headers.get("X-Socrata-RequestId"))
        try:
            body = response.json()
        except ValueError:
            body = None
        result["success"] = response.ok and isinstance(body, list)
        if result["success"]:
            result["row_count"] = len(body)
            result["digest"] = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
            if body and "total" in body[0]:
                result["total"] = int(body[0]["total"])
        else:
            result["body_preview"] = response.text[:700]
    except requests.RequestException as error:
        result.update(success=False, error=type(error).__name__)
    result["seconds"] = round(time.monotonic() - started, 3)
    print(json.dumps(result), flush=True)
    return result


def run(config, rounds, output):
    target = resolve_path(config, output)
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "rounds": rounds,
              "dataset": config["api"]["dataset_id"], "requests": [], "summary": {}}
    dataset = config["api"]["dataset_id"]
    endpoint = f"https://data.cityofnewyork.us/resource/{dataset}.json"
    token = os.environ.get(config["api"].get("app_token_env", "SOCRATA_APP_TOKEN"))
    headers = {"X-App-Token": token} if token else {}
    report["token_present"] = bool(token)
    with requests.Session() as session:
        v3 = probe(session, "POST", f"https://data.cityofnewyork.us/api/v3/views/{dataset}/query.json",
                   "v3", headers=headers, json={"query": "SELECT tpep_pickup_datetime, pulocationid",
                   "page": {"pageNumber": 1, "pageSize": 1}, "includeSynthetic": False})
        report["v3"] = v3
        write_json(target, report)
        jobs = []
        for role in ("history", "replay"):
            query = query_for_range(config["datasets"][role], config["source_timezone"])
            count = "SELECT count(*) AS total " + query[query.index("WHERE "):query.index("ORDER BY")].strip()
            for kind, statement in (("page", query), ("count", count)):
                for style in ("query", "parameters"):
                    jobs.append((f"{style}/{role}/{kind}", statement, style))
        for round_number in range(1, rounds + 1):
            shuffled = jobs[:]
            random.Random(round_number).shuffle(shuffled)
            for label, query, style in shuffled:
                params = ({"$query": f"{query} LIMIT 20 OFFSET 0"} if style == "query"
                          else split_params(query, 20))
                result = probe(session, "GET", endpoint, label, params=params, headers=headers)
                result["round"] = round_number
                report["requests"].append(result)
                write_json(target, report)
                time.sleep(1)
        for style in ("query", "parameters"):
            records = [r for r in report["requests"] if r["label"].startswith(style + "/")]
            elapsed = sorted(r["seconds"] for r in records)
            report["summary"][style] = {
                "attempts": len(records), "successes": sum(r["success"] for r in records),
                "statuses": dict(Counter(str(r.get("status", r.get("error"))) for r in records)),
                "median_seconds": round(statistics.median(elapsed), 3),
                "p95_seconds": elapsed[min(len(elapsed) - 1, int(len(elapsed) * .95))],
            }
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    write_json(target, report)
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="configs/api-smoke.yaml")
    parser.add_argument("--rounds", type=int, choices=range(1, 11), default=5)
    parser.add_argument("--output", default="output/api-reliability/probe.json")
    args = parser.parse_args()
    run(load_config(args.config), args.rounds, args.output)
