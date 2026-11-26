"""Collect and publish a source manifest to HDFS."""

import json
from pathlib import Path

from _ingestion_cli import config_parser
from nyc_taxi_pipeline.ingestion.common import resolve_path
from nyc_taxi_pipeline.ingestion.config import load_config
from nyc_taxi_pipeline.ingestion.manifest import ROLES, publish_datasets


def main():
    parser = config_parser(__doc__)
    parser.add_argument("--role", choices=(*ROLES, "all"), action="append", help="Repeat to select roles; default is all")
    parser.add_argument("--no-resume", action="store_true", help="Disable partial-page resume for manifest v1")
    parser.add_argument("--status-file", type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    if "paths" in config and "zones" not in config:
        from nyc_taxi_pipeline.ingestion.common import load_config as load_v1_config
        from nyc_taxi_pipeline.ingestion.ingest import run_ingestion

        config = load_v1_config(args.config)
        if args.no_resume:
            config.setdefault("api", {})["resume"] = False
        roles = args.role or ["all"]
        roles = ("history", "replay", "zone_lookup") if "all" in roles else roles
        results = run_ingestion(config, roles)
        print(json.dumps(results, ensure_ascii=True))
        return 0 if all(item["status"] == "ready" for item in results) else 1
    status = args.status_file or resolve_path(config, "output/ingestion-publication.json")
    roles = args.role or ["all"]
    result = publish_datasets(config, ROLES if "all" in roles else roles, status_path=status)
    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    raise SystemExit(main())
