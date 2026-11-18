"""Collect and publish a source manifest to HDFS."""

import json
from pathlib import Path

from _ingestion_cli import config_parser
from nyc_taxi_pipeline.ingestion.common import resolve_path
from nyc_taxi_pipeline.ingestion.config import load_config
from nyc_taxi_pipeline.ingestion.manifest import ROLES, publish_datasets


def main():
    parser = config_parser(__doc__)
    parser.add_argument("--role", choices=ROLES, action="append", help="Repeat to select roles; default is all")
    parser.add_argument("--status-file", type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    status = args.status_file or resolve_path(config, "output/ingestion-publication.json")
    result = publish_datasets(config, args.role or ROLES, status_path=status)
    print(json.dumps(result, ensure_ascii=True))


if __name__ == "__main__":
    main()
