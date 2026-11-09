import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from nyc_taxi_pipeline.ingestion.artifact import import_artifact


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify downloaded API data and map its paths to this machine")
    parser.add_argument("artifact", help="One extracted artifact/run folder or its source manifest file")
    args = parser.parse_args()
    try:
        print(json.dumps(import_artifact(args.artifact), ensure_ascii=False, indent=2))
    except (OSError, ValueError, KeyError) as error:
        print(f"Import failed: {error}", file=sys.stderr)
        raise SystemExit(1)
