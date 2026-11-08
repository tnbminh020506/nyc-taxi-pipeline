from pathlib import Path

import pytest

from nyc_taxi_pipeline.ingestion.common import load_config

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def config(tmp_path):
    config = load_config(ROOT / "configs/fixture.yaml")
    for section in config["datasets"].values():
        section["fixture_pages"] = [str(ROOT / path) for path in section["fixture_pages"]]
    config["zone_lookup"]["source"] = str(ROOT / config["zone_lookup"]["source"])
    config["_root"] = tmp_path
    return config
