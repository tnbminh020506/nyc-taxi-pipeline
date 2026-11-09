import json

from nyc_taxi_pipeline.ingestion.live_smoke import run_live_smoke


def test_live_acceptance_refuses_fixture(config):
    report = run_live_smoke(config)
    assert report["status"] == "failed" and "refuses fixture" in report["error"]
    assert json.loads((config["_root"] / "output/api-smoke/report.json").read_text())["status"] == "failed"


def test_live_acceptance_records_download_failure(config, monkeypatch):
    from nyc_taxi_pipeline.ingestion import live_smoke

    for section in config["datasets"].values():
        del section["fixture_pages"]
    config["zone_lookup"]["source"] = "https://example.test/lookup.csv"
    monkeypatch.setattr(live_smoke, "run_ingestion", lambda config: [
        {"dataset_role": "history", "status": "failed", "error": "HTTP 403"}])
    report = run_live_smoke(config)
    assert report["status"] == "failed"
    assert report["datasets"][0]["error"] == "HTTP 403"
