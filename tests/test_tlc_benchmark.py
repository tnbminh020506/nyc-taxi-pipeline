"""Local benchmark mode must never fetch missing/corrupt source files."""

import runpy
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

BENCHMARK = runpy.run_path(str(Path(__file__).resolve().parents[1] / "scripts/benchmark_tlc.py"))


def test_local_reference_cache_never_calls_http(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Local benchmark attempted HTTP")
    monkeypatch.setattr("requests.Session.get", forbidden)
    cache = tmp_path / "cache"
    files = [{"filename": "test.parquet", "path": "https://example.com/test.parquet"}]
    download = BENCHMARK["download_reference"]
    with pytest.raises(FileNotFoundError, match="never downloads"):
        download(files, cache, allow_download=False)
    pq.write_table(pa.table({"value": [1, 2]}), cache / "test.parquet")
    assert download(files, cache, allow_download=False)[0]["rows"] == 2
    (cache / "test.parquet").write_bytes(b"corrupt cache")
    with pytest.raises(pa.ArrowInvalid):
        download(files, cache, allow_download=False)
