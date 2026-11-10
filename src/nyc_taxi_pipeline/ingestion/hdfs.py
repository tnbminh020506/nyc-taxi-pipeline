"""Streaming WebHDFS I/O and immutable, verified dataset publication.

HDFS is the handoff authority. Local files are staging/cache only. A new immutable
manifest is published last; no delete-then-rename of a shared 'latest' manifest.
"""

import copy
import hashlib
import json
import logging
import os
import posixpath
import re
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import pyarrow.parquet as pq
import requests

from .common import resolve_path, write_json
from .ingest import file_sha256

LOG = logging.getLogger(__name__)


class WebHDFS:
    def __init__(self, config):
        self.config = config
        self.settings = config["storage"]
        self.base = self.settings["webhdfs_url"].rstrip("/")
        self.fs_uri = self.settings["fs_uri"].rstrip("/")
        self.authority = urlsplit(self.fs_uri).netloc
        self.replication = self.settings.get("replication", 2)
        self.session = requests.Session()

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def path(self, value):
        parsed = urlsplit(str(value))
        if parsed.scheme and (parsed.scheme != "hdfs" or parsed.netloc != self.authority):
            raise ValueError("HDFS URI must use the configured filesystem authority")
        path = unquote(parsed.path)
        if (not path.startswith("/") or ".." in path.split("/") or "\x00" in path
                or parsed.query or parsed.fragment):
            raise ValueError("HDFS path must be absolute and contain no traversal/query/fragment")
        return posixpath.normpath(path)

    def uri(self, path):
        return self.fs_uri + self.path(path)

    def request(self, method, path, op, **params):
        response = self.session.request(
            method, self.base + "/webhdfs/v1" + quote(self.path(path), safe="/"),
            params={"op": op, "user.name": self.settings.get("user", "hadoop"), **params},
            timeout=(10, self.settings.get("timeout_seconds", 60)), allow_redirects=False)
        response.raise_for_status()
        return response

    def data_url(self, response):
        if response.status_code != 307 or "Location" not in response.headers:
            raise ValueError("WebHDFS expected an explicit NameNode -> DataNode redirect")
        parsed = urlsplit(response.headers["Location"])
        mapping = self.settings.get("data_node_map", {})
        mapped = mapping.get(parsed.netloc)
        if mapped is None:
            # Some NameNodes advertise a Docker IP instead of the configured
            # hostname. Distinct published DataNode ports permit an exact map.
            matches = [value for key, value in mapping.items() if urlsplit("http://" + key).port == parsed.port]
            mapped = matches[0] if len(matches) == 1 else None
        if mapped is not None:
            target = urlsplit(mapped)
            return urlunsplit((target.scheme, target.netloc, parsed.path, parsed.query, ""))
        if parsed.scheme not in ("http", "https"):
            raise ValueError("Invalid DataNode redirect scheme")
        return response.headers["Location"]

    @contextmanager
    def open(self, uri):
        with self.request("GET", uri, "OPEN") as redirect:
            url = self.data_url(redirect)
        with self.session.get(url, stream=True, timeout=(10, self.settings.get("timeout_seconds", 60))) as response:
            response.raise_for_status()
            yield response

    def status(self, uri):
        with self.request("GET", uri, "GETFILESTATUS") as response:
            return response.json()["FileStatus"]

    def blocks(self, uri):
        with self.request("GET", uri, "GETFILEBLOCKLOCATIONS", offset=0, length=self.status(uri)["length"]) as response:
            return response.json()["BlockLocations"]["BlockLocation"]

    def wait_replicas(self, uri):
        deadline = time.monotonic() + self.settings.get("replication_wait_seconds", 60)
        while True:
            status, blocks = self.status(uri), self.blocks(uri)
            if (status["replication"] >= self.replication
                    and (not status["length"] or blocks)
                    and all(not block.get("corrupt") and len(set(block["names"])) >= self.replication for block in blocks)):
                return blocks
            if time.monotonic() >= deadline:
                raise ValueError(f"HDFS file is not replicated to {self.replication} DataNodes: {uri}")
            time.sleep(1)

    def sha256(self, uri):
        digest = hashlib.sha256()
        with self.open(uri) as response:
            for chunk in response.iter_content(1024 * 1024):
                digest.update(chunk)
        return digest.hexdigest()

    def put(self, local, uri):
        local = Path(local)
        checksum = file_sha256(local)
        try:
            exists = self.status(uri)
        except requests.HTTPError as error:
            if error.response.status_code != 404:
                raise
            exists = None
        if exists is None:
            parent = posixpath.dirname(self.path(uri))
            with self.request("PUT", parent, "MKDIRS") as response:
                if not response.json()["boolean"]:
                    raise ValueError("HDFS mkdir failed")
            temporary = self.path(uri) + ".upload-" + uuid.uuid4().hex
            with self.request("PUT", temporary, "CREATE", overwrite="false", replication=self.replication,
                              blocksize=self.settings.get("block_size", 64 * 1024 * 1024)) as redirect:
                url = self.data_url(redirect)
            with local.open("rb") as handle:
                with self.session.put(url, data=handle, headers={"Content-Type": "application/octet-stream"},
                                      timeout=(10, self.settings.get("timeout_seconds", 60)), allow_redirects=False) as response:
                    response.raise_for_status()
                    if response.status_code != 201:
                        raise ValueError("DataNode did not finish CREATE")
            if self.status(temporary)["length"] != local.stat().st_size or self.sha256(temporary) != checksum:
                raise ValueError("HDFS upload length/SHA-256 mismatch")
            self.wait_replicas(temporary)
            with self.request("PUT", temporary, "RENAME", destination=self.path(uri)) as response:
                if not response.json()["boolean"]:
                    raise ValueError("HDFS atomic rename failed")
        else:
            if exists["length"] != local.stat().st_size or self.sha256(uri) != checksum:
                raise ValueError("Existing immutable HDFS file differs; refusing overwrite")
            self.wait_replicas(uri)
        return {"uri": self.uri(uri), "sha256": checksum, "bytes": local.stat().st_size,
                "replication": self.replication}

    def download(self, uri, target, checksum):
        self.path(uri)  # Validate authority/path even when a verified cache exists.
        if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise ValueError("Expected a SHA-256 hex digest for HDFS download")
        target = Path(target)
        if target.is_file() and file_sha256(target) == checksum:
            return target
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(target.name + "." + uuid.uuid4().hex + ".tmp")
        try:
            with partial.open("wb") as handle, self.open(uri) as response:
                for chunk in response.iter_content(1024 * 1024):
                    handle.write(chunk)
            if file_sha256(partial) != checksum:
                raise ValueError("HDFS download SHA-256 differs from manifest")
            os.replace(partial, target)
        finally:
            partial.unlink(missing_ok=True)
        return target

    def json(self, uri):
        content = bytearray()
        with self.open(uri) as response:
            for chunk in response.iter_content(65536):
                content.extend(chunk)
                if len(content) > 8 * 1024 * 1024:
                    raise ValueError("HDFS JSON exceeds 8 MiB")
        return json.loads(content)


def receipt_path(config):
    return resolve_path(config, config["storage"].get("receipt", "data/hdfs-publication.local.json"))


def prepare_sources(config, roles):
    """Require cached originals; HDFS publication never re-crawls TLC implicitly."""
    from .tlc import sources_for_range
    if config.get("source", {}).get("kind") != "tlc_parquet":
        raise ValueError("HDFS pipeline currently requires TLC Parquet input")
    sources = {}
    cache = resolve_path(config, config["source"].get("cache_directory", "output/tlc-source-cache"))
    for role in roles:
        if role == "zone_lookup":
            continue
        for item in sources_for_range(config, role)[0]:
            path = cache / item["filename"] if item["path"].startswith("https://") else Path(item["path"])
            if not path.is_file():
                raise FileNotFoundError(f"Cache original TLC Parquet first; HDFS ingestion does not download it: {path}")
            pq.ParquetFile(path)  # Reject corrupt originals before uploading.
            sources[item["month"]] = path
    return sources


def run_hdfs_ingestion(config, roles):
    from .ingest import run_ingestion
    publication = uuid.uuid4().hex
    root = config["storage"].get("root", "/nyc-taxi").rstrip("/")
    receipt = {"status": "publishing", "publication_id": publication}
    write_json(receipt_path(config), receipt)
    try:
        sources = prepare_sources(config, roles)
        local = copy.deepcopy(config)
        local["storage"] = {"kind": "local"}
        local["source"]["files"] = {month: str(path) for month, path in sources.items()}
        with WebHDFS(config) as client:
            originals = [client.put(path, f"{root}/raw/trips/{file_sha256(path)}/{path.name}")
                         for path in sources.values()]
            # Raw sources are already verified/replicated before DuckDB runs.
            entries = run_ingestion(local, roles)
            if any(entry["status"] != "ready" for entry in entries):
                raise ValueError("Local staging failed; no HDFS ready manifest published")
            published = []
            for entry in entries:
                remote = dict(entry)
                prefix = f"{root}/publications/{publication}/{entry['dataset_role']}"
                raw = Path(entry["raw_path"])
                if raw.is_dir():
                    descriptor = json.loads((raw / "source.json").read_text(encoding="utf-8"))
                    descriptor.update(original_files_retained=True, hdfs_originals=originals)
                    write_json(raw / "source.json", descriptor)
                    remote["raw_path"] = client.uri(prefix + "/raw")
                    raw_files = [client.put(file, prefix + "/raw/" + file.name)
                                 for file in sorted(raw.rglob("*")) if file.is_file()]
                    remote["raw_files"] = raw_files
                else:
                    remote["raw_path"] = client.put(raw, prefix + "/" + raw.name)["uri"]
                if entry.get("parquet_path"):
                    remote["parquet_path"] = client.put(entry["parquet_path"], prefix + "/trips.parquet")["uri"]
                    remote["original_sources"] = originals
                remote.update(storage_kind="hdfs", replication=client.replication, hdfs_verified=True)
                published.append(remote)
            document = {"version": 1, "publication_id": publication, "datasets": published}
            local_manifest = resolve_path(config, "output/hdfs-publications") / f"{publication}.json"
            write_json(local_manifest, document)
            manifest = client.put(local_manifest, f"{root}/manifests/{publication}.json")
        receipt.update(status="ready", manifest_uri=manifest["uri"], manifest_sha256=manifest["sha256"],
                       original_sources=originals, datasets=published)
        write_json(receipt_path(config), receipt)
        LOG.info("Published immutable HDFS manifest: %s", receipt["manifest_uri"])
        return published
    except Exception as error:
        receipt.update(status="failed", error=str(error))
        write_json(receipt_path(config), receipt)
        LOG.exception("HDFS publication failed; no new ready manifest")
        return [{"dataset_role": role, "status": "failed", "error": str(error)} for role in roles]


def hdfs_manifest(config):
    """Pin an explicit shared URI, or require a successful local publication receipt."""
    uri = config["storage"].get("manifest_uri")
    checksum = None
    if not uri:
        receipt = json.loads(receipt_path(config).read_text(encoding="utf-8"))
        if receipt.get("status") != "ready":
            raise ValueError("HDFS publication is not ready")
        uri, checksum = receipt["manifest_uri"], receipt["manifest_sha256"]
    with WebHDFS(config) as client:
        if checksum and client.sha256(uri) != checksum:
            raise ValueError("HDFS manifest checksum differs from publication receipt")
        document = client.json(uri)
    if document.get("version") != 1 or not isinstance(document.get("datasets"), list):
        raise ValueError("Invalid HDFS manifest")
    return document
