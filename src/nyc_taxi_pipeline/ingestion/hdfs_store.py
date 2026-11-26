"""Streaming WebHDFS I/O and immutable, verified dataset publication.

HDFS is the handoff authority. Local files are staging/cache only. A new immutable
manifest is published last; no delete-then-rename of a shared 'latest' manifest.
"""

import hashlib
import json
import os
import posixpath
import re
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests

from .common import file_sha256, json_bytes


def publish_json(store, uri, payload):
    with TemporaryDirectory(prefix="taxi-hdfs-") as directory:
        path = Path(directory) / "document.json"
        path.write_bytes(json_bytes(payload) + b"\n")
        return store.put(path, uri)

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
            renamed = False
            try:
                with self.request("PUT", temporary, "CREATE", overwrite="false", replication=self.replication,
                                  blocksize=self.settings.get("block_size", 64 * 1024 * 1024)) as redirect:
                    url = self.data_url(redirect)
                with local.open("rb") as handle:
                    with self.session.put(url, data=handle, headers={"Content-Type": "application/octet-stream"},
                                          timeout=(10, self.settings.get("timeout_seconds", 60)),
                                          allow_redirects=False) as response:
                        response.raise_for_status()
                        if response.status_code != 201:
                            raise ValueError("DataNode did not finish CREATE")
                if self.status(temporary)["length"] != local.stat().st_size or self.sha256(temporary) != checksum:
                    raise ValueError("HDFS upload length/SHA-256 mismatch")
                self.wait_replicas(temporary)
                with self.request("PUT", temporary, "RENAME", destination=self.path(uri)) as response:
                    renamed = response.json()["boolean"]
                if not renamed:
                    # Another worker may have committed the same content-addressed path.
                    self._verify_existing(local, uri, checksum, self.status(uri))
            finally:
                if not renamed:
                    try:
                        with self.request("DELETE", temporary, "DELETE", recursive="false"):
                            pass
                    except requests.RequestException:
                        pass
        else:
            self._verify_existing(local, uri, checksum, exists)
        return {"uri": self.uri(uri), "sha256": checksum, "bytes": local.stat().st_size,
                "replication": self.replication}

    def _verify_existing(self, local, uri, checksum, status):
        if status["length"] != local.stat().st_size or self.sha256(uri) != checksum:
            raise ValueError("Existing immutable HDFS file differs; refusing overwrite")
        self.wait_replicas(uri)

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
