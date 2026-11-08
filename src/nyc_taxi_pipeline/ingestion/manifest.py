# Bước 1.2 | Chính | Công bố dữ liệu API, Parquet và lookup đã sẵn sàng.
#
# Đầu vào
# - Kết quả tải API và lưu dữ liệu từ ingest.py, kèm phạm vi đã chốt trong configs/demo.example.yaml.
# - Kỳ history và replay tách biệt, theo khoảng [range_start, range_end).
#
# Đầu ra
# - Manifest có dataset_role, source_id, raw_path, parquet_path, status, range_start và range_end.
# - source_id là mã dataset API hoặc định danh nguồn lookup. raw_path trỏ đến thư mục JSON gốc hoặc CSV lookup.
# - parquet_path trỏ đến Parquet của history/replay. Bảo dùng raw_path của zone_lookup tại bước 3.2.
#
# Yêu cầu
# - Với history/replay, một mục cho mỗi bộ dataset_role, source_id, range_start, range_end.
# - Với zone_lookup, một mục theo source_id. Để trống khoảng thời gian và parquet_path.
# - Chỉ công bố ready cho history/replay khi đã tải đủ phạm vi và ghi xong Parquet. Chạy lại cập nhật cùng khóa.
# - Với zone_lookup, chỉ công bố ready khi CSV đã được lưu hoàn chỉnh.
# - Mốc range_start và range_end có múi giờ. Batch và replay chỉ đọc dòng ready đúng vai trò.

import json
from datetime import datetime, timezone
from pathlib import Path

from .common import aware_time, write_json


def read_manifest(path):
    path = Path(path)
    if not path.exists():
        return {"version": 1, "datasets": []}
    with path.open(encoding="utf-8") as handle:
        document = json.load(handle)
    if document.get("version") != 1 or not isinstance(document.get("datasets"), list):
        raise ValueError("Unsupported manifest format")
    return document


def entry_key(entry):
    role = entry["dataset_role"]
    if role == "zone_lookup":
        return role, entry["source_id"]
    return (role, entry["source_id"], aware_time(entry["range_start"]), aware_time(entry["range_end"]))


def upsert_manifest(path, entry):
    """Atomic replacement for a single ingestion writer; reruns replace the same key."""
    entry = dict(entry)
    if entry["status"] == "ready":
        if not Path(entry["raw_path"]).exists():
            raise ValueError("Cannot publish ready without raw data")
        if entry["dataset_role"] != "zone_lookup":
            if not entry.get("coverage_complete") or not Path(entry["parquet_path"]).is_file():
                raise ValueError("Cannot publish ready without complete coverage and Parquet")
    entry["updated_at"] = datetime.now(timezone.utc).isoformat()
    document = read_manifest(path)
    key = entry_key(entry)
    document["datasets"] = [item for item in document["datasets"] if entry_key(item) != key]
    document["datasets"].append(entry)
    write_json(path, document)
    return entry
