# Bàn giao Ingestion v2

Bản bàn giao nằm trên nhánh `demo/ingestion`. Minh xem [báo cáo standalone và năm/tháng dữ liệu](bao-cao-ingestion-standalone.md) để chạy kiểm tra trực tiếp nguồn cloud.

## Chạy từ dữ liệu nguồn

Tạo môi trường Python 3.10 trở lên. Mẫu dưới đây chạy bằng PowerShell từ thư mục repo. Compose dựng Kafka, một NameNode và hai DataNode cho demo trên một máy.

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements-ingestion.txt
Copy-Item configs/ingestion.example.yaml configs/ingestion.local.yaml
docker compose -f docker-compose.hdfs.yml -f docker-compose.ingestion.yml up -d --wait
.venv/Scripts/python.exe scripts/run_ingestion.py --config configs/ingestion.local.yaml
```

Mặc định dùng SODA v2. Có thể cấu hình SODA v3 bằng endpoint `/api/v3/views/<dataset>/query.json` và biến môi trường `SOCRATA_APP_TOKEN`, hoặc chọn `source.kind: tlc_parquet` và khai báo đường dẫn tệp gốc theo tháng. Hai kỳ mẫu giữ khoảng chạy thử TLC trước đây. Nguồn, ngày demo và độ dài lịch sử phải phù hợp với baseline nhóm chọn. GeoJSON và CSV có thể dùng URL hoặc tệp đã tải trước. Tăng `max_pages` nếu kỳ dữ liệu vượt ngân sách tải.

Lệnh ingestion trả `manifest_uri` và ghi trạng thái lượt chạy vào `output/ingestion-publication.json`. Mặc định ghi bốn vai trò. Nếu dùng `--role`, lặp cờ cho từng vai trò cần công bố trong cùng manifest. Replay cần một manifest chứa cả `replay` và `zone_boundaries`.

```powershell
$manifestUri = (Get-Content output/ingestion-publication.json -Raw | ConvertFrom-Json).manifest_uri
.venv/Scripts/python.exe scripts/replay_taxi_stream.py --config configs/ingestion.local.yaml --manifest $manifestUri --scope-id demo-01 --replay-id demo-01 --dry-run --output output/requests.jsonl
```

Khi phát lên Kafka, bỏ `--dry-run --output ...`. Đặt `clock.wall_start` là một mốc giờ thực có offset và `clock.simulation_start` là mốc nghiệp vụ tương ứng. Dùng cùng cấu hình clock cho replay, publisher, simulator và pipeline. Clock tự tiến khi vắng sự kiện. Replay theo timestamp gốc và gửi ngay những sự kiện đã tới hạn khi chạy lại. Khi dùng lại `replay_id`, giữ cùng manifest, scope, clock và seed. Kafka retry nhận cùng ID. Topic mặc định `taxi-events` dùng key JSON UTF-8 `[replay_id,request_id]` hoặc `[replay_id,trip_id]`.

## Dữ liệu giao cho Mạnh và Bảo

- Manifest v2 gồm `history`, `replay`, `zone_lookup`, `zone_boundaries`. Với chuyến đi, `raw_path` trỏ đến index JSON của các trang gốc hoặc Parquet tháng gốc. Với nguồn vùng, `raw_path` trỏ trực tiếp đến CSV hoặc GeoJSON WGS84. Có URI HDFS và SHA-256 cho các tệp dùng để replay.
- API đọc theo mốc pickup và ID nguồn, so số dòng trước/sau phân trang, kiểm tra trùng ID và kỳ dữ liệu. Kỳ API theo floating timestamp phải nằm ngoài vùng chuyển DST. TLC Parquet quét các tệp tháng giao kỳ. Coverage biểu thị phạm vi tải và kiểm tra của nguồn. Các timestamp địa phương mơ hồ hoặc không tồn tại được đưa vào diện dữ liệu lỗi.
- Parquet có thời gian UTC, điểm đón/trả, thời lượng, `source_record_id` và `source_file` gốc. `validation_error` loại dòng khỏi tập nhu cầu. `matching_error` báo thiếu dữ liệu ghép. Request đủ giờ/vùng đón vẫn phát nếu thiếu thông tin ghép.
- Ví dụ JSONL đầy đủ được tạo bằng lệnh dry-run. Một request tạo một `ride_requested`. Trip hai người có một `pickup_completed` và hai `dropoff_completed` với ID riêng.

## Giao diện integration

Đặt `warehouse.schedule_repository` và `warehouse.archive_repository` thành `module:factory`. Mỗi factory nhận cấu hình đã nạp và trả repository. Các chữ ký nằm trong [PickupRepository](../src/nyc_taxi_pipeline/ingestion/pickup_publisher.py), [DropoffRepository](../src/nyc_taxi_pipeline/ingestion/dropoff_simulator.py) và [ArchiveRepository](../src/nyc_taxi_pipeline/ingestion/event_archive.py).

- `claim_pickup` trả `PickupLease` chứa payload v2 đã commit. Kho claim bằng lease nguyên tử theo giờ thực và dùng `now` mô phỏng để xét hạn. Ack nhận ID, lease token và `KafkaAck(topic, partition, offset)`. Nếu worker dừng giữa Kafka ack và DB ack, kho cho phép claim lại sau hết hạn với cùng payload và `event_id`.
- `claim_dropoff` trả từng `DropoffLease` có due, giờ bắt đầu trip, thời lượng và xác nhận pickup đã ack. Simulator đặt `completed_at` đúng giờ due. Ca lịch có thời lượng lỗi vào `quarantine_dropoff`. Xử lý lifecycle của Mạnh cập nhật trạng thái khách/trip từ sự kiện đã nhận.
- `claim_journal` trả `JournalLease` với `entry_id`, `scope_id`, `replay_id` và `kind`. Kind `journal` chứa chuyển trạng thái nghiệp vụ. Kind `admission` thêm `policy_version`, `topic`, `partition`, `offset`, `event_id`, `event_at`, `ingested_at`, `decision` (`accepted`, `quarantined`, `duplicate`), `reason` và `watermark_at` nếu có. `acknowledge_journal` nhận danh sách cặp ID/token và receipt đã commit. `record_event_archive` ghi receipt idempotent trước khi archive worker commit Kafka offsets.

```powershell
.venv/Scripts/python.exe scripts/run_pickup_publisher.py --config configs/ingestion.local.yaml
.venv/Scripts/python.exe scripts/run_dropoff_simulator.py --config configs/ingestion.local.yaml
.venv/Scripts/python.exe scripts/run_event_archive.py --config configs/ingestion.local.yaml events
.venv/Scripts/python.exe scripts/run_event_archive.py --config configs/ingestion.local.yaml journal
.venv/Scripts/python.exe scripts/run_event_archive.py --config configs/ingestion.local.yaml snapshot --spec output/snapshot-spec.json
```

Mỗi worker chạy trong một tiến trình riêng. `events` có thể lưu Kafka lên HDFS trước khi kho sẵn sàng. Khi nối với retention, cấu hình repository để nhận cả receipt của raw events và journal. Payload raw được giữ nguyên bằng Base64 cùng Kafka key, header, topic, partition và offset. Batch đọc theo `archive_offset_ranges`, lọc scope/replay và cutoff, khử trùng theo offset rồi theo ID nghiệp vụ. Offset cuối của mỗi khoảng là exclusive. Batch dùng admission ledger để áp dụng cùng quyết định dữ liệu đến muộn với stream.

Ví dụ yêu cầu snapshot cho một khoảng đang đợi xử lý đủ admission:

```json
{
  "scope_id": "demo-01",
  "replay_id": "demo-01",
  "cutoff": "2025-01-20T13:15:00+00:00",
  "admission_policy_version": "demo-v1",
  "coverage": {"status": "pending"},
  "archive_offset_ranges": [
    {"topic": "taxi-events", "partition": 0, "offset_start": 0, "offset_end": 2}
  ],
  "receipt_uris": ["hdfs://namenode:8020/nyc-taxi/archive/events/<id>.receipt.json"]
}
```

Dùng các URI receipt thực tế từ log worker hoặc repository. Snapshot có coverage `confirmed` cần khớp admission theo từng offset của scope/replay trong khoảng yêu cầu. Các receipt bất biến cho phép chạy lại cùng spec và nhận cùng snapshot ID.

## Kiểm tra

```powershell
.venv/Scripts/python.exe -m pip install pytest
.venv/Scripts/python.exe -m pytest -q
# Kiểm tra riêng với Parquet TLC tháng 1/2025 đã cache
$env:TEST_INGESTION_TLC_JANUARY = (Resolve-Path output/tlc-source-cache/yellow_tripdata_2025-01.parquet).Path
.venv/Scripts/python.exe -m pytest -q tests/test_ingestion_v2.py::test_tlc_uses_original_file_and_normalizes_pickup_and_dropoff
```

Bộ test dùng máy chủ HTTP mô phỏng SODA/WebHDFS, Kafka test double và repository giả lập. Ca TLC dùng Parquet gốc cho **8.461 dòng history, 5.651 dòng replay và 5.651 request v2**. Lượt ngày 18/11 từng nhận HTTP 403 ở endpoint NYC. Ngày 26/11, test standalone đọc thành công toàn bộ 12 tệp tháng năm 2025 qua HTTPS TLC. Máy Windows hiện chưa kết nối được cụm HDFS/Kafka local. Sau khi ghép repository của Bảo và admission ledger của Mạnh, nhóm có thể chạy các CLI trên Compose để nghiệm thu liên dịch vụ.
