# Phần Chính: DuckDB ingestion và replay qua Kafka

> Yêu cầu mới từ Minh đã merge: bàn giao qua **HDFS dùng chung trước**, sau đó đọc replay từ HDFS → Kafka. Dùng `configs/hdfs.example.yaml` theo [hướng dẫn HDFS](hdfs.md). Các bước local bên dưới vẫn là chế độ staging/kiểm thử độc lập; local-ready không thay thế HDFS-ready.

Theo thay đổi phân công ngày 10/11/2026, bước 1.1 dùng **DuckDB đọc trực tiếp Parquet tháng của TLC**, thay cho SODA v2/v3. Không cần tài khoản hoặc `SOCRATA_APP_TOKEN`. Nhánh `demo/ingestion` vẫn xuất phát từ `demo/mvp`; hai điểm chạy của nhóm được giữ nguyên.

```text
TLC HTTPS Parquet tháng → DuckDB lọc/copy → Parquet + manifest
  ├─ history → Spark Batch (Mạnh)
  └─ replay → Python → Kafka → Spark Stream (Mạnh)
TLC Zone Lookup CSV → manifest → PostgreSQL (Bảo)
```

Nguồn được lấy từ [trang TLC chính thức](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). DuckDB chỉ là công cụ đọc/lọc; kết quả nguồn này không được khẳng định giống hệt snapshot Socrata đã thử trước đó. API cũ và các báo cáo vẫn được giữ trong [tài liệu lịch sử](legacy-api-ingestion.md).

## 1. Cài và chạy (PowerShell)

Chạy từ thư mục repo; không cần activate virtualenv:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q -m 'not kafka'
.\.venv\Scripts\python.exe scripts/run_ingestion.py --config configs/demo.example.yaml
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config configs/demo.example.yaml --dry-run --output output/tlc-events.jsonl
```

Linux/macOS dùng `.venv/bin/python`. Runtime cần `requirements-ingestion.txt`; bộ dev thêm pytest, NumPy và psutil cho benchmark. Python 3.10 trở lên; DuckDB cố định 1.5.6 để tái lập kết quả.

Demo đã chỉnh theo yêu cầu **3:2 theo số dòng hợp lệ**: history thứ Hai 06/01/2025 và replay thứ Hai 20/01/2025, cùng **08:00–10:00 New York**, mỗi kỳ có 8 cửa sổ 15 phút. Bản nguồn đã cache cho **8.461 history / 5.651 replay**, tức **59,96%/40,04%**; không chia theo độ dài thời gian hoặc bỏ dòng để ép tỉ lệ. `replay_id` mới là `tlc-demo-3-2-001`, tốc độ 100 sự kiện/giây (~57 giây + overhead). [Bằng chứng demo](evidence/tlc-demo-3-2-2026-11-10.json). Nhóm trưởng vẫn cần chốt phạm vi lịch sử đủ cho baseline. Có thể sao chép `configs/demo.example.yaml` thành `configs/demo.local.yaml`, chỉnh hai kỳ không giao nhau rồi truyền `--config`. Mọi mốc phải có offset; đường dẫn tương đối tính từ `workspace_root`.

Chọn `--role history`, `--role replay` hoặc `--role zone_lookup` nếu chỉ cần một phần. Đầu ra mặc định: `data/tlc-manifest.json`, Parquet trong `data/processed`, provenance/lookup trong `data/raw`. Không dùng lẫn manifest Socrata cũ.

Bộ mẫu offline cũ vẫn chạy bằng `configs/fixture.yaml`; đó là fixture API để kiểm tra hợp đồng, không phải bằng chứng đã tải TLC. Test DuckDB offline tự tạo Parquet có nhiều row group, nhiều tháng, biên thời gian, dòng trùng giá trị, mã vùng lỗi và ca DST.

## 2. Cách đọc dữ liệu lớn

`source.kind: tlc_parquet` chọn backend mới trong `tlc.py`. Chương trình tự liệt kê các tháng giao với khoảng `[range_start, range_end)` theo giờ New York. `url_template` mặc định trỏ tới `yellow_tripdata_{month}.parquet` trên CloudFront của TLC. Đọc hai cột nguồn `tpep_pickup_datetime`, `PULocationID`, lọc bằng SQL và **COPY trực tiếp ra Parquet**, không đưa toàn bộ dữ liệu vào pandas/Python list.

DuckDB dùng `memory_limit: 512MB`, `threads: 2`, spill vào `output/duckdb-temp`, tối đa 8GB đĩa tạm. Memory limit là ngân sách bộ đệm của DuckDB, **không phải trần RSS của cả Python**. Báo cáo benchmark đo RSS thực tế và dung lượng spill. Cần đủ chỗ cho Parquet đích và đĩa tạm; lỗi mạng, schema hoặc hết tài nguyên làm lượt chạy `failed`, không công bố `ready`.

Đọc HTTPS cần extension `httpfs`. Chương trình tải extension **có chữ ký từ kho HTTPS chính thức của DuckDB**, cache riêng theo phiên bản/nền tảng và LOAD bằng đường dẫn tuyệt đối. Cách này xử lý lỗi đổi tên cache `INSTALL httpfs` đã gặp trên Windows. Không cho phép unsigned extension; bật kiểm tra chứng chỉ HTTPS bằng CA certifi. HTTP timeout 60 giây, tối đa 4 retry với backoff. Nếu nguồn vẫn lỗi thì dừng; không âm thầm đổi sang API hoặc fixture.

Muốn đọc bản Parquet đã tải sẵn, khai báo `source.files` là ánh xạ tháng → đường dẫn, ví dụ:

```yaml
source:
  kind: tlc_parquet
  source_id: tlc-yellow-taxi
  files:
    '2025-01': output/tlc-source-cache/yellow_tripdata_2025-01.parquet
    '2025-02': output/tlc-source-cache/yellow_tripdata_2025-02.parquet
```

CDN TLC có thể trả 403 từ một số mạng; [báo cáo đã ghi cả lượt lỗi và lượt đạt](duckdb-validation.md). Với bản nguồn đã cache, đặt thêm `zone_lookup.source` thành đường dẫn CSV cục bộ để không gọi mạng. Không suy ra rằng thay API bằng DuckDB sẽ tự sửa mọi HTTP 403.

Khi có `files`, mọi tháng cần dùng phải được khai báo và có file hợp lệ; không fallback về mạng. `source_id` phải đổi nếu chuyển sang một nguồn khác. Schema nguồn phải có timestamp địa phương không timezone và mã vùng dạng integer; một tháng sai schema làm thất bại cả kỳ.

## 3. Hợp đồng bàn giao

Manifest version 1 và schema bảy cột được giữ. Chỉ đọc mục `ready` đúng vai trò, nguồn và khoảng thời gian. Một writer ingestion ghi một manifest tại một thời điểm.

| Trường | Ý nghĩa với TLC |
| --- | --- |
| `dataset_role` / `source_id` | `history`, `replay`, `zone_lookup`; nguồn chuyến đi `tlc-yellow-taxi` |
| `range_start`, `range_end` | Khoảng UTC `[start,end)`; null với lookup |
| `raw_path` | Với TLC: thư mục **provenance `source.json`**, không phải JSON chuyến gốc; với lookup: CSV nguyên bản |
| `parquet_path` | Parquet cục bộ cho Spark Batch hoặc Python replay |
| `coverage_complete` | Đã đọc xong mọi file tháng được chọn và ghi hết kết quả của bộ lọc |
| `source_file_count`, `source_file_rows` | Số file và tổng số dòng vật lý theo metadata nguồn; không phải số dòng đã xuất |
| `row_count`, `invalid_row_count` | Số dòng thực xuất và số dòng có lỗi |
| `parquet_sha256`, `parquet_bytes` | Kiểm tra file đầu ra; replay từ chối file bị sửa |
| `status`, `error` | `downloading`, `ready`, `failed`; lý do khi thất bại |

`source.json` ghi URL/đường dẫn, số dòng từng file, SQL thực thi, phiên bản DuckDB, phạm vi và checksum đầu ra. Backend đọc từ xa **không lưu toàn bộ file tháng gốc**; `original_files_retained: false` ghi rõ điều này. Benchmark có cache bản gốc riêng để đối chiếu độc lập. Muốn giữ nguồn dài hạn thì tải/cấp các file gốc qua `source.files` và lưu cả cache đó.

**Giới hạn độ phủ:** file tháng TLC có thể chứa timestamp sai tháng hoặc null. Chỉ chọn các file có nhãn tháng giao với kỳ yêu cầu rồi lọc pickup; không quét mọi năm để truy tìm timestamp bị ghi nhầm sang file khác. Timestamp null không gán được vào kỳ. `coverage_complete` mô tả đọc đủ các file đã chọn, không cam kết TLC chứa mọi chuyến ngoài thực tế. URL nguồn có thể được nhà cung cấp cập nhật; giữ Parquet/checksum đã nghiệm thu để chạy demo lặp lại.

| Cột Parquet | Kiểu / quy tắc |
| --- | --- |
| `source_record_id` | string; `tên_file_tháng#file_row_number` |
| `source_row` | int64; vị trí vật lý bắt đầu 0 **trong từng file nguồn** |
| `tpep_pickup_datetime`, `pulocationid` | string; giữ giá trị nguồn để đối chiếu |
| `pickup_at` | timestamp microseconds, UTC (Arrow có thể hiển thị `Etc/UTC`), nullable |
| `pickup_zone_id` | int32 nguyên dương, nullable |
| `validation_error` | string nullable; null là hợp lệ |

Sắp đầu ra theo timestamp địa phương, tên file và vị trí vật lý. Hai dòng giống giá trị vẫn là hai chuyến, không tự deduplicate. Timestamp được hiểu theo `America/New_York`; thời điểm DST mơ hồ/không tồn tại được giữ với `pickup_at = null` và lý do lỗi. Khoảng nhiều tháng đi qua DST được hỗ trợ; mốc đầu/cuối rơi vào giờ mơ hồ bị từ chối. Mã vùng không hợp lệ vẫn giữ dòng, ghi lỗi; replay bỏ dòng lỗi. Spark cần lọc `validation_error IS NULL` và dùng giờ New York khi chia cửa sổ.

Mỗi lần nhập tạo file bất biến mới và cập nhật cùng khóa manifest nguyên tử. Lỗi chạy lại thay trạng thái thành `failed` nhưng không xóa file cũ. Không có resume từng row group; hãy chạy lại ingestion khi lỗi. `--no-resume` chỉ có ý nghĩa với backend API cũ. Sau khi đã nhập, **batch/replay dùng Parquet cục bộ, không phải tải lại TLC mỗi lần demo**. Khi chuyển máy/container, thống nhất mount/đường dẫn tuyệt đối trong manifest. CSV Zone Lookup vẫn bàn giao qua `zone_lookup.ready` cho Bảo.

## 4. Kafka và phát lại

```powershell
docker compose -f docker-compose.ingestion.yml up -d --wait
# Mạnh khởi động Spark stream theo replay_id trước bước tiếp theo.
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config configs/demo.example.yaml --create-topic
```

File compose riêng cung cấp Kafka KRaft một broker, volume giữ dữ liệu.
Endpoint từ host là `localhost:9092`; từ container cùng network là `kafka:29092`.
Nếu Spark ở network khác, cần nối vào network của Kafka trước khi dùng địa chỉ
container. `--create-topic` tạo topic khi chưa có; mặc định `taxi-events`,
1 partition, replication factor 1 cho demo. PostgreSQL do Bảo cấu hình riêng.

Chương trình chỉ đọc mục replay `ready` khớp nguồn/phạm vi của config, kiểm tra
checksum, lọc `[start, end)`, sắp theo pickup bằng SQLite tạm trên đĩa để tránh
nạp cả tập lớn vào RAM. `events_per_second` điều tiết số sự kiện/giây; 0 là tối đa.
Mỗi lần gửi chờ broker acknowledge; lỗi gửi làm lệnh thất bại.

JSON sự kiện:

```json
{
  "replay_id": "taxi-fixture-001",
  "event_id": "<sha256>",
  "pickup_at": "2025-01-20T13:01:00+00:00",
  "pickup_zone_id": 161,
  "source_file": "<parquet_path trong manifest>"
}
```

`event_id` kết hợp nguồn/phạm vi, checksum file và vị trí dòng; cùng file cố định
gửi lại giữ nguyên ID, hai dòng giống giá trị vẫn khác ID. ID cũng ổn định khi
nhập lại fixture giống hệt và sinh cùng bytes Parquet. Đổi nội dung/schema/thứ
tự dòng có thể đổi ID: dùng `--replay-id` mới cho một demo mới. Giữ replay_id khi
thử gửi lại cùng đầu vào; Mạnh khử trùng theo `(replay_id, event_id)`.
Kafka lưu sự kiện, việc đếm/khử trùng nghiệp vụ thuộc Spark.

Đọc kiểm tra bằng console consumer (dừng bằng Ctrl+C):

```powershell
docker compose -f docker-compose.ingestion.yml exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic taxi-events --from-beginning
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
.\.venv\Scripts\python.exe -m pytest -q -m kafka
```

Test Kafka kiểm tra riêng cả đầu vào API fixture và DuckDB Parquet: mỗi ca gửi 4 sự kiện và gửi lại 1 sự kiện, đọc đủ 5 message với 4 event_id, rồi xóa topic tạm. Không có broker thì test này skip rõ lý do.
CI `.github/workflows/ingestion.yml` chạy unit/HTTP/CLI trên Windows và Ubuntu,
và một job Kafka thật trên Ubuntu. Việc Spark khử trùng, nạp PostgreSQL và SQL
ra +50% cần nghiệm thu tiếp khi ghép phần của Mạnh/Bảo.

## 5. Kiểm thử khối lượng thực

```powershell
# Hai tháng thật đã cache, một lượt lớn; không gọi TLC.
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --source-mode local --months 2 --large-repeats 1 --small-repeats 0 --report output/tlc-2months.json
# Chỉ khi cần nghiệm thu mạng mới: cả năm, mốc chia theo 60/40 số dòng.
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --source-mode remote --months 12 --large-repeats 1 --small-repeats 0 --report output/tlc-year.json
```

Benchmark mặc định **local, 1 lượt lớn, 0 lượt nhỏ**; thiếu cache hoặc timezone database thì báo thiếu, không tự tải. Chỉ `--source-mode remote` mới cho phép tải bản gốc thiếu và đọc URL HTTPS. Trong cả hai chế độ, `balanced_ranges` chỉ đọc cache để chọn mốc chia gần **60/40 số dòng hợp lệ**, giữ nguyên các cửa sổ 15 phút. Report ghi cả tỉ lệ dự kiến và thực xuất; tỉ lệ history ngoài 57–63% làm nghiệm thu thất bại. Không chia ngẫu nhiên và không mặc định cắt đôi số tháng. PyArrow đọc cache theo batch 65.536 dòng, độc lập đối chiếu từng vị trí dòng nguồn, UTC pickup, mã vùng và cờ hợp lệ với toàn bộ đầu ra. So hash nội dung đã sắp theo vị trí vật lý, không chỉ so `COUNT(*)`. Lặp lại kiểm tra checksum Parquet và `event_id` trên cùng bộ replay nhỏ. Không phát hàng chục triệu message Kafka trong bài benchmark ingestion.

JSON báo cáo chứa thời gian, RSS, spill, số dòng/file và digest đối chiếu; lỗi vẫn ghi báo cáo và trả exit code 1. [Kết quả đo thực tế](duckdb-validation.md). CI thông thường chỉ chạy unit trên Windows/Ubuntu và Kafka thật cho hai backend, không gọi TLC. Job HTTPS chỉ chạy khi mở workflow thủ công và bật `run_live_tlc: true`; không lặp các lượt nhỏ mặc định. Báo cáo mạng được lưu làm artifact. Xem [prompt ban đêm](overnight-ingestion.md).

Tài liệu: [DuckDB Parquet](https://duckdb.org/docs/stable/data/parquet/overview), [httpfs HTTPS](https://duckdb.org/docs/stable/core_extensions/httpfs/https), [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
