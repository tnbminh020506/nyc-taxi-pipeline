# Phần Chính: lấy dữ liệu và replay qua Kafka

Nhánh `demo/ingestion` được tạo từ `demo/mvp`, triển khai các bước 1.1–1.4.
Hai điểm chạy giữ tại `scripts/run_ingestion.py` và `scripts/replay_taxi_stream.py`.
Các module `common.py`, `ingest.py`, `manifest.py`, `replay.py` hỗ trợ cấu hình,
tải dữ liệu, công bố manifest và phát sự kiện. Python 3.10 trở lên.

## 1. Cài và chạy ngay bằng bộ mẫu (PowerShell)

Chạy từ thư mục repo:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts/run_ingestion.py --config configs/fixture.yaml
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config configs/fixture.yaml --dry-run --output output/fixture-events.jsonl
```

Không cần kích hoạt virtualenv. Trên Linux/macOS dùng `.venv/bin/python`.
`requirements-ingestion.txt` chỉ chứa thư viện chạy; `requirements-dev.txt` thêm pytest.

Kết quả bộ mẫu: history 4 dòng, replay 4 dòng, lookup 1 dòng; manifest tại
`data/fixture-manifest.json`, JSON gốc tại `data/raw/fixture`, Parquet tại
`data/processed/fixture`. JSONL dry-run có 4 sự kiện của vùng 161: 08:01, 08:05,
08:14, 08:15 giờ New York (13:xx UTC). Đây là fixture cục bộ, không phải kết quả
lấy API thật. `input_mode` trong manifest phân biệt `fixture` và `api`.

Hai cửa sổ history được xác nhận nằm trong `confirmed_history_windows` và mục
history của manifest (`confirmed_windows`): 06/01 và 13/01/2025, 08:00–08:15.
Không diễn giải khoảng trống giữa hai ngày của fixture thành độ phủ API thật.
Baseline kỳ vọng là (1 + 3) / 2 = 2; stream kỳ vọng là 3 và 1 ở hai cửa sổ.
Các phép tổng hợp và SQL do phần Mạnh/Bảo triển khai.

## 2. Gọi API thật

```powershell
Copy-Item configs/demo.example.yaml configs/demo.local.yaml
.\.venv\Scripts\python.exe scripts/run_ingestion.py --config configs/demo.local.yaml
# Có thể chạy một phần: --role history, --role replay hoặc --role zone_lookup
```

Nhóm trưởng cần chốt hai khoảng không giao nhau trong `datasets` trước demo.
Các ngày trong file example chỉ là ví dụ. Mọi mốc cấu hình phải có offset.
File example hiện chọn 15 phút mỗi kỳ để chạy thử; cần nhiều cửa sổ lịch sử
thì nhóm chốt và nhập thêm phạm vi tương ứng. Không mặc định tải cả tuần/năm.
Mọi đường dẫn tương đối được tính từ `workspace_root` (mặc định là thư mục cha
của thư mục cấu hình), không phụ thuộc thư mục terminal khi truyền config tuyệt đối.
Token chỉ đọc từ biến môi trường, không lưu vào raw/manifest hoặc commit.

Mặc định hiện dùng SODA v2 công khai tại `resource/irx2-vgm4.json`, chưa cần token.
Để dùng SODA v3, đặt `api.version: 3`, cấu hình token qua biến môi trường và POST tới `api/v3/views/irx2-vgm4/query.json`,
`X-App-Token`, `query`, `page.pageNumber` bắt đầu từ 1 và `page.pageSize`.
Truy vấn lấy `:id AS source_record_id`, `tpep_pickup_datetime`, `pulocationid`,
lọc `[range_start, range_end)`, sắp theo thời điểm đón và `:id` để thứ tự ổn định.
Các response được lưu nguyên bytes thành `page-000001.json`, v.v.;
`request.json` lưu query, endpoint, phiên bản API, phạm vi và số dòng từng trang.

Với SODA v2, `api.version: 2` và endpoint
`https://data.cityofnewyork.us/resource/irx2-vgm4.json`; chương trình dùng GET
`$query` với LIMIT/OFFSET. Không có chuyển nguồn hay fallback âm thầm.

Một trang ngắn hơn `page_size` (kể cả trang rỗng) mới xác nhận hết phạm vi.
Nếu đúng `max_pages` trang đầy, trạng thái là `incomplete`, tăng giới hạn và chạy
lại. Lỗi HTTP/schema/ghi file chuyển thành `failed`; lệnh trả exit code 1.
HTTP 429/500/502/503/504 được retry theo `api.max_retries`, có backoff và Retry-After.
Có thể chỉnh `api.max_retries` (0–20), `request_budget_seconds`, `retry_backoff_seconds`,
`retry_backoff_max_seconds`, `min_retry_delay_seconds`, `retry_jitter_seconds`
và `min_request_interval_seconds`. Cấu hình demo/nghiệm thu hiện dùng tối đa
15 lần retry, ngân sách retry 120 giây, chờ tối thiểu 2 giây sau lỗi, backoff tối
đa 6 giây cộng jitter tối đa 1 giây; request logic mới cách nhau tối thiểu 1 giây.
Timeout connect/read của lần thử sau được giảm theo thời gian còn lại; ngân sách
được kiểm tra giữa các lần thử, không phải cam kết thời gian thực cho DNS/socket.
401/403 và lỗi chứng chỉ TLS không retry. Retry-After vượt ngân sách thì dừng,
không gửi lại sớm hơn yêu cầu của nguồn. Sau lỗi tạm thời, pool kết nối được tạo
lại; lỗi đứt body cũng được ghi nhận và thử lại, không chỉ lỗi trước response.
Số trang, số dòng, số dòng lỗi và trạng thái được log theo kỳ.
API phải giữ dataset ổn định trong một lần tải; thứ tự ổn định không tạo snapshot
giao dịch nếu nhà cung cấp cập nhật dataset giữa các trang.

Thời gian floating của API được hiểu theo `source_timezone: America/New_York`
và chuyển sang UTC. Thời gian địa phương mơ hồ/không tồn tại khi đổi DST được
đánh lỗi; không tự chọn offset. Khoảng API đi qua đổi offset bị từ chối, cần chọn
kỳ demo tránh DST hoặc tách và thiết kế lại các phạm vi trước khi chạy.

## 3. Hợp đồng bàn giao cho Mạnh và Bảo

Manifest là JSON `{ "version": 1, "datasets": [...] }`. Chỉ đọc mục `ready`
đúng vai trò, nguồn và phạm vi. `parquet_path`/`raw_path` là đường dẫn tuyệt đối;
tiến trình nhận cần truy cập được các đường dẫn này. Khi đổi máy/container,
chạy ingestion tại workspace dùng chung hoặc thống nhất lại mount/đường dẫn.

| Trường manifest | Ý nghĩa |
| --- | --- |
| `dataset_role` | `history`, `replay`, `zone_lookup` |
| `source_id` | Dataset API hoặc định danh lookup |
| `range_start`, `range_end` | ISO 8601 UTC; null với lookup |
| `raw_path` | Thư mục trang JSON, hoặc file CSV lookup |
| `parquet_path` | File Parquet history/replay; null với lookup |
| `status` | `downloading`, `ready`, `incomplete`, `failed` |
| `coverage_complete` | Xác nhận đọc hết API, hoặc toàn bộ trang được khai báo của fixture |
| `row_count`, `page_count`, `invalid_row_count` | Số liệu kiểm tra chất lượng của chuyến đi |
| `parquet_sha256` | Checksum file để replay phát hiện dữ liệu đã bị sửa |
| `error` | Lý do khi chưa hoàn tất hoặc thất bại |

Mỗi lần chạy ghi file mới và thay manifest nguyên tử. Cùng vai trò–nguồn–phạm vi
chỉ có một mục, lookup có một mục theo nguồn. Lần tải lại lỗi thay trạng thái cũ,
không để mục cũ tiếp tục được coi là dữ liệu mới. File từ lần cũ vẫn tồn tại cho
tiến trình đang đọc; không tự xóa dữ liệu. Chạy **một tiến trình ingestion** ghi
một manifest tại một thời điểm (chưa hỗ trợ nhiều writer đồng thời).

Với mục chưa `ready`, chương trình có thể tiếp tục từ các trang JSON đã lưu,
kiểm tra checksum từng trang và dựng lại Parquet để tránh nối trùng dòng.
Query/endpoint/phiên bản/page size phải trùng lần trước; trang hỏng được tải lại.
Tắt bằng `--no-resume` hoặc `api.resume: false`. Khi source đã cập nhật, nên tải
mới với `--no-resume`; thứ tự ổn định và resume không tạo snapshot giao dịch.
Lượt `ready` đã hoàn tất sẽ không được dùng như cache cho lần nhập mới.
Sau khi có dữ liệu `ready`, có thể chạy batch/replay nhiều lần từ manifest và
Parquet đã lưu mà không gọi lại API. Chỉ chạy lại `run_ingestion.py` khi thực sự
muốn lấy lại nguồn; Spark và replay không cần tải NYC lại mỗi lần demo.

Schema Parquet dùng chung:

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `source_record_id` | string, nullable | ID dòng Socrata nếu có |
| `source_row` | int64 | Vị trí dòng ổn định trong một lần nhập, bắt đầu từ 0 |
| `tpep_pickup_datetime`, `pulocationid` | string, nullable | Giá trị nguồn để đối chiếu |
| `pickup_at` | timestamp microseconds, UTC, nullable | Thời điểm đón đã chuẩn hóa |
| `pickup_zone_id` | int32, nullable | Mã vùng nguyên dương |
| `validation_error` | string, nullable | Lý do lỗi hoặc null nếu hợp lệ |

Dòng lỗi vẫn được giữ trong Parquet để đối chiếu số lượng; replay bỏ chúng và
ghi số liệu/lý do. Mạnh cần thống nhất lọc `validation_error IS NULL`, kiểm tra
timestamp/mã vùng và dùng giờ New York khi tính thứ/khung 15 phút. Có tên trong
lookup không phải điều kiện để một mã vùng nguyên dương được đếm.
Bảo nhận CSV gốc qua mục `zone_lookup.ready`, ánh xạ `LocationID`, `Zone`,
`Borough`; loader PostgreSQL vẫn thuộc phần Bảo.

## 4. Kafka và phát lại

```powershell
docker compose -f docker-compose.ingestion.yml up -d --wait
# Mạnh khởi động Spark stream theo replay_id trước bước tiếp theo.
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config configs/fixture.yaml --create-topic
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

Test Kafka tạo topic tạm, gửi 4 sự kiện và gửi lại 1 sự kiện, đọc đủ 5 message
với 4 event_id, rồi xóa topic tạm. Không có broker thì test này skip rõ lý do.
CI `.github/workflows/ingestion.yml` chạy unit/HTTP/CLI trên Windows và Ubuntu,
và một job Kafka thật trên Ubuntu. Việc Spark khử trùng, nạp PostgreSQL và SQL
ra +50% cần nghiệm thu tiếp khi ghép phần của Mạnh/Bảo.

## Nghiệm thu riêng với API thật

```powershell
.\.venv\Scripts\python.exe scripts/test_live_api.py --api-version 2
```

`configs/api-smoke.yaml` dùng hai khoảng một phút, `page_size=100`, tối đa 20
trang mỗi kỳ. Đây là phép thử kết nối/nhập liệu, không phải kỳ baseline đủ cho
demo 15 phút. Script từ chối fixture, tải lookup qua HTTPS, so khớp số dòng JSON,
Parquet và truy vấn `COUNT(*)` độc lập, rồi tạo JSONL replay từ dữ liệu thật.
Chỉ `passed` khi cả hai kỳ có dữ liệu, tải đủ, số dòng khớp và replay khớp số
dòng hợp lệ. Báo cáo: `output/api-smoke/report.json`; trạng thái lỗi vẫn được lưu.

Nếu máy hiện tại nhận HTTP 403, có thể chạy phép thử trên GitHub-hosted runner
để so sánh môi trường truy cập, không thay đổi endpoint của dự án:

```powershell
gh workflow run live-api.yml --ref demo/ingestion -f api_version=2
```

Workflow `Live NYC API acceptance` lưu artifact gồm JSON gốc, query, Parquet,
manifest và báo cáo, kể cả khi kiểm tra thất bại. Artifact giữ 7 ngày. Đường dẫn
trong manifest của runner là đường dẫn Linux; cần ánh xạ lại trước khi đọc trên
máy khác. SODA v2 là lựa chọn mặc định hiện tại; v3 được chọn tường minh khi có token.

Để đo nhiều lượt, dùng workflow **NYC API reliability**, chế độ `acceptance`.
Workflow chạy 20 lượt mới theo bốn batch nối tiếp, hai lượt cuối kiểm tra nhiều
trang trong 15 phút. Xem [phương pháp và kết quả](api-reliability.md).
Đây là phép thử chủ động, không phải lịch chạy định kỳ.

Để thử **SODA v3**, cần tài khoản Socrata và app token của chính người chạy:

1. Theo [hướng dẫn chính thức tạo App Token](https://support.socrata.com/hc/en-us/articles/210138558-Generating-App-Tokens-and-API-Keys), đăng nhập tài khoản Data & Insights, mở **Developer Settings → Create New App Token**. Tài liệu cho phép thao tác từ một domain Data & Insights khác như [Evergreen](https://evergreen.data.socrata.com/) nếu trang NYC không truy cập được.
2. Cấu hình token trong phiên PowerShell 7, không đưa vào chat hay YAML:

   ```powershell
   $env:SOCRATA_APP_TOKEN = Read-Host 'Socrata app token' -MaskInput
   .\.venv\Scripts\python.exe scripts/test_live_api.py --api-version 3
   ```

3. Để chạy v3 trên Actions, chủ repo cấu hình repository secret `SOCRATA_APP_TOKEN`, rồi chọn `api_version=3` khi chạy workflow. Không có token thì lượt v3 thất bại rõ ràng; không tự chuyển sang v2.

Token không đảm bảo sửa được mọi HTTP 403. Phân biệt kết quả v2/v3 và máy chạy
trong báo cáo; một lượt v2 thành công không chứng minh v3 đã được nghiệm thu.

## Tải và dùng trên máy bị 403

Có thể chạy ingestion ở GitHub-hosted runner rồi tải kết quả về máy. Đây là cách
lấy dữ liệu công khai qua v2 từ môi trường đã kiểm chứng; không khẳng định đã sửa
được HTTP 403 trực tiếp trên mạng Windows hiện tại.

```powershell
gh workflow run live-api.yml --ref demo/ingestion -f api_version=2 -f config_file=configs/demo.example.yaml
gh run list --workflow live-api.yml --branch demo/ingestion
gh run watch <RUN_ID> --exit-status
gh run download <RUN_ID> --dir output/nyc-download-<RUN_ID>
.\.venv\Scripts\python.exe scripts/import_api_artifact.py output/nyc-download-<RUN_ID>
```

Thay `<RUN_ID>` bằng mã lượt chạy đã chọn. Bước import chỉ nhận artifact có báo
cáo nghiệm thu `passed`, kiểm tra checksum/đếm lại JSON, Parquet và lookup, rồi
tạo `manifest.local.json` và `imported.local.yaml`. File gốc của runner được giữ.
Đường dẫn Linux được ánh xạ sang thư mục dữ liệu trên máy này; bước import không
gọi API. Nếu tải cả nhiều batch đo, chọn riêng một thư mục `run-XXX` để import.

Dùng đường dẫn config mà lệnh import in ra để chạy replay:

```powershell
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config '<CONFIG_PATH>' --dry-run
```

Mạnh/Bảo có thể đọc manifest cục bộ đó và các file được tham chiếu. Đã đối chiếu
trên artifact thật: 516 event_id ở lượt 15 phút được giữ nguyên sau khi nhập về
Windows; `source_file` được đổi sang đường dẫn cục bộ để tra đúng manifest.

## Tài liệu giao thức

- [Socrata SODA3: query và phân trang](https://dev.socrata.com/docs/queries/)
- [Socrata API endpoints](https://dev.socrata.com/docs/endpoints.html)
- [Apache Kafka 4.1 quickstart và Docker image](https://kafka.apache.org/41/getting-started/quickstart/)
