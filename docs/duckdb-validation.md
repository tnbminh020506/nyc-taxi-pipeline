# Nghiệm thu DuckDB/TLC — 10/11/2026

**DuckDB phù hợp với ingestion này:** đã thực sự xuất 48.722.567 dòng từ 12 tháng Yellow Taxi 2025 và đối chiếu độc lập toàn bộ đầu ra. Tuy nhiên, HTTPS TLC vẫn có thể bị CDN chặn theo mạng/IP; không coi DuckDB là cách bảo đảm hết HTTP 403.

Đã chuyển bước 1.1 của Chính sang DuckDB/TLC, giữ schema Parquet bảy cột, manifest v1 và payload replay/Kafka. [Hướng dẫn chạy](ingestion.md) · [phân công cập nhật](guide/giao-viec-demo-taxi.md).

## Kết quả đo thực tế

Tất cả lượt lớn dùng DuckDB 1.5.6, memory_limit **512MB**, 2 threads và spill tối đa 8GB. Memory limit là ngân sách bộ đệm, không phải trần RSS của Python. Số liệu RSS/spill được lấy mẫu mỗi 100ms; thời gian và bộ nhớ phụ thuộc máy/mạng.

| Máy / đầu vào | Dòng thực xuất mỗi lượt | Lượt lớn đạt | Thời gian ingestion | RSS cao nhất | Spill cao nhất |
| --- | ---: | ---: | ---: | ---: | ---: |
| Windows hiện tại / nguồn thật đã cache, tháng 1–2 | 7.052.716 | 2/2 | 9,688 / 9,625 giây | 634 MiB | 251 MiB |
| Ubuntu GitHub runner / HTTPS, tháng 1–2 | 7.052.716 | 1/1 | 7,420 giây | 896 MiB | 256 MiB |
| Ubuntu GitHub runner / HTTPS, cả năm 2025 | **48.722.567** | 1/1 | **61,556 giây** | **913 MiB** | **3,97 GiB** |

Nguồn hai tháng có 7.052.769 dòng vật lý, khoảng 119,50 MB file gốc; mỗi lượt tạo 69,13 MB Parquet. Hai lượt local tạo cùng checksum. Lượt Ubuntu HTTPS cũng tạo cùng checksum Parquet với Windows cho cả history/replay.

Nguồn cả năm có **48.722.602 dòng vật lý**, tổng file gốc **829,97 MB**. Chia history tháng 1–6 và replay tháng 7–12, lọc theo hai phạm vi không giao nhau:

| Đầu ra cả năm | Dòng thực xuất | Dòng được gắn cờ lỗi | Dung lượng Parquet |
| --- | ---: | ---: | ---: |
| history | 24.083.360 | 0 | 236,06 MB |
| replay | 24.639.207 | 11.687 | 243,58 MB |

Dòng lỗi được giữ trong Parquet với `validation_error`; replay sẽ bỏ các dòng đó. Số dòng bị loại khỏi phạm vi được kiểm chứng theo từng file/phạm vi áp dụng, không tự sửa timestamp nguồn. File tháng có thể chứa timestamp ngoài nhãn tháng; `coverage_complete` chỉ xác nhận đã đọc đủ các file được chọn, không khẳng định dữ liệu TLC luôn đầy đủ/chính xác ngoài thực tế.

## Đối chiếu tính đúng và các kiểm thử khác

Không dùng `COUNT(*)` từ metadata làm bằng chứng xử lý lớn. Ingestion thực hiện COPY ra file; validator khác dùng **PyArrow**, đọc bản gốc theo batch 65.536 dòng, tự lọc và chuyển timezone, rồi so SHA-256 của từng vị trí vật lý, UTC pickup, mã vùng và cờ lỗi với toàn bộ đầu ra đã sắp theo vị trí nguồn. Giữ hai dòng có giá trị giống nhau là hai dòng riêng.

- Hai tháng local: cả hai lượt khớp từng dòng; thời gian đối chiếu thêm 48,594 / 49,641 giây. HTTPS Ubuntu: thêm 1,680 giây.
- Cả năm: cả **12 digest từng tháng khớp**, đối chiếu thêm 19,416 giây. Thời gian ingestion 61,556 giây chưa bao gồm tải bản gốc cho validator (4,325 giây) và bước đối chiếu này.
- Các lượt nhỏ thành công tạo 1.029 dòng history, 484 dòng replay và 265 vùng. Replay lặp lại cùng file giữ nguyên thứ tự/payload/event_id.
- **49 unit/HTTP/contract test đạt trên Windows và Ubuntu.** Có kiểm tra nhiều tháng/row group, đúng biên `[start,end)`, giữ dòng trùng giá trị, dòng mã vùng lỗi, DST mơ hồ/không tồn tại, một tháng sai schema và lỗi kết nối không được công bố ready.
- **2 ca Kafka thật đạt** (API fixture và DuckDB Parquet): mỗi ca gửi 4 sự kiện + gửi lại 1, nhận đủ 5 với 4 ID. Bài khối lượng lớn không phát hàng chục triệu message Kafka và chưa chứng minh Spark/PostgreSQL toàn tuyến của Mạnh/Bảo.

Code được CI nghiệm thu tại commit `1ca8874fc11838f5f891cf2986d533e06c6d01de`. [CI + hai tháng qua HTTPS](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42685403658) · [CI + cả năm qua HTTPS](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42685415227). Cả hai workflow đều thành công, gồm Windows, Ubuntu, Kafka và bài dữ liệu thật.

Bằng chứng JSON giữ nguyên từ máy/runner:

- [Hai lượt local hai tháng](evidence/tlc-local-2months-2026-11-10.json).
- [Hai tháng HTTPS Ubuntu](evidence/tlc-remote-2months-2026-11-10.json).
- [Cả năm HTTPS Ubuntu](evidence/tlc-remote-12months-2026-11-10.json).

## Lỗi mạng và cấu hình đã xử lý

Lượt HTTPS Windows đầu chạy đủ **10 bộ nhỏ** và xuất hai tháng xong, nhưng validator PyArrow thiếu IANA timezone database. [Báo cáo lỗi ban đầu](evidence/tlc-arrow-setup-failed-2026-11-10.json). Đã cài timezone database và bổ sung thiết lập tự động vào benchmark trên Windows; runtime DuckDB không cần bước này.

Lượt HTTPS tiếp theo đạt **7 bộ nhỏ rồi bộ thứ 8 lỗi HTTP 403** ở replay và lookup. [Giữ nguyên báo cáo lỗi](evidence/tlc-remote-failed-2026-11-10.json). Response là CloudFront `Request blocked`; chưa chứng minh nguyên nhân cụ thể. Hai workflow Ubuntu sau đó tải/xuất/đối chiếu thành công, nhưng điều đó **không chứng minh đã sửa 403 trên mạng Windows**. Đây là tỷ lệ thành công của lượt ingestion, không phải tỷ lệ của từng HTTP request bên trong httpfs.

Lỗi `INSTALL httpfs` đổi tên cache bị Access denied trên Windows được xử lý bằng tải extension có chữ ký từ kho HTTPS chính thức, LOAD từ cache riêng theo version/platform. Không bỏ kiểm tra chữ ký; bật xác thực TLS bằng certifi.

Cách vận hành để không phải tải lại nhiều lần:

1. Chạy ingestion một lần khi truy cập được TLC, hoặc khai báo `source.files` dùng Parquet tháng đã tải/cache. Với CSV lookup, cũng có thể khai báo đường dẫn local.
2. Batch/replay chỉ dùng Parquet/CSV đã sẵn sàng trong manifest; không gọi lại TLC cho mỗi lần demo.
3. Khi cần kỳ mới mà CDN chặn, lỗi phải được ghi rõ `failed`; không tự chuyển API/fixture hay retry vô hạn. Dùng môi trường truy cập được để tải bản gốc, rồi đưa về cache trước khi chạy local.

Trên workspace Windows này đã chuẩn bị `configs/tlc-cached.local.yaml` và kiểm tra CLI: dùng file tháng 1 thật trong cache + CSV thật, xuất 1.029/484/265 dòng và dry-run 484 sự kiện thành công, không cần gọi CDN. Config local/data/cache không commit lên Git.

## Chạy lại

```powershell
# HTTPS: hai tháng, hai lần COPY mới, 10 bộ nhỏ kèm replay lặp lại.
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --months 2 --large-repeats 2 --small-repeats 10 --report output/tlc-remote.json
# Local: cùng nguồn thật đã cache, tách riêng khỏi nghiệm thu HTTPS.
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --months 2 --large-repeats 2 --small-repeats 0 --source-mode local --report output/tlc-local.json
# HTTPS: cả năm 2025.
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --months 12 --large-repeats 1 --small-repeats 2 --report output/tlc-year.json
```

`--source-mode local` dùng file tháng trong `output/tlc-source-cache`; nếu chưa có thì bước chuẩn bị tải bản gốc vẫn cần mạng. Mỗi report ghi rõ chế độ, nguồn/hash, metrics và kết quả. CLI trả exit 1 khi thất bại. Workflow **Ingestion checks** cho phép chọn 2 hoặc 12 tháng khi chạy thủ công; artifact chỉ chứa báo cáo JSON, không chứa toàn bộ dataset.

Nguồn tham chiếu: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), [DuckDB Parquet](https://duckdb.org/docs/stable/data/parquet/overview), [DuckDB httpfs HTTPS](https://duckdb.org/docs/stable/core_extensions/httpfs/https).
