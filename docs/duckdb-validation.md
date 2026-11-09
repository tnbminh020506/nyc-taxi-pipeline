# Nghiệm thu DuckDB/TLC — 10/11/2026

Đã chuyển bước 1.1 của Chính từ Socrata API sang DuckDB đọc Parquet tháng chính thức của TLC. [Hướng dẫn chạy và hợp đồng](ingestion.md). Giữ schema bảy cột, manifest v1 và payload replay/Kafka.

## Phạm vi kiểm tra

- Thực sự COPY hàng triệu dòng thành Parquet; không dùng COUNT metadata làm bằng chứng xử lý dữ liệu lớn.
- Nguồn tháng 01–02/2025: 7.052.769 dòng vật lý; lọc đúng khoảng tháng được 7.052.716 dòng. Các timestamp sai tháng bị loại theo phạm vi, không tự sửa.
- Hai lần nhập mới từ bản nguồn thật đã cache cục bộ, so checksum đầu ra; đối chiếu PyArrow độc lập từng dòng gồm vị trí vật lý, UTC pickup, mã vùng và cờ hợp lệ. Cả hai lượt đều đạt; [JSON bằng chứng](evidence/tlc-local-2months-2026-11-10.json). Thời gian ingestion 9,688 và 9,625 giây; đối chiếu độc lập mất thêm 48,594 và 49,641 giây. RSS tối đa 634 MiB, spill tối đa 251 MiB; mỗi lượt tạo khoảng 69,13 MB Parquet. Đây là đọc nguồn cục bộ, không bao gồm tải mạng.
- Mục tiêu kiểm tra thêm cả năm 2025 qua HTTPS trên GitHub-hosted runner. Kết quả đang chờ; chưa coi là đạt.

## Lỗi mạng đã quan sát

Lượt HTTPS đầu chạy đủ 10 lần nhỏ (mỗi lần history + replay + lookup), xuất dữ liệu hai tháng thành công, nhưng validator PyArrow trên Windows thiếu IANA timezone database nên chưa nghiệm thu toàn bộ lượt đó. [Giữ báo cáo lượt đầu](evidence/tlc-arrow-setup-failed-2026-11-10.json). Đã cấu hình timezone database riêng cho Arrow; runtime DuckDB không cần bước này.

Lượt HTTPS tiếp theo đạt 7 bộ nhỏ rồi lượt thứ 8 lỗi HTTP 403 ở replay và lookup. Giữ nguyên [báo cáo lỗi](evidence/tlc-remote-failed-2026-11-10.json). Response là CloudFront `Request blocked`; chưa chứng minh được nguyên nhân cụ thể. **Không kết luận HTTPS TLC luôn ổn định trên mạng Windows hiện tại.**

DuckDB xử lý dữ liệu lớn và việc CDN cho phép truy cập là hai phần nghiệm thu riêng. Với file nguồn đã tải sẵn, `source.files` cho phép chạy hoàn toàn cục bộ. Với Parquet đầu ra đã sẵn sàng, batch/replay không cần truy cập TLC nữa. Không tự đổi sang API/fixture khi nguồn bị lỗi.

## Chạy lại

```powershell
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --months 2 --large-repeats 2 --small-repeats 0 --source-mode local --report output/tlc-local-2months.json
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --months 12 --large-repeats 1 --small-repeats 2 --report output/tlc-year.json
```

`--source-mode local` dùng file tháng trong `output/tlc-source-cache`; nếu chưa có thì bước chuẩn bị tải bản gốc vẫn cần mạng. Báo cáo ghi rõ chế độ, không dùng kết quả local để khẳng định HTTP thành công.

Cấu hình engine 512MB/2 threads, spill tối đa 8GB; báo cáo đo RSS tiến trình thực, không gọi memory_limit là trần RAM. Bài lớn không phát hàng triệu message Kafka; ca Kafka riêng kiểm tra payload, ACK và ID khi gửi lại. Chưa nghiệm thu Spark/PostgreSQL toàn tuyến của các thành viên khác.
