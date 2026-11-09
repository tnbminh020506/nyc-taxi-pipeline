<!--
Đầu vào
Đường dẫn tệp, lệnh chạy, log và kết quả kiểm tra thực tế của từng nhánh.
Đầu ra
Báo cáo bàn giao theo thành viên và trạng thái tích hợp của cả nhóm.
-->
# Bước 4.2 · Báo cáo MVP

| Phần | Phụ trách | Trạng thái trên nhánh demo/ingestion |
| --- | --- | --- |
| Bước 1.1–1.4 · Ingestion và replay | Chính | Đã triển khai trên `demo/ingestion`; bộ mẫu và kiểm thử cục bộ đạt; xem ghi chú bên dưới |
| Bước 2.1–2.3 · Spark Batch và Structured Streaming | Mạnh | Đã có đặc tả |
| Bước 3.1–3.3 · PostgreSQL và SQL demo | Bảo | Đã có đặc tả |
| Bước 4.1–4.2 · Chốt dữ liệu và ghép demo | Nhóm trưởng | Đã có kịch bản nghiệm thu |

Mỗi thành viên ghi phần đã làm kèm đường dẫn tệp, luồng và hợp đồng bàn giao, lệnh chạy và kết quả thực tế, phần còn vướng và việc cần phối hợp.

Nhóm trưởng ghi dataset và bộ lọc API, số trang và số dòng, Zone Lookup, kỳ lịch sử, kỳ replay, `replay_id` và kết quả SQL của cửa sổ demo. Ghi riêng kết quả trên bộ mẫu và kết quả lấy dữ liệu từ API thật.

## Bàn giao Chính — Data Ingestion

- Nhánh: `demo/ingestion`, tạo từ `origin/demo/mvp`.
- Mã: `src/nyc_taxi_pipeline/ingestion/` và hai script `scripts/run_ingestion.py`, `scripts/replay_taxi_stream.py`.
- Hướng dẫn và hợp đồng: [docs/ingestion.md](ingestion.md). Kafka độc lập: `docker-compose.ingestion.yml`.
- Bộ mẫu: `python scripts/run_ingestion.py --config configs/fixture.yaml` tạo history 4 dòng (1 + 3 chuyến ở hai cửa sổ), replay 4 dòng, lookup 1 vùng (161), ba mục manifest `ready`.
- `python scripts/replay_taxi_stream.py --config configs/fixture.yaml --dry-run --output output/fixture-events.jsonl` tạo 4 sự kiện, đúng thứ tự 08:01, 08:05, 08:14, 08:15 giờ New York.
- Kiểm thử cục bộ: `python -m pytest -q` đạt 41 ca, 1 ca Kafka skip vì máy chưa có broker. Có ca phân trang, resume/checksum, retry 429/500/503 và đứt body, tạo lại kết nối sau lỗi, ngân sách chờ/Retry-After, dữ liệu lỗi, gửi trùng, chống nghiệm thu giả bằng fixture/mock endpoint, tổng hợp metric và import artifact offline. CI chạy 41 ca trên cả Windows/Ubuntu và 1 ca Kafka thật bằng `docker-compose.ingestion.yml`; xem [các lượt chạy của nhánh](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/workflows/ingestion.yml?query=branch%3Ademo%2Fingestion).
- Đã xác nhận dữ liệu API thật qua SODA v2 trên GitHub Actions ngày 09/11/2026: history 63 dòng, replay 34 dòng, lookup 265 vùng. Hai tập có số dòng Parquet khớp với `COUNT(*)` độc lập từ API; replay tạo 34 sự kiện hợp lệ. [Bằng chứng lượt chạy thành công, attempt 2](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42626510859/attempts/2). Chi tiết phạm vi và giới hạn ở mục bên dưới.
- Đo ổn định sau sửa: **20/20 lượt tải mới đạt, 110/110 lời gọi API hoàn tất sau phục hồi**; thành công ngay chỉ 55,45%. Lỗi 500/503/ReadTimeout của nguồn vẫn có. Đợt trước chỉ đạt 19/20 được giữ trong báo cáo, không xóa lượt lỗi. [Phương pháp, thời gian và toàn bộ bằng chứng](api-reliability.md).
- SODA v3 chưa nghiệm thu vì thiếu credential: POST đúng endpoint trên runner trả JSON `authentication_required`. Trên Windows, metadata, v2 và các trang tài khoản trả HTML 403; không kết luận chỉ thêm token sẽ sửa hết lỗi. **Default demo hiện là v2** công khai. V3 vẫn được hỗ trợ khi có token hợp lệ.
- Chưa kiểm tra Spark/PostgreSQL toàn tuyến vì phần xử lý và kho đang là đặc tả. Mạnh/Bảo dùng manifest, schema Parquet và JSON sự kiện đã ghi trong hướng dẫn để nối phần mình.

### Kết quả nghiệm thu API thật — 09/11/2026

Dataset: `irx2-vgm4`. Endpoint: `https://data.cityofnewyork.us/resource/irx2-vgm4.json` (SODA v2, không app token).
Lệnh: `python scripts/test_live_api.py --api-version 2`; cấu hình `configs/api-smoke.yaml`.
Môi trường tải thành công: GitHub-hosted Ubuntu runner. Đây là phép thử kết nối/nhập liệu với hai kỳ một phút,
không phải phạm vi lịch sử/replay 15 phút chính thức do nhóm trưởng chốt.

| Vai trò | Phạm vi giờ New York `[start, end)` | Số trang | API COUNT(*) | Dòng Parquet | Dòng lỗi |
| --- | --- | --- | --- | --- | --- |
| history | 06/01/2025 08:00–08:01 (-05:00) | 1 | 63 | 63 | 0 |
| replay | 20/01/2025 08:00–08:01 (-05:00) | 1 | 34 | 34 | 0 |

Lookup: 265 dòng, giữ CSV gốc; manifest có ba mục `ready`. Replay dry-run đọc 34 dòng, tạo 34 sự kiện,
không có dòng lỗi hoặc ngoài phạm vi. Artifact gồm JSON gốc, query, kết quả count, Parquet, manifest,
`report.json` và JSONL replay. Artifact trên Actions giữ 7 ngày; số liệu và liên kết lượt chạy được giữ tại báo cáo này.

Lượt đầu history gặp HTTP 503 sau các lần retry, replay đã tải được 34 dòng. Chạy lại attempt 2 thì cả hai kỳ
và kiểm tra count đều đạt. Đây là lỗi dịch vụ tạm thời quan sát được, khác với HTTP 403 trên máy Windows.
Đã tải artifact về Windows, kiểm tra lại checksum hai file Parquet/CSV, ánh xạ đường dẫn manifest sang máy này
và tạo lại thành công 34 sự kiện qua chính module replay. Không coi kết quả này là đã sửa lỗi gọi API từ Windows
hoặc đã xác nhận SODA v3. Hướng dẫn tạo token và chạy lại v3 nằm trong [ingestion.md](ingestion.md).

### Mẫu demo đủ cửa sổ 15 phút

Đã chạy thêm `configs/demo.example.yaml`, v2, page_size=1000, history 06/01/2025
và replay 20/01/2025, cả hai 08:00–08:15 New York. Kết quả: history **1.029**,
replay **484**, lookup **265**; đối chiếu COUNT(*) và replay đạt. Thời gian
**60,749 giây**. [Lượt chạy](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42642938761).

Artifact đã được nhập về Windows bằng `scripts/import_api_artifact.py`, kiểm tra
checksum/số dòng, tạo manifest đường dẫn cục bộ và xác nhận lại 484 event_id.
Có thể dùng file đã tải để ghép với Mạnh/Bảo mà không gọi API lại mỗi lần replay.
Phạm vi này là mẫu đề xuất với một cửa sổ lịch sử, cần nhóm trưởng chốt/thêm kỳ
tham chiếu nếu demo chính thức cần nhiều mẫu. Chưa nghiệm thu Spark/PostgreSQL.
