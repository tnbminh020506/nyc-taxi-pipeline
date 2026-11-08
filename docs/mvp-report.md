<!--
Đầu vào
Đường dẫn tệp, lệnh chạy, log và kết quả kiểm tra thực tế của từng nhánh.
Đầu ra
Báo cáo bàn giao theo thành viên và trạng thái tích hợp của cả nhóm.
-->
# Bước 4.2 · Báo cáo MVP

| Phần | Phụ trách | Trạng thái |
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
- Kiểm thử cục bộ: `python -m pytest -q` đạt 27 ca, 1 ca Kafka skip vì máy chưa có broker. Có ca HTTP v3 phân trang, v2 cấu hình tường minh, retry 429, thiếu trang, giới hạn lượt tải, dữ liệu lỗi, gửi trùng và kiểm tra checksum. CI chạy 27 ca trên cả Windows/Ubuntu và 1 ca Kafka thật bằng chính `docker-compose.ingestion.yml`; xem [các lượt chạy của nhánh](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/workflows/ingestion.yml?query=branch%3Ademo%2Fingestion) để đối chiếu trạng thái từng commit.
- Chưa xác nhận lượt lấy dữ liệu API thật: môi trường hiện không có `SOCRATA_APP_TOKEN`; thử truy cập metadata/endpoint v2 công khai nhận HTTP 403. Không tự đổi nguồn hoặc coi fixture là dữ liệu API thật.
- Chưa kiểm tra Spark/PostgreSQL toàn tuyến vì phần xử lý và kho đang là đặc tả. Mạnh/Bảo dùng manifest, schema Parquet và JSON sự kiện đã ghi trong hướng dẫn để nối phần mình.
