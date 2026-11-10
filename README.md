<!--
Đầu vào
Đề bài MVP, file TLC do nhóm chọn và kết quả bàn giao từ ba nhánh.
Đầu ra
Bản đồ repo, thứ tự tích hợp và tiêu chí nghiệm thu chung.
-->
# NYC Taxi Pipeline

MVP trả lời câu hỏi **Khu vực nào có nhiều chuyến đón khách hơn mức lịch sử trong cùng khung giờ?** Người xem chọn một phiên replay và một cửa sổ 15 phút, rồi đọc kết quả trên PostgreSQL.

Đây là workspace giao việc. Các tệp Python, SQL và YAML chứa chú thích về người phụ trách, đầu vào, đầu ra và yêu cầu xử lý.

Thiết kế tích hợp dùng **HDFS chung** để bàn giao JSON gốc, CSV lookup, Parquet và manifest. Các bước đọc và ghi HDFS cần được triển khai trước khi ghép toàn tuyến.

Xem sơ đồ và yêu cầu đầu vào, đầu ra của từng bước trong [hướng dẫn triển khai pipeline taxi](docs/guide/giao-viec-demo-taxi.md).

## Mục tiêu từng giai đoạn

1. **Data Ingestion · Chính.** Lấy dữ liệu Yellow Taxi qua API theo hai kỳ đã chọn, lưu phản hồi gốc, Parquet và manifest trên HDFS. Đọc tập replay từ HDFS và phát các chuyến qua Kafka để Spark có thể đọc dần.
2. **Data Processing · Mạnh.** Tính mức lịch sử bằng Spark Batch và đếm số chuyến phát lại bằng Spark Structured Streaming. Dùng cùng quy tắc chọn chuyến hợp lệ và cửa sổ 15 phút để hai kết quả so sánh được.
3. **Data Warehouse · Bảo.** Nạp tên vùng và hai bộ kết quả vào PostgreSQL. Cung cấp truy vấn để người xem chọn một phiên và một cửa sổ, rồi xem số chuyến, mức lịch sử và phần trăm chênh lệch theo khu vực.

## Bản đồ công việc theo bước

| Bước | Phụ trách | File cần đọc hoặc triển khai | Bàn giao |
| --- | --- | --- | --- |
| **1.1** Lấy chuyến đi qua NYC Open Data API | Chính, nhóm trưởng chốt mẫu | [Cấu hình API](configs/demo.example.yaml) · [Tiếp nhận nguồn](src/nyc_taxi_pipeline/ingestion/ingest.py) | Các trang JSON và thông tin truy vấn cho hai kỳ `history` và `replay` |
| **1.2** Lưu phản hồi gốc, Parquet và manifest trên HDFS | Chính | [Điểm chạy nhập](scripts/run_ingestion.py) · [Lưu dữ liệu](src/nyc_taxi_pipeline/ingestion/ingest.py) · [Manifest](src/nyc_taxi_pipeline/ingestion/manifest.py) | URI HDFS của manifest, `parquet_path` cho batch và replay, `raw_path` của `zone_lookup` cho Bảo |
| **1.3** Đọc chuyến đi và tạo sự kiện | Chính | [Điểm chạy replay](scripts/replay_taxi_stream.py) · [Chương trình replay](src/nyc_taxi_pipeline/ingestion/replay.py) | Sự kiện pickup có mã phiên, mã sự kiện, giờ đón, vùng đón và file Parquet replay |
| **1.4** Đưa sự kiện vào Kafka | Chính, phối hợp nhóm trưởng | [Chương trình replay](src/nyc_taxi_pipeline/ingestion/replay.py) · [Môi trường Kafka và PostgreSQL](docker-compose.yml) | Topic `taxi-events` mà Spark có thể đọc |
| **2.1** Chuẩn hóa thời gian và dữ liệu | Mạnh, thống nhất với Chính và Bảo | [Quy tắc pickup](src/nyc_taxi_pipeline/processing/pickup.py) | Quy tắc hợp lệ và phân cửa sổ dùng chung cho batch và stream |
| **2.2** Tính mức lịch sử | Mạnh | [Điểm chạy batch](scripts/run_batch.py) · [Spark Batch](src/nyc_taxi_pipeline/pipelines/historical_baseline.py) | Mức lịch sử theo vùng, thứ và khung giờ, kèm số cửa sổ tham chiếu |
| **2.3** Đếm chuyến phát lại | Mạnh | [Điểm chạy stream](scripts/run_stream.py) · [Spark Structured Streaming](src/nyc_taxi_pipeline/pipelines/streaming_counts.py) | Tổng chuyến lũy kế theo phiên, vùng và cửa sổ |
| **3.1** Tạo bảng | Bảo | [Schema PostgreSQL](database/schema.sql) | Ba bảng và khóa để nạp đúng từng loại dữ liệu |
| **3.2** Nạp và cập nhật kết quả | Bảo, phối hợp Mạnh ở phần ghi stream | [Zone Lookup](database/load/zone_lookup.sql) · [Mức lịch sử](database/load/baseline.sql) · [Số đếm stream](database/load/replay_counts.sql) | Ba bảng có dữ liệu để truy vấn |
| **3.3** Truy vấn demo | Bảo | [Truy vấn so sánh](database/queries/compare_pickup_demand.sql) | Tên/mã vùng, số chuyến, mức lịch sử, số cửa sổ tham chiếu và % chênh lệch |
| **4.1** Kiểm tra trên bộ mẫu | Cả nhóm | [Đặc tả bộ mẫu](data/fixtures/README.md) · [Ca nghiệm thu](tests/test_pipeline_contract.py) | Kết quả đối chiếu dự kiến tại từng điểm bàn giao |
| **4.2** Chạy với dữ liệu lấy qua API và bàn giao | Cả nhóm, nhóm trưởng tổng hợp | [Cấu hình demo](configs/demo.example.yaml) · [Báo cáo MVP](docs/mvp-report.md) | Dataset, truy vấn API, phạm vi, lệnh chạy, log và kết quả SQL thực tế khi triển khai |

## Luồng bàn giao và thứ tự tích hợp dự kiến

```text
NYC Open Data API → HDFS dùng chung (JSON gốc, Parquet và manifest)
  ├─ history (Parquet) → Spark Batch → bảng mức lịch sử
  └─ replay (Parquet) → Python Replay → Kafka taxi-events → Spark Stream → bảng số chuyến
Taxi Zone Lookup (CSV) → HDFS (mục zone_lookup trong manifest) → bảng tra tên khu vực
Ba bảng trong PostgreSQL → SQL so sánh theo khu vực và khung 15 phút
```

1. Nhóm trưởng chốt dataset API, kỳ `history` và kỳ `replay`. Chính, Mạnh và Bảo cùng thống nhất schema Parquet, JSON sự kiện, bảng kết quả và vị trí bàn giao trong cấu hình demo.
2. Chuẩn bị HDFS dùng chung, Kafka và ba bảng PostgreSQL. Chính gọi API, lưu phản hồi và Parquet trên HDFS, công bố manifest và bàn giao mục `zone_lookup` cho Bảo nạp.
3. Mạnh chạy batch từ các mục `history` đã sẵn sàng. Bảo nạp kết quả của kỳ lịch sử đã chọn.
4. Mạnh khởi động stream theo mã phiên đã chốt, sau đó Chính phát tập `replay`. Mạnh chuyển tổng số chuyến của mỗi cửa sổ vào kho theo quy tắc nạp của Bảo.
5. Bảo truy vấn theo mã phiên và cửa sổ cần xem. Cả nhóm đối chiếu trên bộ mẫu trước, rồi chạy lại từ API và ghi riêng hai kết quả.

Ba người có thể chuẩn bị phần mình song song từ cùng một bộ mẫu và các đầu ra kỳ vọng. Các mốc trao đổi và lưu kho có múi giờ. Thứ và khung giờ nghiệp vụ tính theo `America/New_York`. Cửa sổ lấy mốc đầu và loại mốc cuối.

Mẫu đề xuất dùng dataset `irx2-vgm4`. Xem tên trường API và cách cấu hình endpoint trong tài liệu nguồn. [Dataset Yellow Taxi 2025](https://data.cityofnewyork.us/Transportation/2025-Yellow-Taxi-Trip-Data/irx2-vgm4/about_data) · [Tài liệu endpoint SODA](https://dev.socrata.com/docs/endpoints.html).
