<!--
Đầu vào
Đề bài MVP, file TLC do nhóm chọn và kết quả bàn giao từ ba nhánh.
Đầu ra
Bản đồ repo, thứ tự tích hợp và tiêu chí nghiệm thu chung.
-->
# NYC Taxi Pipeline

MVP trả lời câu hỏi **Khu vực nào có nhiều chuyến đón khách hơn mức lịch sử trong cùng khung giờ?** Người xem chọn một phiên replay và một cửa sổ 15 phút, rồi đọc kết quả trên PostgreSQL.

Các tệp Python, SQL và cấu hình hiện chứa comment đặc tả đầu vào, đầu ra và yêu cầu xử lý. Nhóm dùng chúng để thống nhất hợp đồng trước khi triển khai.

| Phụ trách | Tệp bắt đầu | Bàn giao |
| --- | --- | --- |
| Chính · Ingestion | [Nhập dữ liệu](scripts/run_ingestion.py) · [Phát lại](scripts/replay_taxi_stream.py) | Parquet nguyên bản, Zone Lookup, [manifest](src/nyc_taxi_pipeline/ingestion/manifest.py) và sự kiện Kafka `taxi-events` |
| Mạnh · Spark | [Batch](scripts/run_batch.py) · [Stream](scripts/run_stream.py) | Mức lịch sử theo vùng, thứ và khung giờ. Tổng chuyến lũy kế theo phiên, vùng và cửa sổ. |
| Bảo · PostgreSQL | [Schema](database/schema.sql) · [Nạp dữ liệu](database/load/) · [Truy vấn demo](database/queries/compare_pickup_demand.sql) | Kết quả đối chiếu số chuyến quan sát với mức lịch sử, kèm số cửa sổ tham chiếu |
| Nhóm trưởng · Tích hợp | [Cấu hình demo](configs/demo.example.yaml) · [Môi trường cục bộ](docker-compose.yml) · [Fixture](data/fixtures/README.md) | Chốt mẫu TLC và hai kỳ dữ liệu tách biệt, nghiệm thu và ghi kết quả thực tế |

## Thứ tự tích hợp dự kiến

1. Chốt file nguồn, cột pickup, múi giờ của dữ liệu nguồn và hai kỳ lịch sử/replay.
2. Khởi tạo Kafka và PostgreSQL. Nhập Parquet, công bố manifest và nạp Zone Lookup.
3. Chạy Spark Batch trên tập `history` và nạp mức lịch sử.
4. Khởi động Spark Stream cho `replay_id` đã chọn, rồi phát tập `replay` vào Kafka.
5. Truy vấn phiên và cửa sổ muốn xem. Đối chiếu [các ca nghiệm thu](tests/test_pipeline_contract.py) và ghi vào [báo cáo MVP](docs/mvp-report.md).

Các mốc trao đổi và lưu kho có múi giờ. Thứ và khung giờ nghiệp vụ tính theo `America/New_York`. Cửa sổ lấy mốc đầu và loại mốc cuối.
