# Sơ đồ · Lịch hoàn tất, lưu trữ và dọn dữ liệu

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Bàn giao lịch cho Chính; cập nhật hoàn tất, receipt HDFS và chính sách retention.

## Hợp đồng dữ liệu và bàn giao

scheduled_events khóa event_id, một lịch hoàn tất cho mỗi thành viên trip; claim/status/ack cho retry. Nhận dropoff_completed, archive receipt và journal ID đã lưu. Ledger giữ event/request đã xử lý đủ lâu theo retry horizon.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Lịch tạo cùng transaction chốt trip; worker claim có hạn để phục hồi khi crash. Ack gửi không đồng nghĩa trip hoàn tất: chỉ sự kiện được xử lý mới cập nhật từng người. Chỉ dọn khi số liệu bền vững, raw/journal có receipt HDFS, lịch xong và dedup vẫn ngăn retry hồi sinh. Xóa theo quan hệ, không dọn request chờ/gia hạn hoặc trip chưa hoàn tất. Retention vận hành, tổng hợp và dedup cấu hình riêng.

## Khu vực code

- [warehouse/schedule_repository.py](../../../src/nyc_taxi_pipeline/warehouse/schedule_repository.py)
- [warehouse/archive_repository.py](../../../src/nyc_taxi_pipeline/warehouse/archive_repository.py)
- [warehouse/retention.py](../../../src/nyc_taxi_pipeline/warehouse/retention.py)
- [warehouse/sql/operations/lifecycle.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/operations/lifecycle.sql)
- [warehouse/sql/operations/retention.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/operations/retention.sql)

## Tiêu chí hoàn thành

Restart lấy lại lịch chưa ack; trả một người không đóng trip ghép; HDFS lỗi chặn dọn; retry sau dọn không tạo trip hay tăng nhu cầu; request hoạt động được giữ.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
