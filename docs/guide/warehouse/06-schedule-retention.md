# Sơ đồ · Outbox, hoàn tất và retention vận hành

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Bàn giao pickup outbox/dropoff schedule cho Chính; hoàn tất và dọn chi tiết có điều kiện.

## Hợp đồng dữ liệu và bàn giao

Claim/lease/ack với event_id ổn định, pickup ack trước lịch trả được phát; receipt raw/journal; dedup giữ đủ retry horizon. Lịch sử minute/batch/stream bị loại khỏi cleanup.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Producer ack không đồng nghĩa khách đã được trả; consumer nhận dropoff mới cập nhật thành viên, đủ người đóng trip. State/dedup cho sự kiện đảo thứ tự. Chỉ dọn vận hành đã kết thúc, aggregate bền vững, archive committed và lịch xử lý đủ. Giữ request chờ/gia hạn, trip chưa xong; không xóa bảng phân tích theo serving_horizon.

## Khu vực code

- [warehouse/schedule_repository.py](../../../src/nyc_taxi_pipeline/warehouse/schedule_repository.py)
- [warehouse/archive_repository.py](../../../src/nyc_taxi_pipeline/warehouse/archive_repository.py)
- [warehouse/retention.py](../../../src/nyc_taxi_pipeline/warehouse/retention.py)
- [warehouse/sql/operations/lifecycle.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/operations/lifecycle.sql)
- [warehouse/sql/operations/retention.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/operations/retention.sql)

## Tiêu chí hoàn thành

Restart không mất sự kiện; trả một người không đóng trip ghép; HDFS lỗi chặn dọn; replay sau dọn không hồi sinh trip; dữ liệu phút hơn 60m vẫn còn.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
