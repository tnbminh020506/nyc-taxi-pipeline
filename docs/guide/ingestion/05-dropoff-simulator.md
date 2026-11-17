# Sơ đồ · Đồng hồ và simulator trả khách

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Đọc lịch bền vững do API/kho tạo và phát hoàn tất đúng hạn; không tự tạo trip.

## Hợp đồng dữ liệu và bàn giao

scheduled_events có event_id, replay_id, trip_id, request_id, due_at, payload, trạng thái gửi. dropoff_completed có completed_at, vùng/tọa độ trả khách, scheduled_event_id. due_at = thời điểm bắt đầu phục vụ + thời lượng nguồn của từng request.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Dùng shared/clock.py cho cả replay, timeout và scheduler; đồng hồ vẫn tiến khi không có request mới. Đọc/claim lịch qua warehouse/schedule_repository.py, gửi Kafka rồi ghi ack. Nếu crash sau gửi trước ack thì gửi lại cùng ID. Ghép hai request tạo hai lịch; không phát hoàn tất cho request đang chờ/hủy. Khi tương tác thủ công dùng 1×.

## Khu vực code

- [ingestion/dropoff_simulator.py](../../../src/nyc_taxi_pipeline/ingestion/dropoff_simulator.py)

## Tiêu chí hoàn thành

Restart không mất lịch; retry không sinh ID mới; không trả khách trước chốt chuyến; chuyến ghép có hai sự kiện riêng; thời lượng lỗi được cách ly theo cấu hình.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
