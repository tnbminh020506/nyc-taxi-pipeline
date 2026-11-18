# Sơ đồ · Simulator trả khách sau pickup

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Đọc lịch bền vững của trip đã chốt; phát hoàn tất từng request, không tự tạo chuyến.

## Hợp đồng dữ liệu và bàn giao

event_id/replay_id/trip_id/request_id/due_at/payload/status; due_at=trip_started_at+source_duration_seconds. Chờ pickup outbox ack, giữ completed_at theo lịch chứ không dùng giờ retry.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Dùng clock chung vẫn tiến lúc vắng dữ liệu; claim lịch có hạn, retry cùng event_id, ghi ack sau Kafka. Trip ghép có hai lịch. Thời lượng lỗi cách ly; không hoàn tất request chờ/hủy; thời gian bắt đầu phục vụ bằng picked_up_at trong demo.

## Khu vực code

- [ingestion/dropoff_simulator.py](../../../src/nyc_taxi_pipeline/ingestion/dropoff_simulator.py)

## Tiêu chí hoàn thành

Restart lấy lại lịch chưa ack; không gửi trước pickup; hai dropoff có request khác nhau; không đánh dấu trip xong chỉ vì producer đã ack.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
