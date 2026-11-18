# 1.4 · Kafka và phát pickup sau điều phối

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Producer dùng chung cho replay, pickup outbox và simulator; topic taxi-events gồm ba loại sự kiện.

## Hợp đồng dữ liệu và bàn giao

Schema v2: ride_requested, pickup_completed, dropoff_completed theo contracts.md. Pickup có trip_id và request_ids, một event/trip; nhận outbox đã commit từ Bảo, trả ack hoặc lỗi.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Tách serialize/gửi khỏi nghiệp vụ. Bổ sung pickup_publisher đọc/claim outbox, phát ngay sau chốt trip và ack. Retry giữ event_id; không tự tạo trip. Phối hợp Kafka key, endpoint host/container và taxi-alerts với Mạnh. Dropoff chỉ đủ điều kiện gửi sau ack pickup.

## Khu vực code

- [ingestion/kafka_publisher.py](../../../src/nyc_taxi_pipeline/ingestion/kafka_publisher.py)
- [ingestion/pickup_publisher.py](../../../src/nyc_taxi_pipeline/ingestion/pickup_publisher.py)

## Tiêu chí hoàn thành

Lỗi broker không báo thành công; restart không mất pickup; một cặp ghép chỉ một pickup nghiệp vụ dù gửi lại; router/archive đọc đủ ba loại.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
