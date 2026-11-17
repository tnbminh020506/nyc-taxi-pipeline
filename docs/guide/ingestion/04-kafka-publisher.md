# 1.4 · Phát sự kiện Kafka

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Producer taxi-events và cấu hình topic; phối hợp Mạnh về taxi-alerts.

## Hợp đồng dữ liệu và bàn giao

Nhận event đã chuẩn hóa, trả ack hoặc lỗi gửi. taxi-events chứa ride_requested/dropoff_completed; giữ replay_id, schema_version và event_id. taxi-alerts do pipeline cảnh báo phát.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Tách serialization và gửi khỏi replay/scheduler. Chỉ báo gửi thành công sau ack; retry giữ ID. Chốt partition key phù hợp thứ tự theo request trong phiên; consumer vẫn phải khử trùng. Ghi số gửi/thử lại/lỗi; cấu hình bootstrap servers nhìn từ host/container trong docker-compose.yml.

## Khu vực code

- [ingestion/kafka_publisher.py](../../../src/nyc_taxi_pipeline/ingestion/kafka_publisher.py)

## Tiêu chí hoàn thành

Payload đọc được từ Kafka; lỗi broker không bị báo thành công; gửi lại giữ ID; có hướng dẫn endpoint và topic để Mạnh kết nối.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
