# Sơ đồ · Archive sự kiện và snapshot cho batch

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Lưu ba loại sự kiện và journal xuống HDFS, cung cấp đầu vào batch độc lập với stream counts.

## Hợp đồng dữ liệu và bàn giao

Raw event v2 cùng Kafka topic/partition/offset; journal chuyển trạng thái; receipt HDFS committed và coverage, input_snapshot_id. Nhận metadata admission/watermark từ xử lý để batch áp dụng cùng tập hợp hợp lệ.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Ghi lô immutable rồi công bố receipt/checkpoint; raw có thể trùng. Snapshot cho batch không đọc file đang ghi. Không coi Kafka ack là archive ack. Duy trì nguồn/phiên, lưu cả quyết định loại sau chốt, chuyển journal qua outbox để không mất dấu vết.

## Khu vực code

- [ingestion/event_archive.py](../../../src/nyc_taxi_pipeline/ingestion/event_archive.py)

## Tiêu chí hoàn thành

Batch đọc lại đủ đặt/đón/trả; retry không mất sự kiện; lỗi HDFS chặn receipt; đủ metadata xác định cửa sổ pending/final và tái lập chính sách dữ liệu muộn.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
