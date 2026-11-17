# Sơ đồ · Lưu sự kiện raw vào HDFS

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Consumer riêng lưu cả hai loại sự kiện và dữ liệu truy vết; cung cấp xác nhận lưu cho kho.

## Hợp đồng dữ liệu và bàn giao

Đầu vào Kafka cùng topic/partition/offset, event_id, replay_id; thêm lịch sử chuyển trạng thái bền vững từ kho. Đầu ra raw HDFS và receipt gồm archive_id, path, phạm vi offset/ID, trạng thái commit. Raw được phép trùng; receipt chỉ xác nhận phần đã ghi bền vững.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Ghi theo lô vào đường dẫn phân vùng nguồn/phiên/ngày; commit dữ liệu rồi mới checkpoint offset và ghi receipt. Restart có thể đọc lại, giữ định danh truy vết. Lưu trạng thái API qua outbox/journal kho để không mất gia hạn/từ chối. Thống nhất receipt với archive_repository.py; Kafka nhận không đồng nghĩa HDFS đã lưu.

## Khu vực code

- [ingestion/event_archive.py](../../../src/nyc_taxi_pipeline/ingestion/event_archive.py)

## Tiêu chí hoàn thành

Đọc lại được request, dropoff và lịch sử xác nhận/từ chối; lỗi HDFS không tạo receipt thành công; retry không mất raw; kho kiểm tra receipt trước dọn.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
