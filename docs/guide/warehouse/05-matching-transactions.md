# 3.2 mở rộng · PostGIS và giao dịch ghép

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Sở hữu truy vấn bán kính, khóa cặp và chuyển trạng thái nguyên tử cho stream/API.

## Hợp đồng dữ liệu và bàn giao

Request khóa (replay_id, request_id), proposal có hai thành viên và version; rejected_pairs khóa phiên cùng cặp ID đã sắp thứ tự. API truyền idempotency_key, expected_version và bằng chứng đồng ý. Trả proposal/trip hiện hành hoặc xung đột.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Dùng ST_DWithin trên geography cho cả điểm đón và đến, đơn vị mét, không lọc theo zone. Khóa request theo thứ tự ổn định, kiểm tra trạng thái/hạn lại trong transaction. Từ chối ghi cặp A–B không thứ tự; hết hạn không ghi từ chối. Xác nhận/đi riêng tạo trips, trip_requests và scheduled_events trong cùng giao dịch; đồng ý phải đủ hai người. Lưu journal mọi chuyển trạng thái, giải phóng đối tác khi đề xuất hủy.

## Khu vực code

- [warehouse/matching_repository.py](../../../src/nyc_taxi_pipeline/warehouse/matching_repository.py)
- [warehouse/sql/operations/matching.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/operations/matching.sql)

## Tiêu chí hoàn thành

Hai người đồng ý mới có trip ghép; concurrent confirm/timeout/solo chỉ một kết quả; retry trả cùng trip; cả hai khoảng cách <=500m; người còn hạn tiếp tục tìm.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
