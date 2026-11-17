# 3.1 · Schema PostgreSQL/PostGIS

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Sở hữu DDL, khóa, index và kiểu dữ liệu cho phân tích/vận hành.

## Hợp đồng dữ liệu và bàn giao

Bảng đích: taxi_zones, demand_windows, historical_baseline, demand_gradients, ride_requests, match_proposals, rejected_pairs, trips, trip_requests, scheduled_events; thêm dedup ledger, transition journal và archive receipts. Xem ../contracts.md cho khóa liên module.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Timestamp dùng timestamptz, trung bình/gradient dạng thập phân, số đếm nguyên không âm. Tọa độ geography(Point,4326), index không gian. Ràng buộc một request chỉ vào một trip và một đề xuất hiệu lực. demand_windows thay replay_pickup_counts/baseline_history_windows, không giữ hai nguồn cùng đóng góp batch. Cho phép mã vùng chưa có tên lookup.

## Khu vực code

- [warehouse/sql/schema.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/schema.sql)
- [warehouse/connection.py](../../../src/nyc_taxi_pipeline/warehouse/connection.py)

## Tiêu chí hoàn thành

DDL tạo đủ bảng/index/ràng buộc; lỗi FK/unique có xử lý; các module thống nhất khóa/version; không còn cơ chế chốt SQL tự cập nhật baseline.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
