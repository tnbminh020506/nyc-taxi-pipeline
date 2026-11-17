# 3.2 · Lưu số đếm và snapshot cảnh báo

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Giao diện đọc mức tham chiếu, upsert tổng và chốt cửa sổ; không phát cảnh báo.

## Hợp đồng dữ liệu và bàn giao

demand_windows khóa (scope_id, zone_id, window_start), giữ replay_id nếu có, window_end, request_count, coverage, is_final, update_version và snapshot baseline. Scope phân biệt nguồn/phiên; snapshot chứa cả giá trị, loại, số mẫu và publication_version.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Gán tổng lũy kế thay vì cộng delta, từ chối cập nhật version cũ để tổng không lùi. Giữ snapshot lúc mở; retry không thay snapshot. Pipeline chỉ gọi upsert sau ack khi có cảnh báo. Chốt tổng cuối/coverage trong giao dịch, làm đầu vào batch kế tiếp; không tính lại baseline. Cửa sổ đủ độ phủ không request là 0; thiếu coverage không tạo 0 giả.

## Khu vực code

- [warehouse/demand_repository.py](../../../src/nyc_taxi_pipeline/warehouse/demand_repository.py)
- [warehouse/sql/load/demand_windows.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/demand_windows.sql)

## Tiêu chí hoàn thành

Retry không tăng tổng; cập nhật cũ không ghi đè; cửa sổ chốt bất biến theo chính sách; đối soát lấy được baseline đã dùng dù batch đã công bố phiên bản mới.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
