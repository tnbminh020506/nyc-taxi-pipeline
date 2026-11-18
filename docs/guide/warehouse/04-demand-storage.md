# 3.2 · Stream counts, phút và serving view

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Ghi totals 15 phút và bucket phút, cung cấp rolling 60 phút cho dashboard.

## Hợp đồng dữ liệu và bàn giao

stream_window_counts có cột request/snapshot do S1 ghi, lifecycle counts do S2 ghi; stream_minute_counts khóa (replay_id,zone_id,minute_start), version/finality. Không TTL lịch sử phút.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Upsert tổng tuyệt đối, từ chối version cũ và bảo vệ từng writer. Chốt 15 phút sau barrier hai writer/coverage; chốt không cập nhật baseline. Bucket phút gán theo event time, 15s sau cùng khóa thì cập nhật bản ghi cũ. Query SUM pickup trong [as_of−60m,as_of), left join đủ zone; phút thiếu coverage không biến thành zero. Phút mở provisional, sửa muộn trước chốt cho phép.

## Khu vực code

- [warehouse/demand_repository.py](../../../src/nyc_taxi_pipeline/warehouse/demand_repository.py)
- [warehouse/minute_repository.py](../../../src/nyc_taxi_pipeline/warehouse/minute_repository.py)
- [warehouse/sql/load/demand_windows.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/demand_windows.sql)
- [warehouse/sql/load/minute_counts.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/minute_counts.sql)
- [warehouse/sql/queries/traffic_dashboard.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/traffic_dashboard.sql)

## Tiêu chí hoàn thành

Delta 2,1,3,2 lưu một dòng cuối 8; retry không thành 10; hai writer không ghi đè nhau; sau 60p PostgreSQL còn dữ liệu cũ, serving chỉ lấy khoảng đúng.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
