# 3.2 · Công bố sau ba batch

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Quản lý staging, batch_runs và transaction công bố cuối; không tự tính nghiệp vụ batch.

## Hợp đồng dữ liệu và bàn giao

begin_batch_run, stage_baselines/gradients/daily_pickups, publish_batch_run(version,window_counts); snapshot/cutoff/coverage, trạng thái từng nhánh.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Kiểm tra schema/khóa/phạm vi của cả ba nhánh và summary; transaction công bố version sau cùng. Insufficient_data hợp lệ có trạng thái rõ, lỗi job rollback. Giữ các grain riêng trong bảng theo hợp đồng, stream đọc cặp baseline cùng version phù hợp applies_on. Không đọc stream totals làm baseline đối chứng.

## Khu vực code

- [warehouse/batch_repository.py](../../../src/nyc_taxi_pipeline/warehouse/batch_repository.py)
- [warehouse/sql/load/baseline.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/baseline.sql)
- [warehouse/sql/load/gradients.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/gradients.sql)
- [warehouse/sql/load/daily_pickups.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/daily_pickups.sql)
- [warehouse/sql/load/batch_windows.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/batch_windows.sql)

## Tiêu chí hoàn thành

Không thấy nửa bộ; retry không nhân dữ liệu; summary nằm sau ba nhánh; giữ lịch sử phiên bản và truy vấn đúng một version.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
