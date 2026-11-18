# 3.3 · Đối soát batch–stream và dashboard

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

SQL đối soát độc lập hai view cùng cửa sổ; giữ truy vấn cảnh báo, xu hướng và vòng đời.

## Hợp đồng dữ liệu và bàn giao

Scope/replay, batch_version, zone, window và cutoff. Kết quả có năm count batch/stream, delta, equal/mismatch/pending, coverage/offsets; dashboard minute/60m và daily average riêng.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Full outer join để lộ dòng thiếu; chỉ so khi cùng tập sự kiện, cả hai final và archive đủ. Không mặc định thiếu thành 0. Mỗi count dùng zone/time theo contracts; trip hoàn tất tại lượt trả cuối. Baseline dùng snapshot đã giữ, ratio NULL nếu thiếu/0. View 60m so đúng khoảng 60m, không so một window 15m. Giữ trace request→trip→pickup→dropoff→HDFS; so bằng nhau chỉ chứng minh nhất quán số đếm.

## Khu vực code

- [warehouse/sql/queries/compare_pickup_demand.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/compare_pickup_demand.sql)
- [warehouse/sql/queries/demand_trends.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/demand_trends.sql)
- [warehouse/sql/queries/ride_lifecycle.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/ride_lifecycle.sql)
- [warehouse/sql/queries/reconcile_views.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/reconcile_views.sql)

## Tiêu chí hoàn thành

5 request/1 cặp/3 solo cùng khung ra 5,4,5,4,1 hai bên; pending chưa coi mismatch; trip qua nhiều khung/zone vẫn đúng; lookup thiếu không mất dòng.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
