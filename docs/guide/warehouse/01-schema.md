# 3.1 · Schema phân tích và vận hành

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

DDL PostGIS, khóa/index cho ba batch/stream, không giữ mô hình demand_windows mơ hồ.

## Hợp đồng dữ liệu và bàn giao

Bảng/khóa chính xác theo contracts.md: batch_runs, batch_window_counts, stream_window_counts, stream_minute_counts, historical_baseline, demand_gradients, daily_pickup_averages; taxi_zones và request/proposal/trip/members/schedule/outbox/dedup/journal/receipt.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Timestamp timestamptz, counts nguyên không âm, trung bình thập phân; geography(Point,4326). Unique request-trip, một pickup/trip và lịch/request. Hai writer stream có version riêng. batch_version được truy vấn duy nhất, không cộng bản sao. Bảng phút giữ toàn bộ; khóa không nullable hoặc có quy tắc NULL rõ.

## Khu vực code

- [warehouse/connection.py](../../../src/nyc_taxi_pipeline/warehouse/connection.py)
- [warehouse/sql/schema.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/schema.sql)

## Tiêu chí hoàn thành

Grain phân biệt phút/15p/ngày, batch/stream riêng; thiếu lookup vẫn giữ số liệu; DDL đáp ứng idempotency và publication barrier.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
