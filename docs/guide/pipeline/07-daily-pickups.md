# 2.2 · Batch 3: trung bình lượt đón trong ngày

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Tính trung bình trip đón trên các khung có hoạt động trong ngày hiện tại; chỉ phục vụ dashboard.

## Hợp đồng dữ liệu và bàn giao

daily_pickup_averages: scope_id, zone_id, business_date, cutoff, batch_version, pickup_trip_sum, active_window_count, average_pickup_trips, is_day_final.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Đọc pickup_completed từ snapshot archive, khử trùng theo phiên/trip. Chọn khung 15 phút đã chốt từ đầu ngày có P>0, average=sum(P)/count(khung). Không lấy cùng slot qua nhiều ngày; không lấy khung đang mở. Stage cho tổng hợp cuối; không khung hợp lệ trả NULL/0 mẫu, ngày chưa xong tạm tính.

## Khu vực code

- [pipeline/batch/daily_pickup_average.py](../../../src/nyc_taxi_pipeline/pipeline/batch/daily_pickup_average.py)

## Tiêu chí hoàn thành

2,0,6,4 cho 4; 0 toàn ngày cho NULL; qua ngày reset phạm vi; ghép hai request chỉ một trip; không dùng chỉ số này kích hoạt cảnh báo.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
