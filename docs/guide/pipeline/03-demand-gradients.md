# 2.2 · Batch 2: gradient trong ngày

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Tính biến động request trong ngày hiện tại mỗi 15 phút.

## Hợp đồng dữ liệu và bàn giao

K khung liên tiếp đã chốt/đủ dữ liệu, cùng zone/scope/ngày; K mặc định 4 >=2. Trả K counts, K−1 gradients, series_end, business_date, cutoff/version hoặc insufficient_data.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

g=(count_sau-count_truoc)/15 yêu cầu/phút; không nối khung thiếu hoặc qua ngày trước; tính cả 0 đủ coverage. Stage kết quả, báo B2 cho bộ tổng hợp cuối. Stream chỉ đọc chuỗi đã công bố không ở tương lai.

## Khu vực code

- [pipeline/batch/demand_gradients.py](../../../src/nyc_taxi_pipeline/pipeline/batch/demand_gradients.py)

## Tiêu chí hoàn thành

20,35,65,50 cho +1,+2,−1; đổi K đúng số phần tử; đầu ngày thiếu K báo thiếu, không lấy ngày cũ bù.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
