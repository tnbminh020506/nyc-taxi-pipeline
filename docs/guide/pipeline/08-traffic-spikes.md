# 2.3 · Stream 2: lưu lượng và spike

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Giữ bucket request 15 giây để spike, bản ghi phút cho dashboard, view pickup 60 phút và số đếm vòng đời 15 phút.

## Hợp đồng dữ liệu và bàn giao

R(t) số request mới trong [t−15s,t); spike nếu R(t)>k*R(t−15s) AND R(t)>M, k>1,M>=0. minute key phiên–zone–minute_start; totals và update_version. View 60 phút tổng pickup; lifecycle writer ghi các count ngoài request của stream_window_counts.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Không dùng tổng phút cho spike. Khoảng trước 0 thì ratio NULL và xét M, thiếu coverage không giả 0. Giữ bucket mở đến watermark, dữ liệu muộn có thể kích hoạt một lần; ID ổn định, không thu hồi cảnh báo. Mỗi trigger gán tổng phút, ví dụ delta 2,1,3,2 → 2,3,6,8, retry không cộng lại. Serving tại mốc phút cộng [t−60m,t), loại bucket hết hạn khỏi state, không xóa PostgreSQL. Phút mở provisional, dữ liệu muộn cập nhật bucket cũ trước final. Đếm completed trip từ đủ dropoff thành viên, giữ state xuyên khung. Tất cả zone có dòng hoặc zero với coverage rõ.

## Khu vực code

- [pipeline/stream/traffic_view.py](../../../src/nyc_taxi_pipeline/pipeline/stream/traffic_view.py)
- [pipeline/stream/demand_spikes.py](../../../src/nyc_taxi_pipeline/pipeline/stream/demand_spikes.py)

## Tiêu chí hoàn thành

k=2,M=5: 3→7 cảnh báo, 3→6 không; 0→6 báo tăng từ 0; retry/stale write không đổi tổng; 61 phút dữ liệu chỉ 60 phút trong view nhưng PostgreSQL vẫn giữ đủ; trip ghép đếm 1 pickup/2 dropoff.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
