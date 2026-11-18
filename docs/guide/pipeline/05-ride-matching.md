# 2.3 · Stream 3: ghép và điều phối

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Tìm cặp bằng cache/PostGIS; điều phối các request đã có đồng ý phía người dùng; xử lý vòng đời.

## Hợp đồng dữ liệu và bàn giao

Cùng phiên, request còn hạn, consent_reference hợp lệ, hai khoảng cách <=500m. commit_dispatch trả trip_id và pickup outbox; pickup một trip có một/hai request. Dropoff theo từng request.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Cache không quyết định bền vững; kho khóa/kiểm tra lại trạng thái và consent rồi tạo trip/outbox/lịch nguyên tử. Không thêm bước hỏi lại. Chọn ứng viên ổn định theo thời gian chờ, khoảng cách, ID; zone không là điều kiện. Hạn 3 phút riêng; hết hạn chờ gia hạn, không tự solo. Cặp từ chối không đề xuất lại; timeout khác từ chối. Hoàn tất từng người, đủ thành viên mới đóng trip.

## Khu vực code

- [pipeline/stream/ride_matching.py](../../../src/nyc_taxi_pipeline/pipeline/stream/ride_matching.py)
- [pipeline/stream/lifecycle.py](../../../src/nyc_taxi_pipeline/pipeline/stream/lifecycle.py)

## Tiêu chí hoàn thành

Hai request ghép thành một pickup, hai dropoff; concurrent/retry không tạo hai trip; timeout vẫn tiến khi vắng sự kiện; không tăng request khi pickup/dropoff.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
