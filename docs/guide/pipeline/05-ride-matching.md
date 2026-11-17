# 2.3 mở rộng · Stream 2: tìm cặp và vòng đời

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Điều phối tìm cặp, timeout và nhận trả khách; kho sở hữu khóa/giao dịch.

## Hợp đồng dữ liệu và bàn giao

Nhận ride_requested đủ ghép hoặc dropoff_completed. Truy vấn ứng viên qua matching_repository; đề xuất đúng hai request cùng phiên, khoảng cách đón và đến đều <=500m. Các thao tác dùng expected_version và thời gian mô phỏng.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Xét người chờ lâu nhất, rồi khoảng cách đón/đến, thời điểm đặt và ID. Zone không phải điều kiện ghép. Kho khóa lại khi giữ cặp; bỏ cặp đã chủ động từ chối. Hạn riêng ban đầu 3 phút, tạo đề xuất không reset. Timeout hủy đề xuất, người còn hạn tiếp tục, người hết hạn chờ gia hạn; không tự đi riêng. Dropoff cập nhật từng người, đủ cả hai mới hoàn tất trip; không tăng nhu cầu.

## Khu vực code

- [pipeline/stream/ride_matching.py](../../../src/nyc_taxi_pipeline/pipeline/stream/ride_matching.py)
- [pipeline/stream/lifecycle.py](../../../src/nyc_taxi_pipeline/pipeline/stream/lifecycle.py)

## Tiêu chí hoàn thành

Khác zone vẫn ghép nếu đủ gần; không giữ một request trong hai cặp; timeout vẫn chạy khi stream vắng; hoàn tất một người chưa đóng trip ghép; retry không hồi sinh trip.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
