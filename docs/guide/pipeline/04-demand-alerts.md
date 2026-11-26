# 2.3 · Stream 1: cảnh báo baseline

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Đếm request 15 phút và cảnh báo vượt baseline; không sở hữu các cột đếm vòng đời.

## Hợp đồng dữ liệu và bàn giao

stream_window_counts: request_count, snapshot hai baseline, request_update_version. Alert baseline_crossing gồm conditions và trend/version; ID theo phiên–zone–window–loại baseline.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Giữ snapshot khi mở khung qua restart; prev<=baseline<current. Thiếu một mức vẫn xét mức kia, 0 kích hoạt request đầu. Kafka ack trước ghi request_count tương ứng; gán tổng tuyệt đối, không cộng delta. Gọi repository chỉ cập nhật cột thuộc mình để Stream 2 không ghi đè. Chốt sau watermark/coverage và cả hai writer hoàn tất.

## Khu vực code

- [pipeline/stream/demand_alerts.py](../../../src/nyc_taxi_pipeline/pipeline/stream/demand_alerts.py)

## Tiêu chí hoàn thành

20/25 báo ở 21/26; cùng lần vượt cả hai gộp; retry không báo nghiệp vụ lặp; Kafka lỗi chưa ghi request_count vượt ngưỡng; snapshot không đổi giữa khung.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
