# 2.3 · Stream 1: đếm và cảnh báo

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Quản lý số đếm/cửa sổ, snapshot baseline và phát taxi-alerts.

## Hợp đồng dữ liệu và bàn giao

Nhận request hợp lệ chưa đếm; đọc cặp baseline và gradient qua warehouse. Ghi demand_windows bằng tổng lũy kế và snapshot hai baseline. Alert có phiên/vùng/cửa sổ/count, các điều kiện vượt, giá trị/số mẫu/version, gradient/K/version/series_end hoặc lý do thiếu.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Giữ baseline bền vững khi mở cửa sổ, kể cả restart. Điều kiện prev <= baseline < current; ID từng điều kiện ổn định theo phiên/vùng/window/type. Cùng lần vượt hai mức gộp thông điệp. Chờ Kafka ack trước upsert số đếm tương ứng; retry cùng ID và tổng. Thiếu một mức vẫn xét mức kia; 0 kích hoạt request đầu. Chốt sau watermark và đủ độ phủ; replay kết thúc chỉ chốt khi xử lý hết. Chốt không tính lại baseline.

## Khu vực code

- [pipeline/stream/demand_alerts.py](../../../src/nyc_taxi_pipeline/pipeline/stream/demand_alerts.py)

## Tiêu chí hoàn thành

Mức 20/25 báo ở 21/26, không báo lặp; Kafka lỗi chưa ghi tổng vượt ngưỡng; restart giữ snapshot; gradient thiếu vẫn cảnh báo; ghi tổng idempotent và cuối cùng chốt đúng.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
