# 2.2 mở rộng · Batch 2: gradient

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Tính xu hướng độc lập với điều kiện điểm nóng.

## Hợp đồng dữ liệu và bàn giao

Nhận K cửa sổ 15 phút liên tiếp, đã chốt, đủ độ phủ, cùng vùng/phạm vi; K=trend_window_count, mặc định 4, tối thiểu 2. Trả K số đếm và K−1 gradient, mốc cửa sổ, K, scope_id, batch_version, cutoff và series_end.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Từ snapshot nhất quán, tính (count_next-count_previous)/15 với đơn vị yêu cầu/phút. Không nối qua cửa sổ thiếu, không thay thiếu bằng 0; thiếu K báo trạng thái chưa đủ. Lưu qua batch_repository; chạy mỗi 15 phút. Stream chỉ đọc series_end không muộn hơn thời điểm xử lý.

## Khu vực code

- [pipeline/batch/demand_gradients.py](../../../src/nyc_taxi_pipeline/pipeline/batch/demand_gradients.py)

## Tiêu chí hoàn thành

20,35,65,50 cho +1,+2,−1; đổi K dùng đúng số phần tử; thiếu khung không tạo chuỗi giả; có phiên bản và mốc dữ liệu để đối soát.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
