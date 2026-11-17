# 2.1 · Chuẩn hóa, khử trùng và phân luồng

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Quy tắc chung cho batch/stream; phân loại tính nhu cầu và ghép riêng.

## Hợp đồng dữ liệu và bàn giao

Nhận Parquet hoặc event version đã hỗ trợ. Trả requested_at chuẩn, zone, window_start/end, iso_weekday 1–7, slot_15m 0–95, demand_valid, match_eligible và lý do lỗi. Khóa event theo replay_id/event_id; request chỉ đóng góp một lần.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Dùng America/New_York và cửa sổ [start,end), phân biệt UTC instant với nhãn slot địa phương, xử lý giờ DST theo chính sách nguồn rõ ràng. Chỉ ride_requested tăng nhu cầu; dropoff đi nhánh vòng đời. Không loại nhu cầu chỉ vì tọa độ/thời lượng ghép lỗi. Checkpoint và sổ khử trùng bền vững theo phiên; dữ liệu sau chốt ghi cách ly. Tách trigger khỏi cửa sổ nghiệp vụ.

## Khu vực code

- [pipeline/normalization.py](../../../src/nyc_taxi_pipeline/pipeline/normalization.py)
- [pipeline/stream/router.py](../../../src/nyc_taxi_pipeline/pipeline/stream/router.py)
- [shared/events.py](../../../src/nyc_taxi_pipeline/shared/events.py)
- [shared/windows.py](../../../src/nyc_taxi_pipeline/shared/windows.py)

## Tiêu chí hoàn thành

08:15 vào khung mới; trùng request không tăng tổng; dropoff không tăng nhu cầu; dữ liệu không đủ ghép vẫn được đếm nếu hợp lệ; cùng đầu vào batch/stream ra cùng cửa sổ.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
