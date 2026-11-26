# 2.1 · Chuẩn hóa và router ba loại sự kiện

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Quy tắc dùng chung cho batch/stream; route request, pickup và dropoff, phân biệt các đại lượng.

## Hợp đồng dữ liệu và bàn giao

Schema v2, event time, scope/replay; khóa nghiệp vụ theo bảng contracts. Tạo bucket 15 giây, phút và 15 phút; demand_valid/match_eligible riêng. Dropoff đến trước pickup cần buffer/state quan hệ.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Timezone America/New_York, [start,end), DST chính sách rõ. Request lỗi tọa độ vẫn đếm nếu thời gian/vùng hợp lệ. Khử trùng event và nghiệp vụ bền vững; ghi admission metadata cho batch. Cùng clock, watermark/coverage và chính sách cách ly sau chốt.

## Khu vực code

- [pipeline/normalization.py](../../../src/nyc_taxi_pipeline/pipeline/normalization.py)
- [pipeline/stream/router.py](../../../src/nyc_taxi_pipeline/pipeline/stream/router.py)
- [shared/events.py](../../../src/nyc_taxi_pipeline/shared/events.py)
- [shared/windows.py](../../../src/nyc_taxi_pipeline/shared/windows.py)

## Tiêu chí hoàn thành

Đúng mốc biên, retry không tăng count; pickup không tăng request; đủ thành viên mới tăng completed_trip; state xuyên cửa sổ/ngày và restart.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
