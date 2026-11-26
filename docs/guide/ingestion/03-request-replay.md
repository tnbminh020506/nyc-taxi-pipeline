# 1.3 · Replay yêu cầu và tọa độ mô phỏng

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Chuyển Parquet replay hoặc dữ liệu simulator thành ride_requested; không tự chốt chuyến.

## Hợp đồng dữ liệu và bàn giao

Dùng hợp đồng sự kiện tại ../contracts.md. request_id ổn định theo phiên và dòng nguồn; event_id ổn định khi retry. requested_at lấy pickup nguồn; giữ dropoff/thời lượng; tọa độ WGS84 và cờ tọa độ mô phỏng.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Lọc đúng phạm vi replay, sắp theo thời gian và định danh ổn định. Seed tọa độ theo dòng nguồn, sinh điểm đón và đến trong polygon tương ứng; CSV tên vùng không đủ sinh tọa độ. Dùng đồng hồ chung; lỗi tọa độ/thời lượng ghi riêng, không loại khỏi đếm nhu cầu nếu thời gian/vùng đón vẫn hợp lệ.

## Khu vực code

- [ingestion/replay.py](../../../src/nyc_taxi_pipeline/ingestion/replay.py)
- [ingestion/coordinates.py](../../../src/nyc_taxi_pipeline/ingestion/coordinates.py)
- [shared/clock.py](../../../src/nyc_taxi_pipeline/shared/clock.py)

## Tiêu chí hoàn thành

Replay lại cùng nguồn/seed cho cùng tọa độ và ID; hai dòng giống giá trị vẫn là hai request; thời gian có múi giờ; payload được router đọc; nguồn mới dùng cùng schema.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
