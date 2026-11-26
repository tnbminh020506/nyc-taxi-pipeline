# 1.1 · Thu thập NYC và nguồn vùng

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Lấy nguồn history/replay và dữ liệu vùng; bàn giao nguồn đầy đủ, chưa xử lý nhu cầu.

## Hợp đồng dữ liệu và bàn giao

Nhận endpoint, dataset, ánh xạ pickup/dropoff, hai khoảng [range_start, range_end) không giao nhau. Trả trang JSON gốc và metadata truy vấn, số trang/dòng, lỗi; lookup gồm LocationID, Zone, Borough; ranh giới vùng là nguồn riêng với phiên bản/CRS.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Phân trang theo thứ tự ổn định; retry có giới hạn, ghi lại truy vấn và tiến độ. Xác minh trường thực tế trước khi tải. Giữ định danh dòng ổn định để phân biệt hai dòng trùng giá trị. Giữ cả pickup, dropoff và thời lượng nguồn; không coi dữ liệu chuyến NYC là yêu cầu đặt xe thực tế.

## Khu vực code

- [ingestion/source_api.py](../../../src/nyc_taxi_pipeline/ingestion/source_api.py)
- [ingestion/zone_sources.py](../../../src/nyc_taxi_pipeline/ingestion/zone_sources.py)

## Tiêu chí hoàn thành

Tải lại không mất hoặc nhân đôi dòng tại ranh giới trang; lỗi/giới hạn tải có trạng thái rõ; hai kỳ tách biệt và giữ được thông tin điểm đến.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
