# 3.2 · Nạp bảng tra và dữ liệu vùng

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Nạp tên vùng và hình học phục vụ truy vấn; Chính bàn giao nguồn HDFS.

## Hợp đồng dữ liệu và bàn giao

Nhận manifest zone_lookup/zone_boundaries ready, raw_path, source_version, CRS. Map LocationID -> zone_id, Zone -> zone_name, Borough -> borough. Khóa taxi_zones là zone_id.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Python đọc HDFS, kiểm tra schema rồi nạp staging và upsert qua SQL; PostgreSQL không tự đọc URI HDFS bằng COPY. Kiểm tra CRS/geometry khi nạp ranh giới. Không loại số liệu chỉ vì thiếu tên vùng; đối soát dùng left join.

## Khu vực code

- [warehouse/zone_loader.py](../../../src/nyc_taxi_pipeline/warehouse/zone_loader.py)
- [warehouse/sql/load/zone_lookup.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/zone_lookup.sql)

## Tiêu chí hoàn thành

Nạp lại giữ kết quả; lỗi nguồn có log; tên/geometry truy được nguồn; mã thiếu lookup vẫn xuất hiện trong phân tích.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
