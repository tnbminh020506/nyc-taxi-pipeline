# 1.2 · HDFS, Parquet và manifest

**Phụ trách:** Chính. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Chuẩn hóa nguồn để bàn giao cho batch, replay và kho; sở hữu việc công bố ready.

## Hợp đồng dữ liệu và bàn giao

Manifest: dataset_role, source_id, range_start/end có múi giờ, raw_path, parquet_path, row_count, coverage, status, source_version. history/replay khóa theo vai trò–nguồn–khoảng; zone_lookup và zone_boundaries khóa theo nguồn/phiên bản, không bắt buộc khoảng và Parquet. Parquet giữ source_record_id, pickup_at/dropoff_at, pickup_zone_id/dropoff_zone_id và nguồn; tọa độ nếu có.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Lưu raw trước, tạo Parquet dẫn xuất, kiểm tra schema/số dòng rồi công bố manifest sau cùng. Dùng vùng tạm và bước commit công bố để người đọc không thấy dữ liệu dở dang. Không đánh đồng tải một phần với đủ độ phủ. Batch/replay chỉ nhận ready đúng vai trò; loader lookup đọc raw_path qua HDFS client.

## Khu vực code

- [ingestion/hdfs_store.py](../../../src/nyc_taxi_pipeline/ingestion/hdfs_store.py)
- [ingestion/manifest.py](../../../src/nyc_taxi_pipeline/ingestion/manifest.py)

## Tiêu chí hoàn thành

Các URI đọc được từ tiến trình nhận; tải thiếu không ready; retry cập nhật cùng khóa; lookup và ranh giới có nguồn rõ; bàn giao schema Parquet cho Mạnh và Bảo.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
