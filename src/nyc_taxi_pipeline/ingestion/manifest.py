# Bước 1.2 | Chính | Công bố dữ liệu API, Parquet và lookup đã sẵn sàng.
#
# Đầu vào
# - Kết quả tải API và lưu dữ liệu từ ingest.py, kèm phạm vi đã chốt trong configs/demo.example.yaml.
# - Kỳ history và replay tách biệt, theo khoảng [range_start, range_end).
#
# Đầu ra
# - Manifest có dataset_role, source_id, raw_path, parquet_path, status, range_start và range_end.
# - source_id là mã dataset API hoặc định danh nguồn lookup. raw_path trỏ đến thư mục JSON gốc hoặc CSV lookup.
# - parquet_path trỏ đến Parquet của history/replay. Bảo dùng raw_path của zone_lookup tại bước 3.2.
#
# Yêu cầu
# - Với history/replay, một mục cho mỗi bộ dataset_role, source_id, range_start, range_end.
# - Với zone_lookup, một mục theo source_id. Để trống khoảng thời gian và parquet_path.
# - Chỉ công bố ready cho history/replay khi đã tải đủ phạm vi và ghi xong Parquet. Chạy lại cập nhật cùng khóa.
# - Với zone_lookup, chỉ công bố ready khi CSV đã được lưu hoàn chỉnh.
# - Mốc range_start và range_end có múi giờ. Batch và replay chỉ đọc dòng ready đúng vai trò.
