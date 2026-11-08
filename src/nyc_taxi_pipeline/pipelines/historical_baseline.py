# Bước 2.2 | Mạnh | Tính mức lịch sử bằng Spark Batch.
#
# Đầu vào
# - Các mục ready có dataset_role = history trong manifest và file Parquet dẫn xuất tại parquet_path.
# - Danh sách cửa sổ có độ phủ đã xác nhận trong kỳ lịch sử và các vùng thuộc phạm vi so sánh.
# - Quy tắc kiểm tra và phân khung trong processing/pickup.py.
#
# Đầu ra
# - Một dòng cho mỗi bộ pickup_zone_id, iso_weekday, slot_15m trong kỳ tham chiếu đang dùng.
# - Các cột bàn giao gồm pickup_zone_id, iso_weekday, slot_15m, baseline_trip_count,
#   reference_windows, history_start và history_end.
# - Log số dòng đọc, hợp lệ và lỗi. Tập kết quả chuyển cho database/load/baseline.sql.
#
# Yêu cầu
# - Lọc đúng [range_start, range_end) của từng dòng manifest trước khi đếm.
# - baseline_trip_count là số chuyến trung bình trên các cửa sổ đã xác nhận độ phủ.
# - Mẫu số reference_windows gồm cả cửa sổ có độ phủ nhưng có 0 chuyến.
# - Chỉ tính từ cửa sổ đã xác nhận. Ghi rõ phạm vi khi mới xác nhận được một phần kỳ lịch sử.
