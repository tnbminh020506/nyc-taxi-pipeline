# Bước 1.3–1.4 | Chính | Tạo sự kiện đón khách và phát qua Kafka.
#
# Đầu vào
# - Các mục ready có dataset_role = replay trong manifest và Parquet tại parquet_path.
# - replay_id, Kafka bootstrap servers, topic taxi-events và tốc độ replay.
#
# Đầu ra
# - JSON trên taxi-events gồm replay_id, event_id, pickup_at, pickup_zone_id và source_file.
# - source_file nhận diện file Parquet replay để có thể tra ngược về phản hồi API qua manifest.
# - Log số dòng đã đọc, số sự kiện đã gửi và lỗi theo file hoặc dòng.
#
# Yêu cầu
# - Lọc pickup theo [range_start, range_end) của từng dòng manifest, rồi phát theo thứ tự pickup_at.
# - Giữ event_id ổn định cho cùng dòng của bộ mẫu cố định, kể cả khi gửi lại.
# - Phân biệt hai dòng nguồn hợp lệ có cùng giá trị. Dùng định danh file và vị trí dòng ổn định.
# - pickup_at theo ISO 8601 có offset hoặc UTC. Ghi rõ quy ước chuyển thời gian nguồn.
# - Payload chỉ chứa thông tin có tại thời điểm đón khách.
# - Python điều tiết tốc độ phát. Kiểm tra Spark đọc được JSON trên topic đã thống nhất.
