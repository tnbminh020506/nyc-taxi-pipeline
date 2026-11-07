# Phụ trách — Chính | MVP-01 | Phát lại sự kiện đón khách qua Kafka.
#
# Đầu vào
# - Các dòng ready có dataset_role = replay trong manifest, Parquet tương ứng và ánh xạ cột TLC.
# - replay_id, Kafka bootstrap servers, topic taxi-events và tốc độ replay.
#
# Đầu ra
# - JSON trên taxi-events gồm replay_id, event_id, pickup_at, pickup_zone_id và source_file.
# - Log số dòng đã đọc, số sự kiện đã gửi và lỗi theo file hoặc dòng.
#
# Yêu cầu
# - Lọc pickup theo [range_start, range_end) của từng dòng manifest, rồi phát theo thứ tự pickup_at.
# - Giữ event_id ổn định cho cùng dòng của bộ mẫu cố định, kể cả khi gửi lại.
# - pickup_at theo ISO 8601 có offset hoặc UTC. Ghi rõ quy ước chuyển thời gian nguồn.
# - Payload chỉ chứa thông tin có tại thời điểm đón khách.
