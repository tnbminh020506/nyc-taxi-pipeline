# Phụ trách — Chính | Điểm chạy phát lại sự kiện.
#
# Đầu vào
# - Cấu hình demo, manifest, replay_id, địa chỉ Kafka, topic và tốc độ phát.
#
# Đầu ra
# - Luồng JSON pickup trên taxi-events và log số sự kiện đã gửi hoặc lỗi.
#
# Yêu cầu
# - Gọi ingestion/replay.py. Chạy sau khi Spark stream của phiên demo đã sẵn sàng.
