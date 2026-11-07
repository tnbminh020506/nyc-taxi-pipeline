# Phụ trách — Mạnh | Điểm chạy Spark Structured Streaming.
#
# Đầu vào
# - Cấu hình Spark và Kafka, replay_id, checkpoint, nhịp trigger và đích PostgreSQL.
#
# Đầu ra
# - Các lần cập nhật tổng số chuyến theo vùng/cửa sổ trong kho và log chất lượng stream.
#
# Yêu cầu
# - Gọi pipelines/streaming_counts.py và dùng hợp đồng ghi của database/load/replay_counts.sql.
