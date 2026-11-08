# Bước 2.3 | Mạnh | Điểm chạy Spark Structured Streaming.
#
# Đầu vào
# - Cấu hình Spark và Kafka, replay_id, checkpoint, nhịp trigger và đích PostgreSQL.
#
# Đầu ra
# - Các lần cập nhật tổng số chuyến theo vùng/cửa sổ trong kho và log chất lượng stream.
#
# Yêu cầu
# - Gọi pipelines/streaming_counts.py. Mạnh nối đầu ra mỗi lần cập nhật với cơ chế ghi do Bảo bàn giao ở bước 3.2.
# - database/load/replay_counts.sql mô tả cột, khóa và quy tắc cập nhật tổng lũy kế.
