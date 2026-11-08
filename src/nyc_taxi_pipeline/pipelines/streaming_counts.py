# Bước 2.3 | Mạnh | Đếm chuyến bằng Spark Structured Streaming.
#
# Đầu vào
# - JSON từ Kafka taxi-events, replay_id cần xử lý, checkpoint riêng của phiên và nhịp trigger.
# - Quy tắc kiểm tra và phân khung trong processing/pickup.py.
#
# Đầu ra
# - Một dòng cho mỗi vùng và cửa sổ của phiên, gồm replay_id, pickup_zone_id, window_start,
#   window_end và trip_count lũy kế. Chuyển tổng hiện tại sang database/load/replay_counts.sql.
# - Log số sự kiện hợp lệ, lỗi và lượt gửi trùng phát hiện được.
#
# Yêu cầu
# - Kiểm tra schema và loại bản ghi lỗi. Khử trùng theo replay_id và event_id trong phiên demo.
# - Gom theo pickup_at. Cấu hình nhịp trigger độc lập với cửa sổ nghiệp vụ 15 phút.
# - Dùng lại checkpoint khi tiếp tục cùng phiên. Dùng replay_id mới khi chạy lại demo từ đầu.
