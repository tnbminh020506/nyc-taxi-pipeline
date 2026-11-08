# Bước 4.1 | Cả nhóm | Kịch bản nghiệm thu hợp đồng dữ liệu.
#
# Đầu vào
# - Fixture mô tả tại data/fixtures/README.md và các điểm chạy của ba phần MVP.
#
# Đầu ra
# - Kết quả từng bước, số liệu đối chiếu trong PostgreSQL và log lỗi/trùng.
#
# Các ca cần kiểm tra
# - Nhập các trang API mẫu, lưu JSON gốc và tạo Parquet đọc được cho history/replay.
# - Nhập lại giữ đúng một mục ready trên mỗi vai trò, nguồn và khoảng thời gian.
# - Phản hồi thiếu trang hoặc kỳ bị giới hạn giữa chừng không được công bố ready.
# - Bảo nạp được Zone Lookup từ mục zone_lookup đã sẵn sàng trong manifest.
# - Batch cho baseline 2 và reference_windows = 2. Một ca riêng tính cả cửa sổ có 0 chuyến.
# - Stream cho 3 chuyến ở 08:00–08:15 và 1 chuyến ở 08:15–08:30 dù có sự kiện gửi lại.
# - Bản ghi lỗi bị loại, log có lý do và số đếm hợp lệ giữ nguyên.
# - Truy vấn cửa sổ đầu trả +50%. Vùng thiếu baseline hoặc baseline bằng 0 vẫn xuất hiện,
#   với tỷ lệ NULL và lý do phù hợp. Kiểm tra cả vùng thiếu tên trong lookup.
# - Nạp lại lookup, batch và cùng đầu ra stream giữ nguyên kết quả.
