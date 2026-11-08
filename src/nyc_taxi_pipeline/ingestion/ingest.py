# Bước 1.1–1.2 | Chính | Gọi NYC Open Data API, lưu phản hồi và tạo Parquet.
#
# Đầu vào
# - Endpoint SODA, mã dataset, app token từ biến môi trường khi dùng SODA v3 và kích thước trang.
# - Hai kỳ history/replay theo tpep_pickup_datetime và các trường API cần lấy, gồm pulocationid.
# - CSV Taxi Zone Lookup và các đường dẫn lưu phản hồi gốc, Parquet và manifest.
#
# Đầu ra
# - Các trang JSON gốc, CSV lookup nguyên bản và Parquet dẫn xuất riêng cho history/replay.
# - Metadata bàn giao cho manifest.py gồm mã dataset, bộ lọc, đường dẫn, trạng thái và lỗi nếu có.
# - Log số trang, số dòng và tình trạng hoàn tất theo kỳ.
#
# Yêu cầu
# - Lọc theo [range_start, range_end) và phân trang theo thứ tự ổn định. Ghi rõ khi dừng do lỗi hoặc giới hạn.
# - Xác nhận ánh xạ từ trường API sang Parquet. Chỉ báo ready khi đã lấy đủ phạm vi và ghi Parquet xong.
# - Bàn giao cho Mạnh schema và parquet_path. Bàn giao cho Bảo raw_path và tên cột của lookup.
