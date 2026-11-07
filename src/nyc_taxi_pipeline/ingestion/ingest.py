# Phụ trách — Chính | MVP-01 | Nhập dữ liệu TLC vào raw storage.
#
# Đầu vào
# - Danh sách URL hoặc đường dẫn cục bộ của Yellow Taxi Parquet và Taxi Zone Lookup.
# - Đường dẫn lưu thô mà Spark và bên nạp kho đều đọc được.
# - Vai trò và khoảng chọn của từng file chuyến đi theo cấu hình demo.
#
# Đầu ra
# - Bản sao nguyên bản của Parquet và Zone Lookup tại raw storage.
# - Kết quả lưu theo file cho manifest.py, gồm nguồn, đường dẫn đích, trạng thái và lỗi nếu có.
# - Log số file lưu thành công và số file lỗi.
#
# Yêu cầu
# - Chỉ báo lưu thành công khi file đích hoàn chỉnh. Chạy lại cùng đầu vào giữ đường dẫn ổn định.
# - Bàn giao đường dẫn Spark thực sự đọc được và tên cột thực tế của Zone Lookup.
