# Phụ trách — Chính | MVP-01 | Công bố tập dữ liệu đã sẵn sàng.
#
# Đầu vào
# - Kết quả lưu theo file từ ingest.py và phạm vi đã chốt trong configs/demo.example.yaml.
# - Kỳ history và replay tách biệt, theo khoảng [range_start, range_end).
#
# Đầu ra
# - Manifest có dataset_role, source_file, raw_path, status, range_start và range_end.
# - Log lỗi theo file/phạm vi. Đường dẫn Zone Lookup được bàn giao riêng cho bên nạp kho.
#
# Yêu cầu
# - Một dòng cho mỗi bộ dataset_role, source_file, range_start, range_end.
# - Chỉ công bố status = ready sau khi raw_path đã có file hoàn chỉnh. Chạy lại cập nhật cùng khóa.
# - Mốc range_start và range_end có múi giờ. Batch và replay chỉ đọc dòng ready đúng vai trò.
