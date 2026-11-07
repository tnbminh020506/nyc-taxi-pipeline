# Phụ trách — Mạnh | MVP-02 | Quy tắc kiểm tra và phân khung dùng chung.
#
# Đầu vào
# - Thời điểm đón khách và mã vùng từ Parquet hoặc JSON Kafka, kèm quy ước thời gian nguồn.
# - Độ dài cửa sổ 15 phút và múi giờ nghiệp vụ America/New_York.
#
# Đầu ra
# - Dòng hợp lệ có pickup_at với múi giờ, pickup_zone_id kiểu số nguyên, window_start, window_end,
#   iso_weekday từ 1 (thứ Hai) đến 7 (Chủ nhật) và slot_15m từ 0 đến 95 theo giờ địa phương.
# - Lý do loại nếu thiếu thời điểm, thời điểm không đọc được hoặc mã vùng không hợp lệ.
#
# Yêu cầu
# - Dùng cùng một quy tắc cho batch và stream. Kiểm tra mã vùng là số nguyên dương.
# - Cửa sổ dạng [bắt đầu, kết thúc). Mốc 08:15 vào khung 08:15–08:30.
# - Kiểm tra mã hợp lệ độc lập với việc Zone Lookup có tên vùng. Fixture đầu tiên tránh ngày đổi giờ.
