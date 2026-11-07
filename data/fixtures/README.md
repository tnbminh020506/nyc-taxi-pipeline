<!--
Đầu vào
Một vùng A có tên trong lookup, hai cửa sổ lịch sử và một phiên replay.
Đầu ra
Đặc tả hàng mẫu và số liệu kỳ vọng để ba nhánh phát triển độc lập.
-->
# Fixture nghiệm thu

Chọn một mã vùng hợp lệ cho A, ba ngày thứ Hai cách nhau ít nhất một tuần và tránh ngày đổi giờ mùa hè. Hai ngày đầu thuộc kỳ `history`. Ngày cuối thuộc kỳ `replay`. Dùng giờ New York, ghi offset thực tế vào các mốc trao đổi. Cả nhóm dùng chung mã vùng, ngày, định danh nguồn và `replay_id`.

| Đầu vào cần dựng | Nội dung |
| --- | --- |
| Zone Lookup | Một dòng cho A, có mã và tên vùng |
| Parquet lịch sử | Cửa sổ 08:00–08:15 có 1 chuyến vào thứ Hai đầu tiên và 3 chuyến vào thứ Hai thứ hai. Cả hai cửa sổ đều đã xác nhận độ phủ. |
| Sự kiện replay | A có các lượt pickup lúc 08:01, 08:05 và 08:14. Gửi lại một sự kiện với cùng `event_id` và `replay_id`. Phát thêm một sự kiện đúng 08:15 với ID riêng. |

| Điểm bàn giao | Kết quả kỳ vọng |
| --- | --- |
| Batch | A, thứ ISO 1, `slot_15m = 32`, `baseline_trip_count = 2`, `reference_windows = 2` |
| Stream | A có 3 chuyến trong 08:00–08:15 và 1 chuyến trong 08:15–08:30 |
| PostgreSQL | Cửa sổ đầu trả tên và mã A, quan sát 3, mức lịch sử 2, 2 cửa sổ tham chiếu và `change_pct = 50` |
| Chạy lại | Gửi trùng một sự kiện hoặc nạp lại cùng kết quả giữ nguyên số đếm |

Trong các ca riêng, đổi một cửa sổ lịch sử thành cửa sổ có độ phủ nhưng có 0 chuyến để kiểm tra mẫu số. Thử thêm vùng thiếu tên, vùng thiếu baseline và vùng có baseline bằng 0 để kiểm tra đầu ra truy vấn.
