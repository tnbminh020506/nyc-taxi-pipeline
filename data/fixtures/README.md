<!--
Đầu vào
Một vùng A có tên trong lookup, hai cửa sổ lịch sử và một phiên replay.
Đầu ra
Đặc tả hàng mẫu và số liệu kỳ vọng để ba nhánh phát triển độc lập.
-->
# Bước 4.1 · Bộ mẫu nghiệm thu

Chọn một mã vùng hợp lệ cho A và bốn ngày thứ Hai, mỗi ngày cách nhau ít nhất một tuần. Tránh ngày đổi giờ mùa hè. Hai ngày đầu thuộc kỳ `history`. Hai ngày sau thuộc kỳ `replay`. Dùng giờ New York, ghi offset thực tế vào các mốc trao đổi. Cả nhóm dùng chung mã vùng, ngày, định danh nguồn và `replay_id`.

| Đầu vào cần dựng | Nội dung |
| --- | --- |
| Phản hồi API mẫu | Dựng các trang JSON cục bộ theo trường `tpep_pickup_datetime` và `pulocationid` cho các bản ghi bên dưới. Dùng để kiểm tra nhập và tạo Parquet trước lượt chạy với API thật. |
| Zone Lookup | Một dòng cho A, có mã và tên vùng |
| Parquet lịch sử | Cửa sổ 08:00–08:15 có 1 chuyến vào thứ Hai đầu tiên và 3 chuyến vào thứ Hai thứ hai. Cả hai cửa sổ đều đã xác nhận độ phủ. |
| Parquet phát lại | A có bốn dòng chuyến đi riêng vào thứ Hai thứ ba, lúc 08:01, 08:05, 08:14 và 08:15. Thêm một chuyến của A lúc 08:01 vào thứ Hai thứ tư để kiểm tra Spark đọc lại baseline trong cùng phiên. Xác nhận độ phủ của khung 08:00–08:15 thứ Hai thứ ba. |
| Sự kiện replay hoặc mô phỏng | Phát lần lượt bốn sự kiện của thứ Hai thứ ba qua `taxi-events` để quan sát số đếm tăng. Sau khi xác nhận PostgreSQL đã chốt cửa sổ đầu, phát sự kiện của thứ Hai thứ tư với cùng `replay_id`. Trong ca gửi trùng, gửi lại sự kiện 08:05 với cùng `event_id` và `replay_id`. Dùng phiên khác khi chạy lại cùng kịch bản bằng nguồn mô phỏng. |

| Điểm bàn giao | Kết quả kỳ vọng |
| --- | --- |
| Batch | Sau khi job Spark Batch hoàn tất, truy vấn `historical_baseline` thấy A, thứ ISO 1, `slot_15m = 32`, `baseline_trip_count = 2`, `reference_windows = 2`. |
| Stream | Spark đọc mức 2 từ PostgreSQL khi mở cửa sổ 08:00–08:15 của thứ Hai thứ ba. A có 3 chuyến trong 08:00–08:15 và 1 chuyến trong 08:15–08:30. Hai chuyến đầu chưa vượt ngưỡng. Chuyến 08:14 phát một cảnh báo có `trip_count = 3` và `baseline_trip_count = 2` khi cửa sổ còn mở. |
| Thứ tự đầu ra | Trong ca kiểm tra, tạm giữ bước upsert sau khi Kafka xác nhận. Consumer điều phối đọc được cảnh báo khi PostgreSQL vẫn đang có tổng 2. Cho bước upsert chạy tiếp và xác nhận tổng lên 3. |
| Trước khi chốt | PostgreSQL đã lưu tổng 3 trong `replay_pickup_counts`. Baseline vẫn là 2 với 2 cửa sổ tham chiếu. |
| Sau khi chốt | Đặt độ trễ cho phép bằng 0 trong ca mẫu và chờ Spark xử lý tiến độ đến mốc 08:15. Kiểm tra `baseline_history_windows` có đúng một mẫu tổng 3 của cửa sổ đầu. `historical_baseline` của A, thứ ISO 1, `slot_15m = 32` có mức 7/3 với 3 cửa sổ tham chiếu. |
| Cửa sổ tương ứng sau đó | Sự kiện 08:01 thứ Hai thứ tư mở cửa sổ mới trong cùng phiên. Spark đọc mức 7/3 với 3 cửa sổ tham chiếu. |
| Đối soát | Sau khi cập nhật baseline, truy vấn cửa sổ đầu vẫn trả tên và mã A, quan sát 3, mức đã dùng 2, 2 cửa sổ tham chiếu và `change_pct = 50`. |
| Chạy lại | Gửi trùng sự kiện giữ nguyên số đếm. Nếu ghi SQL lỗi sau khi Kafka đã nhận cảnh báo, thử lại với cùng `alert_id` để consumer điều phối chỉ xử lý một lần và SQL lưu đúng tổng lũy kế. Gọi lại thao tác chốt hoặc chạy lại cùng cửa sổ với `replay_id` khác để kiểm tra mẫu lịch sử vẫn chỉ được cộng một lần. |

Dùng dữ liệu mẫu khởi tạo lại cho các ca chạy độc lập. Chạy thêm một phiên trong đó hai hoặc ba chuyến của khung 08:00–08:15 đi vào cùng micro-batch. Kết quả vẫn có đúng một cảnh báo cho vùng A và khung đó.

Trong các ca riêng, đổi một cửa sổ lịch sử thành cửa sổ có độ phủ nhưng có 0 chuyến để kiểm tra mẫu số. Thử vùng thiếu tên để kiểm tra SQL, vùng thiếu mức lịch sử để kiểm tra ghi số đếm kèm log, và vùng có mức lịch sử bằng 0 để kiểm tra chuyến đầu tiên kích hoạt cảnh báo. Thử chốt cửa sổ stream có độ phủ và 0 chuyến, rồi thử một cửa sổ stream thiếu độ phủ để kiểm tra chỉ ca đầu được cộng vào mẫu số. Phát thêm một chuyến cùng vùng và cửa sổ sau khi vượt ngưỡng để kiểm tra tổng vẫn tăng và điều phối chỉ xử lý một cảnh báo. Mô phỏng lỗi phát cảnh báo để kiểm tra Spark chưa ghi tổng vượt ngưỡng vào SQL và có thể thử lại.
