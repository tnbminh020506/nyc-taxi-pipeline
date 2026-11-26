<!--
Đầu vào
Đường dẫn tệp, lệnh chạy, log và kết quả kiểm tra thực tế của từng nhánh.
Đầu ra
Báo cáo bàn giao theo thành viên và trạng thái tích hợp của cả nhóm.
-->
# Bước 4.2 · Báo cáo MVP

| Phần | Phụ trách | Trạng thái |
| --- | --- | --- |
| Bước 1.1–1.4 · Ingestion và replay | Chính | Đã có đặc tả |
| Bước 2.1–2.3 · Spark Batch và cảnh báo trong stream | Mạnh | Đã đặc tả cảnh báo và chốt cửa sổ |
| Bước 3.1–3.3 · PostgreSQL và SQL đối soát | Bảo | Đã đặc tả vòng cập nhật baseline |
| Bước 4.1–4.2 · Chốt dữ liệu và ghép demo | Nhóm trưởng | Đã đặc tả nghiệm thu cảnh báo và cập nhật baseline |

Mỗi thành viên ghi phần đã làm kèm đường dẫn tệp, luồng và hợp đồng bàn giao, lệnh chạy và kết quả thực tế, phần còn vướng và việc cần phối hợp.

Nhóm trưởng ghi dataset, bộ lọc API, số trang và số dòng, Zone Lookup, kỳ lịch sử, nguồn stream và `replay_id`. Ghi kết quả Spark Batch công bố vào `historical_baseline`, mức Spark Streaming truy vấn khi mở cửa sổ, mốc chốt cửa sổ và số đếm cuối, baseline và số cửa sổ tham chiếu trước/sau giao dịch, cùng mức Spark đọc ở cửa sổ tương ứng tiếp theo. Với ca vượt ngưỡng, ghi `alert_id`, khu vực, cửa sổ, số đếm, mức lịch sử, mốc phát sự kiện, mốc Kafka xác nhận, mốc consumer điều phối nhận cảnh báo, mốc bắt đầu và hoàn tất ghi số đếm vào SQL. Ghi nhịp trigger đã dùng và kết quả ca giữ upsert để kiểm chứng thứ tự. Dùng truy vấn SQL để đối soát số đếm. Ghi riêng kết quả trên bộ mẫu và kết quả lấy dữ liệu từ API.
