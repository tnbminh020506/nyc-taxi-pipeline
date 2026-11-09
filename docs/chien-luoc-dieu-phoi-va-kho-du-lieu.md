# Chiến lược Dữ liệu & Nghiệp vụ Điều phối Taxi (NYC Taxi Pipeline)

> **Người soạn:** Uông Gia Bảo (Phụ trách Data Warehouse & Analytics)  
> **Mục đích:** Thống nhất định hướng nghiệp vụ, khả năng đáp ứng của kho dữ liệu PostgreSQL và cơ chế phối hợp giữa các nhánh Ingestion (Chính), Processing (Mạnh) và Warehouse (Bảo).

---

## 1. Tầm nhìn Nghiệp vụ: Bài toán Trung tâm Điều phối Taxi

Dưới góc nhìn của **Trung tâm Điều phối Taxi (Fleet Dispatch Center)**, mục tiêu cốt lõi của pipeline là **Cân bằng Cung - Cầu theo thời gian thực trong từng khung 15 phút**:
1. **Phát hiện Điểm nóng bùng nổ (Demand Surge / Hotspots):** Khu vực nào có số chuyến đón khách thực tế vượt xa mức trung bình lịch sử (`change_pct > +30%`), cảnh báo nguy cơ thiếu xe cục bộ.
2. **Định vị Khu vực dư nguồn cung (Surplus Areas):** Khu vực nào có nhu cầu thực tế thấp hơn hẳn thường lệ (`change_pct < -30%`), gợi ý điều chuyển tài xế từ vùng này sang chi viện cho các điểm nóng lân cận (cùng Quận/Borough).
3. **Giám sát Phân bố Không gian & Cửa ngõ:** Đo lường tỷ trọng nhu cầu giữa các Quận (Manhattan, Queens, Brooklyn...) và theo dõi riêng các khu vực trọng điểm như Sân bay (JFK, LaGuardia).

---

## 2. Khả năng đáp ứng của Kho dữ liệu (Dựa trên 3 Bảng đã thiết kế)

Để các nhánh tránh làm thừa hoặc kỳ vọng sai về dữ liệu, kho PostgreSQL chốt phạm vi phân tích như sau:

| Thông tin nghiệp vụ | Trạng thái | Giải thích kỹ thuật |
| :--- | :---: | :--- |
| **Cảnh báo Điểm nóng (Surge Zones)** |  **Hỗ trợ 100%** | So sánh `trip_count` (Stream) với `baseline_trip_count` (Batch) theo khung 15 phút. |
| **Gợi ý Khu vực điều chuyển xe** |  **Hỗ trợ 100%** | Lọc vùng có độ lệch âm lớn và gom nhóm theo cột `borough` trong `taxi_zones`. |
| **Tỷ trọng & Mật độ theo Quận/Borough** |  **Hỗ trợ 100%** | Gom nhóm (`GROUP BY borough`) trên kết quả stream thời gian thực. |
| **Theo dõi Cửa ngõ / Sân bay (JFK, LGA)** |  **Hỗ trợ 100%** | Lọc theo `zone_name` hoặc `pickup_zone_id` đặc thù. |
| **Xu hướng 15 phút liên tiếp (Time-series)** |  **Hỗ trợ 100%** | Đồ thị biến động qua các `window_start` liên tiếp trong cùng phiên `replay_id`. |
| **Số xe trống / Tỷ lệ Cung - Cầu** | ❌ **Không hỗ trợ** | Nguồn TLC chỉ có cuốc xe thành công, không có GPS xe trống thời gian thực. |
| **Thời gian chờ xe (ETA) / Tỷ lệ hủy chuyến** | ❌ **Không hỗ trợ** | Nguồn mở không chứa sự kiện đặt xe và hủy cuốc của khách. |
| **Tổng chuyến cả năm / cả tháng** | ❌ **Không hỗ trợ** | Kho chỉ lưu số trung bình mẫu lịch sử và các khung giờ demo của phiên stream. |

---

## 3. Kiến trúc Kho dữ liệu PostgreSQL (`database/schema.sql`)

Kho dữ liệu đã được triển khai sẵn sàng gồm **3 bảng chuẩn hóa** (đã tối ưu hóa Index và Check Constraint):

1. **`taxi_zones`**:
   - `pickup_zone_id` (PK, INTEGER)
   - `zone_name` (TEXT)
   - `borough` (TEXT)
2. **`historical_baseline`**:
   - Khóa chính: `(pickup_zone_id, iso_weekday, slot_15m)`
   - Cột số liệu: `baseline_trip_count` (NUMERIC), `reference_windows` (INTEGER), `history_start` (TIMESTAMPTZ), `history_end` (TIMESTAMPTZ)
3. **`replay_pickup_counts`**:
   - Khóa chính: `(replay_id, pickup_zone_id, window_start)`
   - Cột số liệu: `window_end` (TIMESTAMPTZ), `trip_count` (BIGINT)

> **Lưu ý ràng buộc:** Không dùng Foreign Key chặn cứng từ bảng số liệu sang `taxi_zones` để đảm bảo khi stream gặp mã vùng mới phát sinh thì pipeline vẫn ghi nhận số liệu bình thường, không bị dừng luồng stream.

---
