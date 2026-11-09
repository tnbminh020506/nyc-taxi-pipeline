# Đặc tả Danh mục Truy vấn & Báo cáo Điều hành Taxi (NYC Taxi Pipeline)

> **Người soạn:** Uông Gia Bảo (Data Warehouse & Analytics)  
> **Gửi tới:** Nhóm trưởng, Chính (Data Ingestion), Mạnh (Data Processing)  
> **Mục tiêu:** Thống nhất toàn bộ danh mục thông tin truy vấn phục vụ demo, báo cáo và giải bài toán điều phối taxi cho trung tâm tổng.

---

## 1. TỔNG QUAN PHÂN LOẠI TRUY VẤN

Để vừa **đạt điểm tuyệt đối bài kiểm thử tự động của đề tài**, vừa **đáp ứng trọn vẹn yêu cầu nghiệp vụ của Nhóm trưởng** (số chuyến, mật độ, điều phối xe...), hệ thống báo cáo được chia làm 3 nhóm truy vấn:

1. **Nhóm A: Truy vấn Nghiệm thu Bắt buộc (Core MVP)** — Chấm điểm tự động theo hợp đồng đồ án.
2. **Nhóm B: Truy vấn Nghiệp vụ Điều hành & Điều phối Taxi** — Trả lời bài toán điều xe, mật độ theo vùng, khu vực còn điều phối được.
3. **Nhóm C: Truy vấn Quy mô Thời gian (Tháng / Tuần / Cao điểm)** — Đánh giá sản lượng và xu hướng lịch sử.

---

## 2. CHI TIẾT CÁC THÔNG TIN TRUY VẤN SẼ THỰC HIỆN

### NHÓM A: TRUY VẤN NGHIỆM THU BẮT BUỘC (CORE MVP)

#### Truy vấn 1: So sánh Nhu cầu Đón khách theo Cửa sổ 15 phút (`compare_pickup_demand.sql`)
*Phục vụ câu hỏi đề tài: "Khu vực nào ghi nhận nhiều chuyến đón khách hơn mức lịch sử trong cùng khung 15 phút?"*

- **Tham số đầu vào:** `replay_id` (phiên phát lại) và `window_start` (cửa sổ 15 phút cần xem).
- **Các thông tin hiển thị (Đúng chuẩn 7 cột nghiệm thu):**
  1. `pickup_zone_id`: Mã định danh khu vực.
  2. `zone_name`: Tên khu vực (nếu chưa có trong danh mục thì hiển thị mã ID).
  3. `observed_trip_count`: Số chuyến đón khách thực tế đếm từ luồng Stream của Mạnh.
  4. `baseline_trip_count`: Mức trung bình lịch sử tương ứng (cùng Thứ, cùng Khung 15 phút).
  5. `reference_windows`: Số lượng cửa sổ lịch sử dùng làm mẫu số (độ tin cậy của baseline).
  6. `change_pct`: Tỷ lệ tăng/giảm so với lịch sử: `100 * (observed - baseline) / baseline`.
  7. `change_unavailable_reason`: Lý do nếu không tính được `%` (`missing_baseline` hoặc `zero_baseline`).
- **Thứ tự sắp xếp:** Tỷ lệ tăng giảm dần (`change_pct DESC NULLS LAST`).

---

### NHÓM B: TRUY VẤN ĐIỀU HÀNH & ĐIỀU PHỐI TAXI (THEO YÊU CẦU NHÓM TRƯỞNG)

#### Truy vấn 2: Ma trận Điều phối Taxi — Xác định "Khu vực còn điều phối được xe" (`taxi_dispatch_matrix.sql`)
*Phục vụ trung tâm tổng: Phát lệnh điều xe từ vùng vắng khách sang chi viện cho vùng thiếu xe.*

- **Thông tin hiển thị:**
  - `borough`: Tên Quận (Manhattan, Queens, Brooklyn, Bronx, Staten Island).
  - `zone_name`: Tên khu vực cụ thể.
  - `observed_trip_count` vs `baseline_trip_count`: So sánh thực tế và lịch sử.
  - `net_trip_diff`: Số lượng chuyến chênh lệch tuyệt đối (`observed - baseline`).
  - `dispatch_action`: Trạng thái phân loại điều phối:
    - **`SURGE / NEED_TAXIS` (Thiếu xe trầm trọng):** Nhu cầu tăng vọt (`change_pct >= +30%` và vượt trên 10 chuyến).
    - **`SURPLUS / CAN_REALLOCATE` (Khu vực còn điều phối được xe):** Nhu cầu thấp hơn hẳn lịch sử (`change_pct <= -30%`), tài xế đang vắng khách, **đây chính là nguồn xe để điều chuyển đi nơi khác**.
    - **`BALANCED` (Cung cầu ổn định):** Dao động bình thường quanh mức lịch sử.
  - `suggested_transfer_to`: Gợi ý điều phối: Tự động ghép cặp chuyển xe từ vùng thừa sang vùng thiếu trong cùng một Quận.

#### Truy vấn 3: Mật độ Taxi theo Khu vực và theo Quận (`zone_and_borough_density.sql`)
*Phục vụ đánh giá mức độ tập trung khách hàng trên toàn thành phố.*

- **Thông tin hiển thị:**
  - **Mật độ theo Khu vực (Zone Density %):**
    $$\text{Tỷ trọng nhu cầu vùng} = \frac{\text{Số chuyến của vùng}}{\text{Tổng số chuyến toàn New York trong khung 15p}} \times 100\%$$
    *(Xác định top 10 khu vực đón khách nhộn nhịp nhất thành phố hiện tại).*
  - **Mật độ gộp theo Quận (Borough Share %):**
    - Tổng số chuyến thực tế của cả Quận trong khung 15 phút.
    - Tỷ lệ % đóng góp của từng Quận vào tổng lưu lượng taxi toàn thành phố (ví dụ: Manhattan chiếm 68%, Queens 18%, Brooklyn 10%...).

#### Truy vấn 4: Giám sát Trọng điểm Cửa ngõ Giao thông & Sân bay (`airport_and_transit_hubs.sql`)
*Theo dõi riêng các đầu mối giao thông quy mô lớn nhất NYC.*

- **Thông tin hiển thị:**
  - Tình hình đón khách tại: **JFK Airport**, **LaGuardia Airport (LGA)**, **Newark Airport (EWR)**, **Penn Station**, **Grand Central**.
  - So sánh lưu lượng thực tế và lịch sử để tổng đài chủ động gom xe đón các đợt chuyến bay hạ cánh.

---

### NHÓM C: TRUY VẤN QUY MÔ THỜI GIAN (THÁNG, NĂM, TUẦN, CAO ĐIỂM)

> [!NOTE]
> **Giải thích kỹ thuật gửi Nhóm trưởng:**  
> Dữ liệu trong kho PostgreSQL là **dữ liệu tổng hợp (Aggregated/Baseline Data)** của các kỳ khảo sát được chọn lọc từ API (chứ không lưu toàn bộ hàng triệu cuốc thô của cả năm để tối ưu dung lượng).  
> Do đó, thông tin về quy mô thời gian được phản ánh chuẩn xác qua các truy vấn sau:

#### Truy vấn 5: Tổng sản lượng chuyến đi trong Kỳ khảo sát (`trip_volume_summary.sql`)
*Đánh giá quy mô số chuyến taxi trong kỳ khảo sát (Tháng/Năm/Kỳ Replay).*

- **Thông tin hiển thị:**
  - `survey_period`: Kỳ khảo sát tháng/năm đang được chọn (ví dụ: Tháng 01/2025).
  - `total_replay_trips`: Tổng số lượt đón khách đã được stream xử lý qua tất cả các cửa sổ của phiên.
  - `active_zones_count`: Số lượng khu vực có phát sinh đón khách.
  - `peak_15m_window`: Cửa sổ 15 phút ghi nhận sản lượng đón khách cao nhất toàn thành phố.

#### Truy vấn 6: Bản đồ Nhiệt Khung giờ Cao điểm trong Tuần (`weekly_peak_hours.sql`)
*Khai thác từ bảng `historical_baseline` để phân tích quy luật theo lịch.*

- **Thông tin hiển thị:**
  - `iso_weekday`: Thứ trong tuần (Thứ Hai đến Chủ Nhật).
  - `peak_slot_15m`: Khung giờ cao điểm nhất trong ngày của từng thứ (ví dụ: Thứ Sáu cao điểm lúc 18:30–18:45).
  - `average_citywide_trips`: Lượng khách trung bình lịch sử toàn thành phố tại khung giờ đó.
  - Phục vụ: Giúp trung tâm lập kế hoạch phân ca trực cho tài xế theo từng ngày trong tuần.

#### Truy vấn 7: Diễn biến Biến động Thời gian thực (`time_series_trend.sql`)
*Đồ thị xu hướng qua các khung 15 phút liên tiếp của phiên demo.*

- **Thông tin hiển thị:**
  - Chuỗi mốc thời gian liên tiếp (`window_start`: 08:00, 08:15, 08:30, 08:45...).
  - Số chuyến thực tế theo thời gian so với đường chuẩn lịch sử (Baseline) của các khu vực trọng điểm.
  - Cho biết nhu cầu đang dốc lên (bắt đầu cao điểm) hay dốc xuống (hạ nhiệt) để dừng điều xe.

---

## 3. TỔNG KẾT BẢNG ĐỐI CHIẾU NHIỆM VỤ

| Yêu cầu của Nhóm trưởng | Tên Truy vấn thực hiện | Nguồn bảng dữ liệu |
| :--- | :--- | :--- |
| **Khu vực còn điều phối được taxi** | Truy vấn 2: `taxi_dispatch_matrix.sql` (`SURPLUS / CAN_REALLOCATE`) | `replay_pickup_counts` + `historical_baseline` + `taxi_zones` |
| **Mật độ taxi trong vùng / quận** | Truy vấn 3: `zone_and_borough_density.sql` | `replay_pickup_counts` + `taxi_zones` |
| **Số chuyến taxi theo kỳ (tháng/phiên)** | Truy vấn 5: `trip_volume_summary.sql` | `replay_pickup_counts` + `historical_baseline` |
| **Khu vực thiếu xe / Điểm nóng** | Truy vấn 1: `compare_pickup_demand.sql` (Core MVP) | `replay_pickup_counts` + `historical_baseline` + `taxi_zones` |
| **Cao điểm theo thứ / tuần** | Truy vấn 6: `weekly_peak_hours.sql` | `historical_baseline` |
| **Cửa ngõ Sân bay & Ga tàu** | Truy vấn 4: `airport_and_transit_hubs.sql` | `replay_pickup_counts` + `taxi_zones` |

---

*Tài liệu này đã được đồng bộ vào repository để làm căn cứ triển khai cho các bước tiếp theo.*
