# Đặc tả Danh mục Truy vấn & Báo cáo Điều hành Taxi (NYC Taxi Pipeline)

> **Người soạn:** Uông Gia Bảo (Data Warehouse & Analytics)  
> **Gửi tới:** Nhóm trưởng, Chính (Data Ingestion), Mạnh (Data Processing)  
> **Mục tiêu:** Thống nhất toàn bộ danh mục thông tin truy vấn phục vụ demo, báo cáo và giải bài toán điều phối taxi cho trung tâm tổng, kèm đánh giá tính khả thi kỹ thuật dựa trên 3 bảng dữ liệu hiện có trong kho PostgreSQL.

---

## 1. BẢNG TỔNG KẾT ĐÁNH GIÁ TÍNH KHẢ THI (DỰA TRÊN 3 BẢNG POSTGRESQL HIỆN CÓ)

Dưới đây là bảng đối chiếu toàn bộ các nhu cầu thông tin điều hành, nêu rõ **Khả thi hay Không khả thi** và giải thích kỹ thuật:

| STT | Thông tin / Chỉ số mong muốn | Khả thi trên 3 bảng hiện có? | File / Cách xử lý kỹ thuật | Lý do & Giới hạn dữ liệu |
| :---: | :--- | :---: | :--- | :--- |
| **1** | **So sánh nhu cầu đón khách 15 phút (Core MVP)** |  **Khả thi** | `compare_pickup_demand.sql` | So khớp trực tiếp `replay_pickup_counts` và `historical_baseline` theo khung 15p. *(Đạt 100% tiêu chí nghiệm thu).* |
| **2** | **Xác định khu vực còn điều phối được xe** |  **Khả thi** | `taxi_dispatch_matrix.sql` | Lọc các vùng có số chuyến thực tế giảm mạnh so với lịch sử (`change_pct <= -30%`), dư xe để điều đi. |
| **3** | **Gợi ý ghép cặp điều xe theo Quận** |  **Khả thi** | `taxi_dispatch_matrix.sql` | Tự động ghép vùng thừa xe (`SURPLUS`) sang vùng thiếu xe (`SURGE`) trong cùng một `borough` (Quận). |
| **4** | **Mật độ đón khách theo từng Vùng (%)** |  **Khả thi** | `zone_and_borough_density.sql` | Lấy số chuyến của vùng chia cho tổng số chuyến toàn thành phố trong cùng khung 15 phút (`OVER()`). |
| **5** | **Tỷ trọng / Mật độ đón khách theo Quận (%)** |  **Khả thi** | `zone_and_borough_density.sql` | `GROUP BY borough`, tính tỷ lệ đóng góp của từng Quận (Manhattan, Queens, Brooklyn...) vào lưu lượng NYC. |
| **6** | **Giám sát Cửa ngõ Sân bay & Ga tàu** |  **Khả thi** | `airport_and_transit_hubs.sql` | Lọc theo `zone_name` hoặc ID của JFK Airport, LaGuardia (LGA), Newark (EWR), Penn Station... |
| **7** | **Bản đồ nhiệt Giờ cao điểm trong tuần** |  **Khả thi** | `weekly_peak_hours.sql` | Khai thác từ `historical_baseline`, tìm thứ mấy và khung 15p nào có lượng đón khách trung bình cao nhất. |
| **8** | **Xu hướng nhu cầu theo thời gian (Time-series)** |  **Khả thi** | `time_series_trend.sql` | Đồ thị biến động qua các `window_start` liên tiếp (08:00 -> 08:15 -> 08:30) của phiên replay. |
| **9** | **Tổng số chuyến trong kỳ demo / phiên replay** |  **Khả thi** | `trip_volume_summary.sql` | `SUM(trip_count)` qua các cửa sổ của phiên phát lại hiện tại. |
| **10** | **Tổng số chuyến taxi trong tháng / cả năm** | ❌ **Không khả thi** | *Không lưu trong kho* | Kho PostgreSQL chỉ lưu dữ liệu tổng hợp (mức trung bình lịch sử và vài khung giờ của phiên replay demo), không lưu toàn bộ cuốc thô của cả năm. |
| **11** | **Số lượng xe trống / Tỷ lệ Cung - Cầu** | ❌ **Không khả thi** | *Nguồn không có* | Dữ liệu NYC Open Data chỉ ghi nhận cuốc xe đã đón khách thành công (`pickup_datetime`), không có GPS xe trống đang chạy tìm khách. |
| **12** | **Thời gian khách chờ xe (ETA) & Tỷ lệ hủy cuốc** | ❌ **Không khả thi** | *Nguồn không có* | Dữ liệu nguồn mở không phải dữ liệu app đặt xe (không có nhật ký bấm gọi xe, không có cuốc bị tài xế/khách hủy). |

---

## 2. CHI TIẾT CÁC TRUY VẤN KHẢ THI SẼ TRIỂN KHAI

### NHÓM A: TRUY VẤN NGHIỆM THU BẮT BUỘC (CORE MVP)

#### Truy vấn 1: So sánh Nhu cầu Đón khách theo Cửa sổ 15 phút (`compare_pickup_demand.sql`)
- **Trạng thái:**  **Khả thi 100%**.
- **Đầu vào:** `replay_id` và mốc giờ `window_start` (ví dụ: `2025-01-20 08:00:00-05`).
- **Thông tin hiển thị (7 cột bắt buộc):**
  1. `pickup_zone_id`: Mã khu vực đón khách.
  2. `zone_name`: Tên khu vực (fallback sang mã vùng nếu khuyết tên).
  3. `observed_trip_count`: Số chuyến đón thực tế từ Stream.
  4. `baseline_trip_count`: Mức trung bình lịch sử (cùng Thứ, cùng Khung 15p).
  5. `reference_windows`: Số cửa sổ lịch sử tham chiếu.
  6. `change_pct`: Phần trăm chênh lệch: `100 * (observed - baseline) / baseline`.
  7. `change_unavailable_reason`: Lý do nếu không tính được % (`missing_baseline` hoặc `zero_baseline`).
- **Thứ tự:** `change_pct DESC NULLS LAST`.

---

### NHÓM B: TRUY VẤN NGHIỆP VỤ ĐIỀU HÀNH & ĐIỀU PHỐI (THEO NHÓM TRƯỞNG GIAO)

#### Truy vấn 2: Ma trận Điều phối — Xác định "Khu vực còn điều phối được xe" (`taxi_dispatch_matrix.sql`)
- **Trạng thái:**  **Khả thi 100%**.
- **Ý nghĩa:** Trả lời cho tổng đài biết: *Lấy xe từ đâu và chuyển xe đi đâu?*
- **Thông tin hiển thị:**
  - `borough`: Tên Quận.
  - `zone_name`: Tên khu vực.
  - `observed_trip_count` vs `baseline_trip_count`.
  - `net_trip_diff`: Chênh lệch số chuyến tuyệt đối (`observed - baseline`).
  - `dispatch_action`: Phân loại hành động:
    - **`SURGE / NEED_TAXIS` (Thiếu xe trầm trọng):** Nhu cầu tăng vọt (`change_pct >= +30%`).
    - **`SURPLUS / CAN_REALLOCATE` (Khu vực còn điều phối được xe):** Nhu cầu giảm sâu (`change_pct <= -30%`), tài xế vắng khách, **đây chính là nguồn xe để rút đi chi viện**.
    - **`BALANCED` (Cung cầu ổn định):** Hoạt động bình thường.
  - `suggested_transfer_to`: Tự động gợi ý chuyển xe từ vùng thừa sang vùng thiếu trong cùng Quận.

#### Truy vấn 3: Mật độ Taxi theo Khu vực và theo Quận (`zone_and_borough_density.sql`)
- **Trạng thái:**  **Khả thi 100%**.
- **Ý nghĩa:** Đánh giá mức độ tập trung khách hàng trên toàn bản đồ NYC.
- **Thông tin hiển thị:**
  - **Mật độ theo Vùng (%):** Tỷ trọng số chuyến của từng vùng trên tổng số chuyến toàn thành phố tại khung 15 phút đó (Top 10 điểm nóng nhất).
  - **Mật độ gộp theo Quận (%):** Tỷ lệ thị phần chuyến đón của từng Quận (Manhattan, Queens, Brooklyn...).

#### Truy vấn 4: Giám sát Trọng điểm Cửa ngõ Sân bay & Ga tàu (`airport_and_transit_hubs.sql`)
- **Trạng thái:**  **Khả thi 100%**.
- **Thông tin hiển thị:** Lọc riêng lưu lượng đón khách tại các sân bay lớn (JFK Airport, LaGuardia, Newark) và nhà ga lớn để tổng đài chuẩn bị số lượng xe đón khách theo các đợt chuyến bay hạ cánh.

---

### NHÓM C: TRUY VẤN QUY MÔ THỜI GIAN (LỊCH SỬ & PHIÊN DEMO)

#### Truy vấn 5: Tổng sản lượng chuyến đi của Phiên Demo (`trip_volume_summary.sql`)
- **Trạng thái:**  **Khả thi 100%**.
- **Thông tin hiển thị:** Tổng số chuyến đón khách lũy kế qua các cửa sổ của phiên phát lại (`replay_id`), số vùng hoạt động, và khung giờ có lưu lượng khách cao nhất (Peak Window).

#### Truy vấn 6: Bản đồ Nhiệt Khung giờ Cao điểm trong Tuần (`weekly_peak_hours.sql`)
- **Trạng thái:**  **Khả thi 100%**.
- **Thông tin hiển thị:** Thứ mấy trong tuần và khung 15 phút nào thường có lượng khách lịch sử cao nhất để phân ca trực cho tài xế.

#### Truy vấn 7: Diễn biến Biến động Thời gian thực (`time_series_trend.sql`)
- **Trạng thái:**  **Khả thi 100%**.
- **Thông tin hiển thị:** Biến động số chuyến qua các khung 15 phút liên tiếp (08:00, 08:15, 08:30...) so với baseline để biết xu hướng đang tăng hay đã đạt đỉnh và bắt đầu giảm.

---

## 3. LỜI KHUYÊN KHI TRAO ĐỔI VỚI NHÓM TRƯỞNG

Khi báo cáo với Nhóm trưởng, bạn có thể nêu rõ:
1. **Các mục làm được xuất sắc (Mục 1 đến 9):** Hệ thống giải quyết trọn vẹn bài toán **Phát hiện điểm nóng**, **Xác định khu vực thừa xe để điều phối**, **Tính mật độ theo Vùng/Quận** và **Quy luật cao điểm theo tuần**.
2. **Giải thích các mục không khả thi (Mục 10, 11, 12):** Do đặc thù dữ liệu TLC NYC Open Data không có GPS xe trống và không phải nhật ký app đặt xe, nên ta không có số xe trống và ETA. Đối với số chuyến cả năm, kho dữ liệu đã tối ưu bằng cách lưu mức trung bình lịch sử đại diện thay vì lưu hàng chục gigabyte dữ liệu thô.
