# Tài liệu Đặc tả Chi tiết Kho Dữ liệu PostgreSQL (Data Warehouse Spec)

> **Người phụ trách:** Uông Gia Bảo (Data Warehouse & Analytics)  
> **Nhánh Git:** `demo/warehouse`  
> **Phạm vi quản lý:** Thư mục `database/`

---

## 1. TỔNG QUAN PHẠM VI CÔNG VIỆC

Là người chịu trách nhiệm phần **Data Warehouse**, bạn chỉ cần tập trung thao tác trong thư mục `database/` với 3 bảng cốt lõi. 

Các bảng này được thiết kế để:
1. Nhận dữ liệu danh mục vùng đón khách từ bạn **Chính (Ingestion)**.
2. Nhận kết quả tính toán trung bình lịch sử từ bạn **Mạnh (Spark Batch)**.
3. Nhận số đếm thời gian thực từ bạn **Mạnh (Spark Structured Streaming)**.
4. Cung cấp dữ liệu để chạy câu truy vấn so sánh độ lệch nhu cầu đón khách theo từng khung 15 phút.

---

## 2. ĐẶC TẢ CHI TIẾT 3 BẢNG VÀ THUỘC TÍNH

```
   ┌──────────────────────┐
   │      taxi_zones      │
   ├──────────────────────┤
   │ PK  pickup_zone_id   │
   │     zone_name        │
   │     borough          │
   └──────────┬───────────┘
              │ (Không bắt buộc FK cứng - Cho phép mã vùng mới)
              ├─────────────────────────────────────────────┐
              ▼                                             ▼
┌───────────────────────────────┐             ┌───────────────────────────────┐
│      historical_baseline      │             │     replay_pickup_counts      │
├───────────────────────────────┤             ├───────────────────────────────┤
│ PK  pickup_zone_id            │             │ PK  replay_id                 │
│ PK  iso_weekday               │             │ PK  pickup_zone_id            │
│ PK  slot_15m                  │             │ PK  window_start              │
│     baseline_trip_count       │             │     window_end                │
│     reference_windows         │             │     trip_count                │
│     history_start             │             └───────────────────────────────┘
│     history_end               │
└───────────────────────────────┘
```

---

### BẢNG 1: `taxi_zones` (Danh mục Khu vực đón khách)
- **Nguồn cấp:** File CSV `taxi_zone_lookup.csv` do Chính tải về từ NYC Open Data.
- **Mục đích:** Tra cứu tên quận (`borough`) và tên vùng (`zone_name`) từ mã định danh.

| Tên thuộc tính | Kiểu dữ liệu | Ràng buộc kỹ thuật | Ý nghĩa & Nguồn gốc |
| :--- | :--- | :--- | :--- |
| `pickup_zone_id` | `INTEGER` | `PRIMARY KEY`, `> 0` | Mã khu vực đón khách (ánh xạ từ cột `LocationID` trong file CSV TLC). |
| `zone_name` | `TEXT` | `NOT NULL`, `btrim <> ''` | Tên khu vực cụ thể (ánh xạ từ cột `Zone`, ví dụ: *JFK Airport*, *Times Square*). |
| `borough` | `TEXT` | `NOT NULL`, `btrim <> ''` | Tên Quận quản lý (ánh xạ từ cột `Borough`, ví dụ: *Manhattan*, *Queens*, *Brooklyn*). |

---

### BẢNG 2: `historical_baseline` (Mức chuẩn Lịch sử)
- **Nguồn cấp:** Đầu ra của tiến trình Spark Batch (`run_batch.py`) do Mạnh tính toán.
- **Mục đích:** Lưu số lượng chuyến đón khách trung bình của một khu vực trong quá khứ, tương ứng với từng Thứ trong tuần và từng Khung 15 phút.
- **Khóa chính kết hợp (Composite PK):** `(pickup_zone_id, iso_weekday, slot_15m)`

| Tên thuộc tính | Kiểu dữ liệu | Ràng buộc kỹ thuật | Ý nghĩa & Quy ước nghiệp vụ |
| :--- | :--- | :--- | :--- |
| `pickup_zone_id` | `INTEGER` | **PK (1)**, `> 0` | Mã khu vực đón khách. |
| `iso_weekday` | `INTEGER` | **PK (2)**, `1 đến 7` | Thứ trong tuần theo chuẩn ISO: `1` = Thứ Hai, `2` = Thứ Ba, ..., `7` = Chủ Nhật. |
| `slot_15m` | `INTEGER` | **PK (3)**, `0 đến 95` | Chỉ số khung 15 phút trong ngày theo giờ New York (`America/New_York`):<br>• Slot `0`: 00:00 - 00:15<br>• Slot `32`: 08:00 - 08:15<br>• Slot `95`: 23:45 - 24:00 |
| `baseline_trip_count` | `NUMERIC` | `NOT NULL`, `>= 0` | Số chuyến trung bình lịch sử (có phần thập phân, ví dụ: `2.0`, `15.75`). |
| `reference_windows` | `INTEGER` | `NOT NULL`, `> 0` | Số lượng cửa sổ 15 phút có độ phủ được dùng làm mẫu số để chia trung bình (độ tin cậy của baseline). |
| `history_start` | `TIMESTAMPTZ` | `NOT NULL` | Mốc bắt đầu của kỳ lịch sử khảo sát (có múi giờ). |
| `history_end` | `TIMESTAMPTZ` | `NOT NULL`, `> start` | Mốc kết thúc của kỳ lịch sử khảo sát (có múi giờ). |

---

### BẢNG 3: `replay_pickup_counts` (Số chuyến Phát lại từ Stream)
- **Nguồn cấp:** Tiến trình Spark Structured Streaming (`run_stream.py`) do Mạnh kết nối với Kafka.
- **Mục đích:** Lưu tổng số chuyến đón khách đếm được theo thời gian thực tại mỗi cửa sổ 15 phút cho từng phiên chạy (`replay_id`).
- **Khóa chính kết hợp (Composite PK):** `(replay_id, pickup_zone_id, window_start)`

| Tên thuộc tính | Kiểu dữ liệu | Ràng buộc kỹ thuật | Ý nghĩa & Quy ước nghiệp vụ |
| :--- | :--- | :--- | :--- |
| `replay_id` | `TEXT` | **PK (1)**, `btrim <> ''` | Mã phiên chạy giả lập phát lại (ví dụ: `'demo-01'`, `'replay-20250120'`). |
| `pickup_zone_id` | `INTEGER` | **PK (2)**, `> 0` | Mã khu vực đón khách. |
| `window_start` | `TIMESTAMPTZ` | **PK (3)** | Thời điểm bắt đầu của cửa sổ 15 phút (có múi giờ, quy ước tính mốc đầu `[`). |
| `window_end` | `TIMESTAMPTZ` | `NOT NULL`, `> start` | Thời điểm kết thúc của cửa sổ 15 phút (có múi giờ, quy ước loại trừ mốc cuối `)`). |
| `trip_count` | `BIGINT` | `NOT NULL`, `>= 0` | Tổng số chuyến đón khách lũy kế đếm được trong cửa sổ đó (tổng mới nhất). |

---

## 3. CÁC QUY TẮC & KỸ THUẬT QUAN TRỌNG ĐÃ THIẾT LẬP

1. **Không đặt Foreign Key cứng sang `taxi_zones`:**
   - *Yêu cầu đề bài:* Cho phép bảng số liệu lưu trữ các mã vùng hợp lệ (`> 0`) kể cả khi mã đó chưa kịp có tên trong bảng `taxi_zones`. Khi truy vấn, nếu thiếu tên vùng thì hệ thống sẽ hiển thị mã ID.
2. **Múi giờ chuẩn hóa (`America/New_York`):**
   - Mọi mốc thời gian lưu trữ trong DB đều dùng `TIMESTAMPTZ`.
   - Khi truy vấn, hệ thống sẽ chuyển đổi `window_start AT TIME ZONE 'America/New_York'` để suy ra đúng Thứ (`iso_weekday`) và Khung giờ (`slot_15m`).
3. **Chỉ số Index tối ưu tốc độ:**
   - `idx_replay_pickup_counts_session_window` trên `(replay_id, window_start)`: Tối ưu lọc nhanh theo phiên và thời điểm.
   - `idx_historical_baseline_weekday_slot` trên `(iso_weekday, slot_15m)`: Tối ưu JOIN với bảng baseline.
4. **Cơ chế nạp chống trùng (Idempotent):**
   - Khi nạp lại cùng dữ liệu hoặc khi Spark gửi lại cùng micro-batch, hệ thống dùng cơ chế `UPSERT` (`ON CONFLICT DO UPDATE`) để cập nhật số chuyến mới mà không sinh dòng trùng lặp.
