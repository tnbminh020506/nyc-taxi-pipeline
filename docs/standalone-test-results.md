# KẾT QUẢ TEST STANDALONE KHO DỮ LIỆU POSTGRESQL (TỰ SINH RANDOM DATA)
> **Thời điểm chạy test:** 2026-11-10 22:59:18
> **Phiên replay:** `demo-replay-test-01` | **Cửa sổ khảo sát:** `2025-01-20T08:00:00-05:00`
> **Số lượng vùng khảo sát ngẫu nhiên:** 12 khu vực

---
## 1. BÁO CÁO CỐT LÕI (CORE MVP 3.3): SO SÁNH NHU CẦU ĐÓN KHÁCH
| pickup_zone_id | zone_name                 | observed_trip_count | baseline_trip_count | reference_windows | change_pct | change_unavailable_reason |
| -------------- | ------------------------- | ------------------- | ------------------- | ----------------- | ---------- | ------------------------- |
| 142            | Lincoln Square East       | 51                  | 6.43                | 2                 | 693.16     | NULL                      |
| 237            | Upper East Side South     | 55                  | 14.84               | 4                 | 270.62     | NULL                      |
| 1              | Newark Airport            | 48                  | 13.95               | 4                 | 244.09     | NULL                      |
| 138            | LaGuardia Airport         | 28                  | 11.28               | 2                 | 148.23     | NULL                      |
| 79             | East Village              | 17                  | 8.91                | 3                 | 90.8       | NULL                      |
| 61             | Crown Heights North       | 37                  | 35.45               | 4                 | 4.37       | NULL                      |
| 161            | Midtown Center            | 30                  | 33.77               | 2                 | -11.16     | NULL                      |
| 43             | Central Park              | 10                  | 36.56               | 3                 | -72.65     | NULL                      |
| 230            | Times Sq/Theatre District | 4                   | 38.37               | 2                 | -89.58     | NULL                      |
| 48             | Clinton East              | 0                   | 32.09               | 4                 | -100.0     | NULL                      |
... (Hiển thị 10/13 dòng)

## 2. MA TRẬN ĐIỀU PHỐI TAXI (ĐIỂM NÓNG SURGE & VÙNG CÒN XE ĐIỀU PHỐI SURPLUS)
| pickup_zone_id | zone_name                 | borough   | observed_trip_count | baseline_trip_count | net_trip_diff | change_pct | dispatch_action        | dispatch_recommendation                                  |
| -------------- | ------------------------- | --------- | ------------------- | ------------------- | ------------- | ---------- | ---------------------- | -------------------------------------------------------- |
| 142            | Lincoln Square East       | Manhattan | 51                  | 6.43                | 44.57         | 693.16     | SURGE_NEED_TAXIS       | Điểm nóng thiếu xe - Cần tiếp nhận xe chi viện           |
| 237            | Upper East Side South     | Manhattan | 55                  | 14.84               | 40.16         | 270.62     | SURGE_NEED_TAXIS       | Điểm nóng thiếu xe - Cần tiếp nhận xe chi viện           |
| 1              | Newark Airport            | EWR       | 48                  | 13.95               | 34.05         | 244.09     | SURGE_NEED_TAXIS       | Điểm nóng thiếu xe - Cần tiếp nhận xe chi viện           |
| 138            | LaGuardia Airport         | Queens    | 28                  | 11.28               | 16.72         | 148.23     | SURGE_NEED_TAXIS       | Điểm nóng thiếu xe - Cần tiếp nhận xe chi viện           |
| 79             | East Village              | Manhattan | 17                  | 8.91                | 8.09          | 90.8       | SURGE_NEED_TAXIS       | Điểm nóng thiếu xe - Cần tiếp nhận xe chi viện           |
| 43             | Central Park              | Manhattan | 10                  | 36.56               | -26.56        | -72.65     | SURPLUS_CAN_REALLOCATE | Điều chuyển xe sang Lincoln Square East (#142, +693.16%) |
| 230            | Times Sq/Theatre District | Manhattan | 4                   | 38.37               | -34.37        | -89.58     | SURPLUS_CAN_REALLOCATE | Điều chuyển xe sang Lincoln Square East (#142, +693.16%) |
| 48             | Clinton East              | Manhattan | 0                   | 32.09               | -32.09        | -100.0     | SURPLUS_CAN_REALLOCATE | Điều chuyển xe sang Lincoln Square East (#142, +693.16%) |
| 24             | Bloomingdale              | Manhattan | 51                  | nan                 | 51.0          | nan        | NEW_ACTIVITY           | Phát sinh nhu cầu mới ngoài lịch sử                      |
| 132            | JFK Airport               | Queens    | 14                  | 0.0                 | 14.0          | nan        | NEW_ACTIVITY           | Phát sinh nhu cầu mới ngoài lịch sử                      |
... (Hiển thị 10/13 dòng)

## 3. MẬT ĐỘ ĐÓN KHÁCH THEO TỪNG VÙNG (%)
| pickup_zone_id | zone_name             | borough   | observed_trip_count | borough_total_trips | city_total_trips | zone_in_borough_share_pct | city_share_pct |
| -------------- | --------------------- | --------- | ------------------- | ------------------- | ---------------- | ------------------------- | -------------- |
| 237            | Upper East Side South | Manhattan | 55                  | 218.0               | 353.0            | 25.23                     | 15.58          |
| 24             | Bloomingdale          | Manhattan | 51                  | 218.0               | 353.0            | 23.39                     | 14.45          |
| 142            | Lincoln Square East   | Manhattan | 51                  | 218.0               | 353.0            | 23.39                     | 14.45          |
| 1              | Newark Airport        | EWR       | 48                  | 48.0                | 353.0            | 100.0                     | 13.6           |
| 61             | Crown Heights North   | Brooklyn  | 37                  | 37.0                | 353.0            | 100.0                     | 10.48          |
| 161            | Midtown Center        | Manhattan | 30                  | 218.0               | 353.0            | 13.76                     | 8.5            |
| 138            | LaGuardia Airport     | Queens    | 28                  | 42.0                | 353.0            | 66.67                     | 7.93           |
| 79             | East Village          | Manhattan | 17                  | 218.0               | 353.0            | 7.8                       | 4.82           |
| 132            | JFK Airport           | Queens    | 14                  | 42.0                | 353.0            | 33.33                     | 3.97           |
| 43             | Central Park          | Manhattan | 10                  | 218.0               | 353.0            | 4.59                      | 2.83           |
... (Hiển thị 10/13 dòng)

## 4. GIÁM SÁT SÂN BAY & CỬA NGÕ HUYẾT MẠCH
| pickup_zone_id | zone_name                 | borough   | observed_trip_count | baseline_trip_count | change_pct |
| -------------- | ------------------------- | --------- | ------------------- | ------------------- | ---------- |
| 1              | Newark Airport            | EWR       | 48                  | 13.95               | 244.09     |
| 161            | Midtown Center            | Manhattan | 30                  | 33.77               | -11.16     |
| 138            | LaGuardia Airport         | Queens    | 28                  | 11.28               | 148.23     |
| 132            | JFK Airport               | Queens    | 14                  | 0.0                 | nan        |
| 230            | Times Sq/Theatre District | Manhattan | 4                   | 38.37               | -89.58     |

## 5. TỔNG HỢP SẢN LƯỢNG CHUYẾN ĐI CỦA PHIÊN REPLAY
| replay_id           | total_windows_observed | total_active_zones | total_observed_trips | first_window_start        | last_window_end           | peak_window_start         | peak_window_trips |
| ------------------- | ---------------------- | ------------------ | -------------------- | ------------------------- | ------------------------- | ------------------------- | ----------------- |
| demo-replay-test-01 | 3                      | 13                 | 974.0                | 2025-01-20 20:00:00+07:00 | 2025-01-20 20:45:00+07:00 | 2025-01-20 20:30:00+07:00 | 380.0             |

## 6. DIỄN BIẾN CHUỖI THỜI GIAN QUA CÁC CỬA SỔ 15 PHÚT
| window_start              | window_end                | pickup_zone_id | zone_name             | observed_trip_count | baseline_trip_count | change_pct |
| ------------------------- | ------------------------- | -------------- | --------------------- | ------------------- | ------------------- | ---------- |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 237            | Upper East Side South | 55                  | 14.84               | 270.62     |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 24             | Bloomingdale          | 51                  | nan                 | nan        |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 142            | Lincoln Square East   | 51                  | 6.43                | 693.16     |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 1              | Newark Airport        | 48                  | 13.95               | 244.09     |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 61             | Crown Heights North   | 37                  | 35.45               | 4.37       |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 161            | Midtown Center        | 30                  | 33.77               | -11.16     |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 138            | LaGuardia Airport     | 28                  | 11.28               | 148.23     |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 79             | East Village          | 17                  | 8.91                | 90.8       |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 132            | JFK Airport           | 14                  | 0.0                 | nan        |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 43             | Central Park          | 10                  | 36.56               | -72.65     |
... (Hiển thị 10/37 dòng)

## 7. TOP KHUNG GIỜ CAO ĐIỂM LỊCH SỬ TRONG TUẦN
| iso_weekday | slot_15m | slot_time_ny | zones_with_baseline | citywide_baseline_trips | avg_zone_baseline_trips | history_start             | history_end               |
| ----------- | -------- | ------------ | ------------------- | ----------------------- | ----------------------- | ------------------------- | ------------------------- |
| 1           | 32       | 8.:00        | 11                  | 231.65                  | 21.06                   | 2025-01-06 20:00:00+07:00 | 2025-01-13 20:15:00+07:00 |
