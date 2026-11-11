# KẾT QUẢ TEST STANDALONE KHO DỮ LIỆU POSTGRESQL (TỰ SINH RANDOM DATA)
> **Thời điểm chạy test:** 2026-11-11 20:41:51
> **Phiên replay:** `demo-replay-test-01` | **Cửa sổ khảo sát:** `2025-01-20T08:00:00-05:00`
> **Số lượng vùng khảo sát ngẫu nhiên:** 3 khu vực

---
## 1. BÁO CÁO CỐT LÕI (CORE MVP 3.3): SO SÁNH NHU CẦU ĐÓN KHÁCH
| pickup_zone_id | zone_name                 | observed_trip_count | baseline_trip_count | reference_windows | change_pct | change_unavailable_reason |
| -------------- | ------------------------- | ------------------- | ------------------- | ----------------- | ---------- | ------------------------- |
| 161            | Midtown Center            | 30                  | 33.77               | 2                 | -11.16     | NULL                      |
| 230            | Times Sq/Theatre District | 4                   | 38.37               | 2                 | -89.58     | NULL                      |
| 132            | JFK Airport               | 8                   | 0.0                 | 2                 | nan        | zero_baseline             |
| 999            | 999                       | 8                   | nan                 | NULL              | nan        | missing_baseline          |

## 2. MA TRẬN ĐIỀU PHỐI TAXI (ĐIỂM NÓNG SURGE & VÙNG CÒN XE ĐIỀU PHỐI SURPLUS)
| pickup_zone_id | zone_name                 | borough   | observed_trip_count | baseline_trip_count | net_trip_diff | change_pct | dispatch_action        | dispatch_recommendation                      |
| -------------- | ------------------------- | --------- | ------------------- | ------------------- | ------------- | ---------- | ---------------------- | -------------------------------------------- |
| 230            | Times Sq/Theatre District | Manhattan | 4                   | 38.37               | -34.37        | -89.58     | SURPLUS_CAN_REALLOCATE | Khu vực dư xe - Sẵn sàng điều phối liên quận |
| 132            | JFK Airport               | Queens    | 8                   | 0.0                 | 8.0           | nan        | NEW_ACTIVITY           | Phát sinh nhu cầu mới ngoài lịch sử          |
| 999            | 999                       | Unknown   | 8                   | nan                 | 8.0           | nan        | NEW_ACTIVITY           | Phát sinh nhu cầu mới ngoài lịch sử          |
| 161            | Midtown Center            | Manhattan | 30                  | 33.77               | -3.77         | -11.16     | BALANCED               | Duy trì đội xe hiện tại                      |

## 3. MẬT ĐỘ ĐÓN KHÁCH THEO TỪNG VÙNG (%)
| pickup_zone_id | zone_name                 | borough   | observed_trip_count | borough_total_trips | city_total_trips | zone_in_borough_share_pct | city_share_pct |
| -------------- | ------------------------- | --------- | ------------------- | ------------------- | ---------------- | ------------------------- | -------------- |
| 161            | Midtown Center            | Manhattan | 30                  | 34.0                | 50.0             | 88.24                     | 60.0           |
| 132            | JFK Airport               | Queens    | 8                   | 8.0                 | 50.0             | 100.0                     | 16.0           |
| 999            | 999                       | Unknown   | 8                   | 8.0                 | 50.0             | 100.0                     | 16.0           |
| 230            | Times Sq/Theatre District | Manhattan | 4                   | 34.0                | 50.0             | 11.76                     | 8.0            |

## 4. GIÁM SÁT SÂN BAY & CỬA NGÕ HUYẾT MẠCH
| pickup_zone_id | zone_name                 | borough   | observed_trip_count | baseline_trip_count | change_pct |
| -------------- | ------------------------- | --------- | ------------------- | ------------------- | ---------- |
| 161            | Midtown Center            | Manhattan | 30                  | 33.77               | -11.16     |
| 132            | JFK Airport               | Queens    | 8                   | 0.0                 | nan        |
| 230            | Times Sq/Theatre District | Manhattan | 4                   | 38.37               | -89.58     |

## 5. TỔNG HỢP SẢN LƯỢNG CHUYẾN ĐI CỦA PHIÊN REPLAY
| replay_id           | total_windows_observed | total_active_zones | total_observed_trips | first_window_start        | last_window_end           | peak_window_start         | peak_window_trips |
| ------------------- | ---------------------- | ------------------ | -------------------- | ------------------------- | ------------------------- | ------------------------- | ----------------- |
| demo-replay-test-01 | 3                      | 4                  | 284.0                | 2025-01-20 20:00:00+07:00 | 2025-01-20 20:45:00+07:00 | 2025-01-20 20:30:00+07:00 | 138.0             |

## 6. DIỄN BIẾN CHUỖI THỜI GIAN QUA CÁC CỬA SỔ 15 PHÚT
| window_start              | window_end                | pickup_zone_id | zone_name                 | observed_trip_count | baseline_trip_count | change_pct |
| ------------------------- | ------------------------- | -------------- | ------------------------- | ------------------- | ------------------- | ---------- |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 161            | Midtown Center            | 30                  | 33.77               | -11.16     |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 132            | JFK Airport               | 8                   | 0.0                 | nan        |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 999            | 999                       | 8                   | nan                 | nan        |
| 2025-01-20 20:00:00+07:00 | 2025-01-20 20:15:00+07:00 | 230            | Times Sq/Theatre District | 4                   | 38.37               | -89.58     |
| 2025-01-20 20:15:00+07:00 | 2025-01-20 20:30:00+07:00 | 161            | Midtown Center            | 47                  | nan                 | nan        |
| 2025-01-20 20:15:00+07:00 | 2025-01-20 20:30:00+07:00 | 132            | JFK Airport               | 43                  | nan                 | nan        |
| 2025-01-20 20:15:00+07:00 | 2025-01-20 20:30:00+07:00 | 230            | Times Sq/Theatre District | 6                   | nan                 | nan        |
| 2025-01-20 20:30:00+07:00 | 2025-01-20 20:45:00+07:00 | 230            | Times Sq/Theatre District | 57                  | nan                 | nan        |
| 2025-01-20 20:30:00+07:00 | 2025-01-20 20:45:00+07:00 | 161            | Midtown Center            | 47                  | nan                 | nan        |
| 2025-01-20 20:30:00+07:00 | 2025-01-20 20:45:00+07:00 | 132            | JFK Airport               | 34                  | nan                 | nan        |

## 7. TOP KHUNG GIỜ CAO ĐIỂM LỊCH SỬ TRONG TUẦN
| iso_weekday | slot_15m | slot_time_ny | zones_with_baseline | citywide_baseline_trips | avg_zone_baseline_trips | history_start             | history_end               |
| ----------- | -------- | ------------ | ------------------- | ----------------------- | ----------------------- | ------------------------- | ------------------------- |
| 1           | 32       | 8.:00        | 3                   | 72.14                   | 24.05                   | 2025-01-06 20:00:00+07:00 | 2025-01-13 20:15:00+07:00 |
