-- Truy vấn mở rộng 6 | Bảo | Bản đồ Nhiệt Khung giờ Cao điểm trong Tuần (Weekly Peak Hours).
--
-- Đầu ra
-- - Top các khung giờ 15 phút có tổng lưu lượng lịch sử toàn thành phố cao nhất.

SELECT
    b.iso_weekday,
    b.slot_15m,
    LPAD((b.slot_15m / 4)::TEXT, 2, '0') || ':' || LPAD(((b.slot_15m %% 4) * 15)::TEXT, 2, '0') AS slot_time_ny,
    COUNT(DISTINCT b.pickup_zone_id) AS zones_with_baseline,
    ROUND(SUM(b.baseline_trip_count), 2) AS citywide_baseline_trips,
    ROUND(AVG(b.baseline_trip_count), 2) AS avg_zone_baseline_trips,
    MIN(b.history_start) AS history_start,
    MAX(b.history_end) AS history_end
FROM historical_baseline b
GROUP BY b.iso_weekday, b.slot_15m
ORDER BY
    citywide_baseline_trips DESC,
    b.iso_weekday ASC,
    b.slot_15m ASC
LIMIT %(limit)s;
