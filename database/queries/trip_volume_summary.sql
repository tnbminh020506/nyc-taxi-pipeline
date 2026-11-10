-- Truy vấn mở rộng 5 | Bảo | Tổng hợp Quy mô Sản lượng Chuyến đi của Phiên (Trip Volume Summary).
--
-- Đầu vào
-- - replay_id cần thống kê.

WITH window_totals AS (
    SELECT
        window_start,
        window_end,
        SUM(trip_count) AS window_trips,
        COUNT(DISTINCT pickup_zone_id) AS window_active_zones
    FROM replay_pickup_counts
    WHERE replay_id = %(replay_id)s
    GROUP BY window_start, window_end
),
peak_win AS (
    SELECT
        window_start AS peak_window_start,
        window_trips AS peak_window_trips
    FROM window_totals
    ORDER BY window_trips DESC, window_start ASC
    LIMIT 1
)
SELECT
    %(replay_id)s AS replay_id,
    COUNT(DISTINCT r.window_start) AS total_windows_observed,
    COUNT(DISTINCT r.pickup_zone_id) AS total_active_zones,
    COALESCE(SUM(r.trip_count), 0) AS total_observed_trips,
    MIN(r.window_start) AS first_window_start,
    MAX(r.window_end) AS last_window_end,
    pw.peak_window_start,
    pw.peak_window_trips
FROM replay_pickup_counts r
LEFT JOIN peak_win pw ON TRUE
WHERE r.replay_id = %(replay_id)s
GROUP BY pw.peak_window_start, pw.peak_window_trips;
