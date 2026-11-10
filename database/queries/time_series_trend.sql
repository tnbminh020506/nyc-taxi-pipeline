-- Truy vấn mở rộng 7 | Bảo | Diễn biến Biến động theo Chuỗi Thời gian (Time-Series Rolling Trend).
--
-- Đầu vào
-- - replay_id và tùy chọn pickup_zone_id (nếu NULL thì lấy tổng toàn thành phố theo từng cửa sổ 15 phút).

SELECT
    r.window_start,
    r.window_end,
    r.pickup_zone_id,
    COALESCE(NULLIF(btrim(z.zone_name), ''), r.pickup_zone_id::TEXT) AS zone_name,
    r.trip_count AS observed_trip_count,
    b.baseline_trip_count,
    CASE
        WHEN b.baseline_trip_count IS NULL OR b.baseline_trip_count = 0 THEN NULL
        ELSE ROUND(
            100.0 * (r.trip_count::NUMERIC - b.baseline_trip_count) / b.baseline_trip_count,
            2
        )
    END AS change_pct
FROM replay_pickup_counts r
LEFT JOIN taxi_zones z
    ON z.pickup_zone_id = r.pickup_zone_id
LEFT JOIN historical_baseline b
    ON b.pickup_zone_id = r.pickup_zone_id
   AND b.iso_weekday = EXTRACT(ISODOW FROM (r.window_start AT TIME ZONE 'America/New_York'))::INTEGER
   AND b.slot_15m = (
       EXTRACT(HOUR FROM (r.window_start AT TIME ZONE 'America/New_York'))::INTEGER * 4
       + FLOOR(EXTRACT(MINUTE FROM (r.window_start AT TIME ZONE 'America/New_York')) / 15.0)::INTEGER
   )
WHERE r.replay_id = %(replay_id)s
  AND (%(pickup_zone_id)s IS NULL OR r.pickup_zone_id = %(pickup_zone_id)s)
ORDER BY
    r.window_start ASC,
    r.trip_count DESC,
    r.pickup_zone_id ASC;
