-- Truy vấn mở rộng 3 | Bảo | Mật độ Taxi theo Khu vực và Tỷ trọng theo Quận (Zone & Borough Density).
--
-- Đầu vào
-- - replay_id và window_start có múi giờ của cửa sổ cần xem.
--
-- Đầu ra
-- - Tỷ trọng chuyến đón của từng vùng trên toàn thành phố (city_share_pct)
--   và tỷ trọng của vùng trong Quận (borough_share_pct).

WITH zone_counts AS (
    SELECT
        r.pickup_zone_id,
        COALESCE(NULLIF(btrim(z.zone_name), ''), r.pickup_zone_id::TEXT) AS zone_name,
        COALESCE(NULLIF(btrim(z.borough), ''), 'Unknown') AS borough,
        r.trip_count AS observed_trip_count,
        SUM(r.trip_count) OVER () AS city_total_trips,
        SUM(r.trip_count) OVER (PARTITION BY COALESCE(NULLIF(btrim(z.borough), ''), 'Unknown')) AS borough_total_trips
    FROM replay_pickup_counts r
    LEFT JOIN taxi_zones z
        ON z.pickup_zone_id = r.pickup_zone_id
    WHERE r.replay_id = %(replay_id)s
      AND r.window_start = %(window_start)s::TIMESTAMPTZ
)
SELECT
    pickup_zone_id,
    zone_name,
    borough,
    observed_trip_count,
    borough_total_trips,
    city_total_trips,
    CASE
        WHEN borough_total_trips > 0
            THEN ROUND(100.0 * observed_trip_count::NUMERIC / borough_total_trips, 2)
        ELSE 0.0
    END AS zone_in_borough_share_pct,
    CASE
        WHEN city_total_trips > 0
            THEN ROUND(100.0 * observed_trip_count::NUMERIC / city_total_trips, 2)
        ELSE 0.0
    END AS city_share_pct
FROM zone_counts
ORDER BY
    observed_trip_count DESC,
    pickup_zone_id ASC;
