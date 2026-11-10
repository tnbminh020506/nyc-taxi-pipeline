-- Bước 3.3 | Bảo | Truy vấn câu hỏi demo trên PostgreSQL.
--
-- Đầu vào
-- - replay_id và window_start có múi giờ của cửa sổ cần xem.
-- - replay_pickup_counts, historical_baseline và taxi_zones.
--
-- Đầu ra
-- - Một dòng cho mỗi vùng có số liệu quan sát trong cửa sổ, gồm pickup_zone_id,
--   zone_name, observed_trip_count, baseline_trip_count, reference_windows,
--   change_pct và change_unavailable_reason.
--
-- Yêu cầu
-- - Đổi window_start sang America/New_York để tìm iso_weekday và slot_15m.
-- - Giữ dòng quan sát nếu thiếu tên vùng hoặc baseline. Khi thiếu tên, hiển thị mã vùng.
-- - change_pct là 100 × (quan sát − lịch sử) / lịch sử. Trả NULL và lý do khi thiếu
--   baseline hoặc baseline bằng 0. Mặc định xếp tỷ lệ giảm dần, NULL ở cuối.

WITH observed AS (
    SELECT
        r.replay_id,
        r.pickup_zone_id,
        r.window_start,
        r.window_end,
        r.trip_count AS observed_trip_count,
        EXTRACT(ISODOW FROM (r.window_start AT TIME ZONE 'America/New_York'))::INTEGER AS iso_weekday,
        (
            EXTRACT(HOUR FROM (r.window_start AT TIME ZONE 'America/New_York'))::INTEGER * 4
            + FLOOR(EXTRACT(MINUTE FROM (r.window_start AT TIME ZONE 'America/New_York')) / 15.0)::INTEGER
        ) AS slot_15m
    FROM replay_pickup_counts r
    WHERE r.replay_id = %(replay_id)s
      AND r.window_start = %(window_start)s::TIMESTAMPTZ
)
SELECT
    o.pickup_zone_id,
    COALESCE(NULLIF(btrim(z.zone_name), ''), o.pickup_zone_id::TEXT) AS zone_name,
    o.observed_trip_count,
    b.baseline_trip_count,
    b.reference_windows,
    CASE
        WHEN b.baseline_trip_count IS NULL THEN NULL
        WHEN b.baseline_trip_count = 0 THEN NULL
        ELSE ROUND(
            100.0 * (o.observed_trip_count::NUMERIC - b.baseline_trip_count) / b.baseline_trip_count,
            2
        )
    END AS change_pct,
    CASE
        WHEN b.baseline_trip_count IS NULL THEN 'missing_baseline'
        WHEN b.baseline_trip_count = 0 THEN 'zero_baseline'
        ELSE NULL
    END AS change_unavailable_reason
FROM observed o
LEFT JOIN taxi_zones z
    ON z.pickup_zone_id = o.pickup_zone_id
LEFT JOIN historical_baseline b
    ON b.pickup_zone_id = o.pickup_zone_id
   AND b.iso_weekday = o.iso_weekday
   AND b.slot_15m = o.slot_15m
ORDER BY
    change_pct DESC NULLS LAST,
    o.observed_trip_count DESC,
    o.pickup_zone_id ASC;
