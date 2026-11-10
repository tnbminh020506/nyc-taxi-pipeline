-- Truy vấn mở rộng 2 | Bảo | Ma trận Điều phối Taxi (Xác định khu vực còn điều phối được xe & Điểm nóng thiếu xe).
--
-- Đầu vào
-- - replay_id và window_start có múi giờ của cửa sổ cần xem.
--
-- Đầu ra
-- - Trạng thái điều phối (SURGE_NEED_TAXIS, SURPLUS_CAN_REALLOCATE, BALANCED, NEW_ACTIVITY)
--   và gợi ý ghép cặp điều chuyển xe từ vùng dư xe (SURPLUS) sang vùng thiếu xe (SURGE) trong cùng Quận (borough).

WITH comparison AS (
    SELECT
        r.pickup_zone_id,
        COALESCE(NULLIF(btrim(z.zone_name), ''), r.pickup_zone_id::TEXT) AS zone_name,
        COALESCE(NULLIF(btrim(z.borough), ''), 'Unknown') AS borough,
        r.trip_count AS observed_trip_count,
        b.baseline_trip_count,
        (r.trip_count::NUMERIC - COALESCE(b.baseline_trip_count, 0)) AS net_trip_diff,
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
      AND r.window_start = %(window_start)s::TIMESTAMPTZ
),
classified AS (
    SELECT
        c.*,
        CASE
            WHEN c.baseline_trip_count IS NULL OR c.baseline_trip_count = 0 THEN 'NEW_ACTIVITY'
            WHEN c.change_pct >= 30.0 THEN 'SURGE_NEED_TAXIS'
            WHEN c.change_pct <= -30.0 THEN 'SURPLUS_CAN_REALLOCATE'
            ELSE 'BALANCED'
        END AS dispatch_action
    FROM comparison c
),
top_surge_per_borough AS (
    SELECT DISTINCT ON (borough)
        borough,
        pickup_zone_id AS target_zone_id,
        zone_name AS target_zone_name,
        change_pct AS target_change_pct
    FROM classified
    WHERE dispatch_action = 'SURGE_NEED_TAXIS'
    ORDER BY borough, change_pct DESC, net_trip_diff DESC
)
SELECT
    cl.pickup_zone_id,
    cl.zone_name,
    cl.borough,
    cl.observed_trip_count,
    cl.baseline_trip_count,
    ROUND(cl.net_trip_diff, 2) AS net_trip_diff,
    cl.change_pct,
    cl.dispatch_action,
    CASE
        WHEN cl.dispatch_action = 'SURPLUS_CAN_REALLOCATE' AND ts.target_zone_name IS NOT NULL
            THEN 'Điều chuyển xe sang ' || ts.target_zone_name || ' (#' || ts.target_zone_id || ', +' || ts.target_change_pct || '%)'
        WHEN cl.dispatch_action = 'SURPLUS_CAN_REALLOCATE'
            THEN 'Khu vực dư xe - Sẵn sàng điều phối liên quận'
        WHEN cl.dispatch_action = 'SURGE_NEED_TAXIS'
            THEN 'Điểm nóng thiếu xe - Cần tiếp nhận xe chi viện'
        WHEN cl.dispatch_action = 'NEW_ACTIVITY'
            THEN 'Phát sinh nhu cầu mới ngoài lịch sử'
        ELSE 'Duy trì đội xe hiện tại'
    END AS dispatch_recommendation
FROM classified cl
LEFT JOIN top_surge_per_borough ts
    ON ts.borough = cl.borough
ORDER BY
    CASE cl.dispatch_action
        WHEN 'SURGE_NEED_TAXIS' THEN 1
        WHEN 'SURPLUS_CAN_REALLOCATE' THEN 2
        WHEN 'NEW_ACTIVITY' THEN 3
        ELSE 4
    END,
    cl.change_pct DESC NULLS LAST,
    cl.pickup_zone_id ASC;

