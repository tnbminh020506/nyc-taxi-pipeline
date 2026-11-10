-- Truy vấn mở rộng 4 | Bảo | Giám sát Cửa ngõ Giao thông, Sân bay & Nhà ga lớn (Airports & Transit Hubs).
--
-- Đầu vào
-- - replay_id và window_start có múi giờ của cửa sổ cần xem.

SELECT
    r.pickup_zone_id,
    COALESCE(NULLIF(btrim(z.zone_name), ''), r.pickup_zone_id::TEXT) AS zone_name,
    COALESCE(NULLIF(btrim(z.borough), ''), 'Unknown') AS borough,
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
  AND r.window_start = %(window_start)s::TIMESTAMPTZ
  AND (
      r.pickup_zone_id IN (1, 132, 138, 161, 162, 186, 230)
      OR COALESCE(z.zone_name, '') ILIKE ANY (ARRAY['%%Airport%%', '%%Penn Station%%', '%%Grand Central%%', '%%Times Sq%%'])
  )
ORDER BY
    r.trip_count DESC,
    r.pickup_zone_id ASC;

