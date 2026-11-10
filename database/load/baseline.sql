-- Bước 3.2 | Bảo | Nạp một kỳ mức lịch sử.
--
-- Đầu vào
-- - Đầu ra đầy đủ của Spark Batch gồm pickup_zone_id, iso_weekday, slot_15m,
--   baseline_trip_count, reference_windows, history_start và history_end.
--
-- Đầu ra
-- - Bảng historical_baseline có một dòng cho mỗi vùng, thứ và khung giờ của kỳ đang dùng.
-- - Báo số dòng đã nạp và số dòng lỗi.
--
-- Yêu cầu
-- - Kiểm tra đầu ra cùng kỳ và duy nhất theo (pickup_zone_id, iso_weekday, slot_15m).
-- - Thay trọn bộ mức lịch sử trong một giao dịch sau khi kiểm tra thành công.
-- - Nạp lại cùng kết quả giữ nguyên số dòng và số liệu.

-- Quy trình nạp chuẩn trong 1 Transaction (được thực thi qua warehouse/loader.py):
-- 1. Kiểm tra toàn bộ dòng đầu vào có cùng cặp (history_start, history_end) và không trùng khóa
--    (pickup_zone_id, iso_weekday, slot_15m).
-- 2. Trong cùng một giao dịch (BEGIN ... COMMIT), làm sạch kỳ cũ bằng TRUNCATE TABLE historical_baseline
--    và chèn trọn bộ kỳ mới:
INSERT INTO historical_baseline (
    pickup_zone_id,
    iso_weekday,
    slot_15m,
    baseline_trip_count,
    reference_windows,
    history_start,
    history_end
)
VALUES (
    %(pickup_zone_id)s,
    %(iso_weekday)s,
    %(slot_15m)s,
    %(baseline_trip_count)s,
    %(reference_windows)s,
    %(history_start)s,
    %(history_end)s
)
ON CONFLICT (pickup_zone_id, iso_weekday, slot_15m) DO UPDATE
SET
    baseline_trip_count = EXCLUDED.baseline_trip_count,
    reference_windows   = EXCLUDED.reference_windows,
    history_start       = EXCLUDED.history_start,
    history_end         = EXCLUDED.history_end;
