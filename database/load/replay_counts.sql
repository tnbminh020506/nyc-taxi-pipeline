-- Bước 3.2 | Bảo phối hợp Mạnh | Nạp số đếm lũy kế của stream.
--
-- Đầu vào
-- - Các dòng từ một lần cập nhật của Spark gồm replay_id, pickup_zone_id,
--   window_start, window_end và trip_count lũy kế.
--
-- Đầu ra
-- - Bảng replay_pickup_counts có một dòng mỗi vùng/cửa sổ của phiên.
--
-- Yêu cầu
-- - Bảo bàn giao cơ chế ghi và cách truyền dữ liệu để Mạnh nối từ run_stream.py.
-- - Cập nhật theo (replay_id, pickup_zone_id, window_start), gán trip_count bằng tổng mới.
-- - Ghi theo giao dịch và thống nhất một luồng ghi tuần tự cho mỗi replay_id.
-- - Ghi lại cùng lần cập nhật giữ nguyên số dòng và số đếm.

-- Câu lệnh UPSERT chuẩn (đồng bộ với PostgresUpsertSink trong streaming_counts.py
-- và hàm upsert_replay_counts trong warehouse/loader.py):
INSERT INTO replay_pickup_counts (
    replay_id,
    pickup_zone_id,
    window_start,
    window_end,
    trip_count
)
VALUES (
    %(replay_id)s,
    %(pickup_zone_id)s,
    %(window_start)s,
    %(window_end)s,
    %(trip_count)s
)
ON CONFLICT (replay_id, pickup_zone_id, window_start) DO UPDATE
SET
    window_end = EXCLUDED.window_end,
    trip_count = EXCLUDED.trip_count;
