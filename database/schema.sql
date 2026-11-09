-- Bước 3.1 | Bảo | Khởi tạo PostgreSQL.
--
-- Đầu vào
-- - Cơ sở dữ liệu demo và ba hợp đồng nạp trong database/load/.
--
-- Đầu ra
-- - taxi_zones có một dòng mỗi pickup_zone_id, gồm zone_name và borough từ lookup.
-- - historical_baseline có khóa (pickup_zone_id, iso_weekday, slot_15m) và cột
--   baseline_trip_count, reference_windows, history_start, history_end.
-- - replay_pickup_counts có khóa (replay_id, pickup_zone_id, window_start) và cột
--   window_end, trip_count.
--
-- Yêu cầu
-- - Chỉ dùng một kỳ tham chiếu đang hoạt động. Lưu các mốc thời gian dưới dạng timestamptz.
-- - Dùng kiểu số phù hợp cho trung bình có phần thập phân và cho số đếm nguyên.
-- - Cho phép các bảng số liệu giữ mã vùng chưa có tên trong taxi_zones.

-- 1. Bảng danh mục khu vực đón khách (từ Taxi Zone Lookup CSV)
-- Lưu ý: Không đặt Foreign Key từ các bảng số liệu sang bảng này để cho phép
-- giữ lại các mã vùng hợp lệ (> 0) nhưng chưa có tên trong bảng tra cứu.
CREATE TABLE IF NOT EXISTS taxi_zones (
    pickup_zone_id      INTEGER PRIMARY KEY,
    zone_name           TEXT NOT NULL,
    borough             TEXT NOT NULL,
    CONSTRAINT chk_taxi_zones_pickup_zone_id CHECK (pickup_zone_id > 0),
    CONSTRAINT chk_taxi_zones_zone_name_not_blank CHECK (btrim(zone_name) <> ''),
    CONSTRAINT chk_taxi_zones_borough_not_blank CHECK (btrim(borough) <> '')
);

-- 2. Bảng mức chuẩn lịch sử (từ Spark Batch)
-- Chỉ lưu một kỳ tham chiếu đang hoạt động tại một thời điểm (thay thế trọn bộ trong 1 transaction khi nạp).
-- Múi giờ nghiệp vụ: America/New_York
-- iso_weekday: 1 (Thứ Hai) -> 7 (Chủ Nhật)
-- slot_15m: 0 (00:00-00:15) -> 95 (23:45-24:00)
CREATE TABLE IF NOT EXISTS historical_baseline (
    pickup_zone_id      INTEGER NOT NULL,
    iso_weekday         INTEGER NOT NULL,
    slot_15m            INTEGER NOT NULL,
    baseline_trip_count NUMERIC NOT NULL,
    reference_windows   INTEGER NOT NULL,
    history_start       TIMESTAMPTZ NOT NULL,
    history_end         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pickup_zone_id, iso_weekday, slot_15m),
    CONSTRAINT chk_hist_pickup_zone_id CHECK (pickup_zone_id > 0),
    CONSTRAINT chk_hist_iso_weekday CHECK (iso_weekday BETWEEN 1 AND 7),
    CONSTRAINT chk_hist_slot_15m CHECK (slot_15m BETWEEN 0 AND 95),
    CONSTRAINT chk_hist_baseline_trip_count CHECK (baseline_trip_count >= 0),
    CONSTRAINT chk_hist_reference_windows CHECK (reference_windows > 0),
    CONSTRAINT chk_hist_time_range CHECK (history_end > history_start)
);

-- 3. Bảng số chuyến phát lại đếm lũy kế theo cửa sổ 15 phút (từ Spark Structured Streaming)
CREATE TABLE IF NOT EXISTS replay_pickup_counts (
    replay_id           TEXT NOT NULL,
    pickup_zone_id      INTEGER NOT NULL,
    window_start        TIMESTAMPTZ NOT NULL,
    window_end          TIMESTAMPTZ NOT NULL,
    trip_count          BIGINT NOT NULL,
    PRIMARY KEY (replay_id, pickup_zone_id, window_start),
    CONSTRAINT chk_replay_id_not_blank CHECK (btrim(replay_id) <> ''),
    CONSTRAINT chk_replay_pickup_zone_id CHECK (pickup_zone_id > 0),
    CONSTRAINT chk_replay_trip_count CHECK (trip_count >= 0),
    CONSTRAINT chk_replay_window CHECK (window_end > window_start)
);

-- Các Index tối ưu hóa truy vấn so sánh theo phiên + cửa sổ và JOIN theo thứ + khung 15 phút
CREATE INDEX IF NOT EXISTS idx_replay_pickup_counts_session_window
    ON replay_pickup_counts (replay_id, window_start);

CREATE INDEX IF NOT EXISTS idx_historical_baseline_weekday_slot
    ON historical_baseline (iso_weekday, slot_15m);
