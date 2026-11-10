<!--
Đầu vào
Đường dẫn tệp, lệnh chạy, log và kết quả kiểm tra thực tế của từng nhánh.
Đầu ra
Báo cáo bàn giao theo thành viên và trạng thái tích hợp của cả nhóm.
-->
# Bước 4.2 · Báo cáo MVP

| Phần | Phụ trách | Trạng thái |
| --- | --- | --- |
| Bước 1.1–1.4 · Ingestion và replay | Chính | Đã triển khai trên nhánh `demo/ingestion` |
| Bước 2.1–2.3 · Spark Batch và Structured Streaming | Mạnh | Đã triển khai trên nhánh `demo/pipeline` |
| Bước 3.1–3.3 · PostgreSQL và SQL/Python demo | Bảo | Đã triển khai hoàn chỉnh trên nhánh `demo/warehouse` (Schema, Load, Queries SQL + Python CLI, Unit Tests) |
| Bước 4.1–4.2 · Chốt dữ liệu và ghép demo | Nhóm trưởng | Đã có kịch bản nghiệm thu |

Mỗi thành viên ghi phần đã làm kèm đường dẫn tệp, luồng và hợp đồng bàn giao, lệnh chạy và kết quả thực tế, phần còn vướng và việc cần phối hợp.

Nhóm trưởng ghi dataset và bộ lọc API, số trang và số dòng, Zone Lookup, kỳ lịch sử, kỳ replay, `replay_id` và kết quả SQL của cửa sổ demo. Ghi riêng kết quả trên bộ mẫu và kết quả lấy dữ liệu từ API thật.

---

## Bảo · Bước 3.1–3.3 Data Warehouse & Analytics

### 1. Danh mục tệp đã triển khai

| Bước | Thành phần | Đường dẫn tệp |
| :--- | :--- | :--- |
| **3.1** | Khởi tạo Schema PostgreSQL (3 bảng, CHECK constraints, Indexes) | [database/schema.sql](../database/schema.sql) |
| **3.2** | Hợp đồng SQL nạp dữ liệu (Zone Lookup, Baseline, Replay Counts) | [database/load/zone_lookup.sql](../database/load/zone_lookup.sql) · [database/load/baseline.sql](../database/load/baseline.sql) · [database/load/replay_counts.sql](../database/load/replay_counts.sql) |
| **3.2** | Module Python & CLI nạp kho dữ liệu | [warehouse/loader.py](../src/nyc_taxi_pipeline/warehouse/loader.py) · [scripts/load_warehouse.py](../scripts/load_warehouse.py) |
| **3.3** | Câu truy vấn SQL cốt lõi (Core MVP) | [database/queries/compare_pickup_demand.sql](../database/queries/compare_pickup_demand.sql) |
| **3.3+** | Bộ truy vấn SQL mở rộng (Điều phối xe, Mật độ vùng/quận, Cửa ngõ, Chuỗi thời gian) | [taxi_dispatch_matrix.sql](../database/queries/taxi_dispatch_matrix.sql) · [zone_and_borough_density.sql](../database/queries/zone_and_borough_density.sql) · [airport_and_transit_hubs.sql](../database/queries/airport_and_transit_hubs.sql) · [trip_volume_summary.sql](../database/queries/trip_volume_summary.sql) · [weekly_peak_hours.sql](../database/queries/weekly_peak_hours.sql) · [time_series_trend.sql](../database/queries/time_series_trend.sql) |
| **3.3** | Module Python & CLI thực thi truy vấn báo cáo | [warehouse/queries.py](../src/nyc_taxi_pipeline/warehouse/queries.py) · [scripts/query_warehouse.py](../scripts/query_warehouse.py) |
| **Kiểm thử** | Unit & Contract Tests cho Data Warehouse | [tests/test_warehouse.py](../tests/test_warehouse.py) |
| **Tài liệu** | Đặc tả Schema, Chiến lược điều phối & Danh mục truy vấn | [dac-ta-chi-tiet-kho-du-lieu.md](dac-ta-chi-tiet-kho-du-lieu.md) · [chien-luoc-dieu-phoi-va-kho-du-lieu.md](chien-luoc-dieu-phoi-va-kho-du-lieu.md) · [danh-muc-truy-van-dieu-hanh.md](danh-muc-truy-van-dieu-hanh.md) |

---

### 2. Hợp đồng bàn giao & Quy tắc xử lý

1. **Khởi tạo bảng (`database/schema.sql`):**
   - `taxi_zones`: khóa chính `(pickup_zone_id)`, cột `zone_name`, `borough`.
   - `historical_baseline`: khóa chính `(pickup_zone_id, iso_weekday, slot_15m)`, cột `baseline_trip_count` (`NUMERIC`), `reference_windows` (`INTEGER`), `history_start` (`TIMESTAMPTZ`), `history_end` (`TIMESTAMPTZ`).
   - `replay_pickup_counts`: khóa chính `(replay_id, pickup_zone_id, window_start)`, cột `window_end` (`TIMESTAMPTZ`), `trip_count` (`BIGINT`).
   - Không đặt Foreign Key cứng sang `taxi_zones` để giữ lại các mã vùng hợp lệ chưa có tên trong bảng tra cứu.

2. **Nạp dữ liệu (`warehouse/loader.py` & `scripts/load_warehouse.py`):**
   - **Từ Chính (`zone_lookup`):** Đọc trực tiếp file CSV hoặc tự động tìm `raw_path` của mục `dataset_role = "zone_lookup"`, `status = "ready"` trong `manifest.json`. Ánh xạ `LocationID -> pickup_zone_id`, `Zone -> zone_name`, `Borough -> borough` và thực hiện `ON CONFLICT (pickup_zone_id) DO UPDATE`.
   - **Từ Mạnh Batch (`historical_baseline`):** Đọc file CSV hoặc thư mục `part-*.csv` do `run_batch.py` xuất ra. Kiểm tra toàn bộ dòng thuộc cùng một kỳ `(history_start, history_end)` và không trùng khóa `(pickup_zone_id, iso_weekday, slot_15m)`, sau đó thay trọn bộ bảng trong một giao dịch (`TRUNCATE` + `INSERT`).
   - **Từ Mạnh Stream (`replay_pickup_counts`):** Cung cấp hàm `upsert_replay_counts` và class `PostgresReplayUpsertSink` tương thích trực tiếp với `foreachBatch` của `run_stream.py` (`ON CONFLICT (replay_id, pickup_zone_id, window_start) DO UPDATE SET window_end = EXCLUDED.window_end, trip_count = EXCLUDED.trip_count`).

3. **Truy vấn bằng Python (`warehouse/queries.py` & `scripts/query_warehouse.py`):**
   - Chuyển đổi `window_start` sang múi giờ `America/New_York` ngay trong PostgreSQL để tính `iso_weekday` (1..7) và `slot_15m` (0..95).
   - Giữ nguyên các dòng quan sát nếu thiếu tên vùng (fallback sang `pickup_zone_id::text`) hoặc thiếu baseline (`change_pct = NULL`, `change_unavailable_reason = 'missing_baseline'` hoặc `'zero_baseline'`).
   - Sắp xếp mặc định: `change_pct DESC NULLS LAST`.

---

### 3. Hướng dẫn lệnh chạy (Python CLI)

#### A. Khởi tạo bảng và nạp dữ liệu vào PostgreSQL
```bash
# Đặt chuỗi kết nối PostgreSQL qua biến môi trường (hoặc truyền trực tiếp --dsn)
export POSTGRES_DSN="postgresql://postgres:postgres@localhost:5432/nyc_taxi"

# Khởi tạo 3 bảng và nạp toàn bộ dữ liệu từ CSV / thư mục Batch
python scripts/load_warehouse.py \
    --init-schema \
    --zone-lookup data/raw/taxi_zone_lookup.csv \
    --baseline data/processed/baseline \
    --replay-counts data/processed/replay_counts.csv
```

#### B. Thực thi truy vấn bằng Python (`scripts/query_warehouse.py`)
```bash
# 1. Truy vấn bắt buộc Bước 3.3 (So sánh nhu cầu đón khách theo cửa sổ 15 phút)
python scripts/query_warehouse.py \
    --query compare \
    --replay-id taxi-fixture-001 \
    --window-start 2025-01-20T08:00:00-05:00

# 2. Truy vấn Ma trận Điều phối Taxi (Điểm nóng thiếu xe & Khu vực còn điều phối được xe)
python scripts/query_warehouse.py \
    --query dispatch \
    --replay-id taxi-fixture-001 \
    --window-start 2025-01-20T08:00:00-05:00

# 3. Truy vấn Mật độ taxi theo Vùng (%) hoặc gộp theo Quận (Borough Share %)
python scripts/query_warehouse.py --query density --replay-id taxi-fixture-001 --window-start 2025-01-20T08:00:00-05:00
python scripts/query_warehouse.py --query borough --replay-id taxi-fixture-001 --window-start 2025-01-20T08:00:00-05:00

# 4. Chạy TẤT CẢ 8 bảng báo cáo cùng lúc và xuất ra file Markdown hoặc JSON
python scripts/query_warehouse.py \
    --query all \
    --replay-id taxi-fixture-001 \
    --window-start 2025-01-20T08:00:00-05:00 \
    --output docs/warehouse-query-results.md

# 5. Tự động tạo bảng + nạp dữ liệu mẫu Bước 4.1 + chạy toàn bộ truy vấn kiểm tra nhanh
python scripts/query_warehouse.py --seed-fixture --query all
```

#### C. Chạy kiểm thử tự động (Unit & Contract Tests)
```bash
python -m unittest tests/test_warehouse.py -v
```
