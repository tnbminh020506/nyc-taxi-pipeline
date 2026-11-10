# Tóm tắt Công việc Nhánh Data Warehouse (Bước 3.1–3.3 · Bảo)

> **Người thực hiện:** Uông Gia Bảo  
> **Nhánh Git:** `demo/warehouse`  
> **Vị trí thư mục dự án:** `nyc-taxi-pipeline/` (Đường dẫn cục bộ: `c:\Users\Gia Bao\OneDrive\Máy tính\Tài liệu học tập\2026-1\Big_data\nyc-taxi-pipeline`)  
> **Phạm vi:** Cơ sở dữ liệu PostgreSQL, các script nạp dữ liệu và truy vấn phân tích bằng Python.

---

## 1. Bản đồ Thư mục & Tệp tin lưu trữ (File Locations)

| Thành phần | Vị trí thư mục / Tệp tin | Mục đích sử dụng |
| :--- | :--- | :--- |
| **Schema DDL** | `database/schema.sql` | Khởi tạo 3 bảng chính (`taxi_zones`, `historical_baseline`, `replay_pickup_counts`), constraints và index |
| **SQL Nạp dữ liệu** | `database/load/` | Chứa `zone_lookup.sql`, `baseline.sql`, `replay_counts.sql` |
| **SQL Truy vấn** | `database/queries/` | Chứa `compare_pickup_demand.sql` (Core MVP 3.3) và 6 file SQL mở rộng (điều phối xe, mật độ vùng/quận...) |
| **Source Code Python** | `src/nyc_taxi_pipeline/warehouse/` | Module Python gồm: `db.py` (kết nối), `loader.py` (nạp dữ liệu), `queries.py` (truy vấn & xuất bảng) |
| **Script Dòng lệnh** | `scripts/` | `scripts/load_warehouse.py` (nạp DB) và `scripts/query_warehouse.py` (chạy truy vấn) |
| **Kiểm thử tự động** | `tests/test_warehouse.py` | Unit test và test hợp đồng cho toàn bộ chức năng warehouse |
| **Tài liệu hướng dẫn** | `docs/` | `mvp-report.md`, `dac-ta-chi-tiet-kho-du-lieu.md`, `danh-muc-truy-van-dieu-hanh.md` |

---

## 2. Những việc đã hoàn thành

1. **Khởi tạo Schema (`database/schema.sql`):**
   - Đã tạo chuẩn 3 bảng: `taxi_zones` (danh mục vùng), `historical_baseline` (mức chuẩn lịch sử từ Spark Batch), `replay_pickup_counts` (số chuyến stream từ Kafka/Spark).
   - Thiết lập đầy đủ khóa chính (Composite PK), ràng buộc toàn vẹn `CHECK constraints` và `INDEX` tối ưu tốc độ JOIN.

2. **Xây dựng bộ SQL Load & Queries (`database/`):**
   - `database/load/`: Script nạp UPSERT cho `zone_lookup.sql`, `baseline.sql` (transaction an toàn) và `replay_counts.sql` (chống trùng lặp).
   - `database/queries/`: SQL cốt lõi `compare_pickup_demand.sql` (+50% theo đề bài) cùng các truy vấn mở rộng phục vụ điều hành (ma trận điều phối xe thiếu/dư, mật độ theo vùng/quận, sân bay, chuỗi thời gian).

3. **Hiện thực hóa bằng Python (`src/nyc_taxi_pipeline/warehouse/`):**
   - `db.py`: Quản lý kết nối PostgreSQL an toàn qua biến môi trường `POSTGRES_DSN`.
   - `loader.py`: Tự động nạp Zone Lookup từ manifest/CSV, nạp Spark Batch CSV và cung cấp sink UPSERT cho Spark Streaming.
   - `queries.py`: Các hàm Python gọi trực tiếp SQL, hỗ trợ xuất định dạng bảng Markdown hoặc JSON.

4. **Script dòng lệnh (CLI) phục vụ chạy demo (`scripts/`):**
   - `scripts/load_warehouse.py`: Lệnh nạp dữ liệu vào database.
   - `scripts/query_warehouse.py`: Lệnh chạy truy vấn phân tích, có cờ `--seed-fixture` để tự tạo bảng và test nhanh dữ liệu mẫu 4.1 trong 1 giây.

5. **Kiểm thử tự động (`tests/test_warehouse.py`):**
   - Đã viết unit test kiểm tra toàn bộ luồng nạp và truy vấn (đạt `4/4 test OK`).

---

## 3. Hướng dẫn chạy nhanh

Mở terminal tại thư mục gốc `nyc-taxi-pipeline/`:

```bash
# 1. Chạy unit test kiểm tra code
python -m unittest tests/test_warehouse.py -v

# 2. Test nhanh toàn bộ 8 truy vấn phân tích trên PostgreSQL (tự nạp dữ liệu mẫu)
python scripts/query_warehouse.py --seed-fixture --query all

# 3. Chạy câu truy vấn bắt buộc Bước 3.3 (Core MVP)
python scripts/query_warehouse.py --query compare --replay-id taxi-fixture-001 --window-start 2025-01-20T08:00:00-05:00
```
