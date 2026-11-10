<!--
Đầu vào
Đường dẫn tệp, lệnh chạy, log và kết quả kiểm tra thực tế của từng nhánh.
Đầu ra
Báo cáo bàn giao theo thành viên và trạng thái tích hợp của cả nhóm.
-->
# Bước 4.2 · Báo cáo MVP

| Phần | Phụ trách | Trạng thái |
| --- | --- | --- |
| Bước 1.1–1.4 · Ingestion và replay | Chính | Đã có đặc tả |
| Bước 2.1–2.3 · Spark Batch và Structured Streaming | Mạnh | Đã triển khai trên nhánh `demo/pipeline`. Đạt bộ mẫu và đã chạy toàn tuyến từ HDFS của Chính |
| Bước 3.1–3.3 · PostgreSQL và SQL demo | Bảo | Đã có đặc tả |
| Bước 4.1–4.2 · Chốt dữ liệu và ghép demo | Nhóm trưởng | Đã có kịch bản nghiệm thu |

Mỗi thành viên ghi phần đã làm kèm đường dẫn tệp, luồng và hợp đồng bàn giao, lệnh chạy và kết quả thực tế, phần còn vướng và việc cần phối hợp.

Nhóm trưởng ghi dataset và bộ lọc API, số trang và số dòng, Zone Lookup, kỳ lịch sử, kỳ replay, `replay_id` và kết quả SQL của cửa sổ demo. Ghi riêng kết quả trên bộ mẫu và kết quả lấy dữ liệu từ API thật.

## Mạnh · Bước 2.1–2.3 Data Processing

### Tệp

| Bước | Tệp |
| --- | --- |
| 2.1 | [processing/pickup.py](../src/nyc_taxi_pipeline/processing/pickup.py) · [processing/spark_session.py](../src/nyc_taxi_pipeline/processing/spark_session.py) |
| 2.2 | [pipelines/historical_baseline.py](../src/nyc_taxi_pipeline/pipelines/historical_baseline.py) · [scripts/run_batch.py](../scripts/run_batch.py) |
| 2.3 | [pipelines/streaming_counts.py](../src/nyc_taxi_pipeline/pipelines/streaming_counts.py) · [scripts/run_stream.py](../scripts/run_stream.py) |
| Môi trường và kiểm tra | [docker/spark/Dockerfile](../docker/spark/Dockerfile) · [tests/test_processing.py](../tests/test_processing.py) |

### Hợp đồng bàn giao

**Quy tắc chung (2.1)**

- Phiên Spark dùng `America/New_York`.
- Chuỗi thời gian không offset (floating timestamp của SODA) được hiểu là giờ New York.
- Cửa sổ là `[start, end)`.
- `slot_15m = giờ × 4 + phút / 15` (lấy phần nguyên).
- `iso_weekday`: 1 là thứ Hai, 7 là Chủ nhật.
- Mã vùng phải là số nguyên dương.
- Lý do loại: `source_validation_error` (dòng ingestion đã đánh dấu trong `validation_error`), `missing_pickup_at`, `unparseable_pickup_at`, `missing_pickup_zone_id`, `invalid_pickup_zone_id`. Stream có thêm `malformed_event`, `missing_replay_id`, `missing_event_id`.

**Nhận từ Chính**

Đã đối chiếu với nhánh `demo/ingestion` bằng cách chạy chính code ingestion và replay của Chính trên `configs/fixture.yaml`.

- Manifest `{"version": 1, "datasets": [...]}`.
  - Chỉ đọc mục `history` có `status = ready`.
  - Từ chối mục có `coverage_complete = false`.
  - Cửa sổ đã xác nhận độ phủ lấy theo thứ tự: `--coverage`, rồi `confirmed_windows` của mục manifest, cuối cùng là toàn bộ `[range_start, range_end)`.
- Parquet dùng cột chuẩn `pickup_at` (timestamp UTC) và `pickup_zone_id` (int32). Dòng có `validation_error` khác rỗng bị loại.
- Sự kiện Kafka là JSON gồm `replay_id`, `event_id` (SHA-256), `pickup_at` (ISO 8601 UTC `+00:00`), `pickup_zone_id` (số nguyên), `source_file`.

**Giao cho Bảo, batch**

- Thư mục CSV có header, một file `part-*.csv`.
- Các cột: `pickup_zone_id, iso_weekday, slot_15m, baseline_trip_count, reference_windows, history_start, history_end`.
- Mốc thời gian dạng `2025-03-17T00:00:00-04:00`.
- Mỗi lần chạy ghi đè thư mục đầu ra.

**Giao cho Bảo, stream**

- Mỗi micro-batch gọi sink với các dòng `replay_id, pickup_zone_id, window_start, window_end, trip_count` (tổng lũy kế).
- Sink `postgres` tạm thời thực hiện `INSERT … ON CONFLICT (replay_id, pickup_zone_id, window_start) DO UPDATE SET trip_count = EXCLUDED.trip_count` trong một giao dịch. Bảo cần xác nhận cách ghi này hoặc thay bằng cơ chế ghi của mình.

### Lệnh chạy

```bash
docker build -t nyc-taxi-spark:dev docker/spark
docker run --rm -v "$PWD":/workspace nyc-taxi-spark:dev python3 -m pytest -p no:cacheprovider tests/test_processing.py

python3 scripts/run_batch.py --manifest <URI manifest> --output <URI thư mục kết quả> \
    [--coverage START/END ...] [--zones 161,230]
POSTGRES_DSN=postgresql://... python3 scripts/run_stream.py --replay-id demo-01 \
    --kafka-bootstrap <host:port> --topic taxi-events --checkpoint-root <URI> --sink postgres
```

### Kết quả trên bộ mẫu

Chạy ngày 2026-11-10. Vùng A = 161. Thứ Hai 2025-03-17 và 2025-03-24 là kỳ history, thứ Hai 2025-03-31 là kỳ replay.

- **Chạy từ HDFS của Chính**, ngày 2026-11-10. Môi trường:
  - Cụm `docker-compose.hdfs.yml` của nhánh `demo/ingestion`: 1 NameNode, 2 DataNode, replication 2.
  - Ingestion `configs/hdfs.example.yaml` với TLC 2025-01. Trong container chỉ đổi `webhdfs_url`/`data_node_map` sang tên host trong mạng Docker.
  - Kafka cấu hình như `docker-compose.ingestion.yml` (`kafka:29092`), PostgreSQL dùng `schema.sql` của nhánh `demo/warehouse`.
  - Kết quả theo từng bước:
    - `run_batch.py --manifest hdfs://namenode:8020/nyc-taxi/manifests/<id>.json --conf spark.hadoop.dfs.replication=2`: đọc 8.461 chuyến history (06/01 08:00–10:00), 179 vùng × 8 cửa sổ = 1.432 dòng. Kết quả ghi CSV lên `hdfs://…/nyc-taxi/processing/baseline/<id>`, `fsck` HEALTHY, replication 2.0.
    - Bật `run_stream.py` trước, checkpoint đặt trên HDFS, sink `postgres`. Sau đó Chính chạy `replay_taxi_stream.py` hai lần (2 × 5.651 sự kiện, lần hai toàn là gửi trùng). Bảng `replay_pickup_counts` có 751 ô, tổng 5.651 chuyến, **khớp từng ô** với số đếm tính trực tiếp từ Parquet replay trên HDFS.
    - Lần gửi lại bị loại hết: 3.150 sự kiện bị khử trùng, 2.501 sự kiện bị watermark loại vì đến trễ hơn 1 giờ so với mốc lớn nhất (`pickup_at` 08:00–09:00).
    - Nạp baseline từ HDFS và chạy câu so sánh cho khung 08:00–08:15 ngày 20/01/2025: Times Sq/Theatre District có 20 chuyến so với mức lịch sử 17, tức +17,6%.
- Chạy trên đầu ra thật của ingestion Chính (bộ mẫu tháng 1/2025, vùng 161, `taxi-fixture-001`):
  - Batch dùng 2 cửa sổ trong `confirmed_windows` và ra đúng 1 dòng `161,1,32,2.0,2`.
  - Stream đọc 4 sự kiện dry-run của `replay.py` và ra 3 chuyến (08:00–08:15) và 1 chuyến (08:15–08:30) giờ New York.
- `tests/test_processing.py`: 13/13 ca đạt. Ngoài ca đọc đúng định dạng manifest và Parquet của `demo/ingestion`, các ca gồm:
  - quy tắc 08:15, offset UTC, Chủ nhật slot 95, các lý do loại;
  - baseline 2 / 2;
  - cửa sổ 0 chuyến cho 0.5 / 2;
  - độ phủ một phần;
  - xuất CSV;
  - stream 3 và 1 dù có sự kiện trùng, JSON hỏng và sự kiện thuộc phiên khác;
  - khởi động lại cùng checkpoint.
- Chạy thử toàn tuyến với Kafka 3.8 và PostgreSQL 16 tạm:
  - `run_batch.py` ra `161,1,32,2.0,2`.
  - `run_stream.py` ghi `replay_pickup_counts` = 3 (08:00–08:15) và 1 (08:15–08:30), log `duplicates_dropped = 1`.
  - Chạy lại cùng checkpoint, gửi lại một sự kiện cũ và một sự kiện mới 08:10: tổng thành 4, sự kiện cũ bị loại.

### Còn vướng và cần phối hợp

- **Nhóm trưởng:**
  - Đưa image Spark vào `docker-compose.yml`, cùng mạng với HDFS, Kafka và PostgreSQL.
  - Chốt danh sách cửa sổ đã xác nhận độ phủ (`--coverage`).
- **Chính:**
  - Ở chế độ HDFS, Spark phải chạy cùng mạng Docker với NameNode và DataNode (`taxi-hdfs_default`). Truyền `--conf spark.hadoop.dfs.replication=2`, vì cụm chỉ có hai DataNode.
  - Lịch sử demo chỉ có một thứ Hai (06/01), nên `reference_windows = 1`. Muốn baseline ổn định hơn, nên lấy thêm nhiều thứ Hai.
  - Kafka của Chính: container cùng mạng dùng `kafka:29092`, máy host dùng `localhost:9092`.
  - Với API thật, `confirmed_windows` hiện chỉ được ghi ở chế độ fixture. Nếu thiếu thì batch dùng toàn bộ khoảng `ready`.
- **Bảo:**
  - Xác nhận DDL của `replay_pickup_counts` khớp câu upsert ở trên.
  - Xác nhận `baseline.sql` đọc được CSV này.
