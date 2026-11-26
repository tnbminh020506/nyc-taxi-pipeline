# Bản đồ giao việc

Đọc [sơ đồ tổng thể](giao-viec-demo-taxi.md), [hợp đồng chung](contracts.md), rồi chọn công việc bên dưới. Xem [bàn giao Ingestion v2](../ingestion-v2.md) để chạy và đối chiếu phần ingestion. MVP toàn tuyến cần code tích hợp từ pipeline và warehouse.

Ba thư mục tương ứng ba nhánh: `ingestion` — Chính, `pipeline` — Mạnh, `warehouse` — Bảo. Các mục 4.1 và 4.2 dành cho cả nhóm, không tạo file giao việc riêng và không sửa bộ mẫu, test hay báo cáo của hai mục đó. API/client quản trị có khu vực riêng; nhận đồng ý đã có từ phía người dùng, người triển khai do nhóm phân công.

## Danh mục

| Công việc từ README/sơ đồ | Phụ trách | Hướng dẫn |
| --- | --- | --- |
| 1.1 · Thu thập NYC và nguồn vùng | Chính | [ingestion/01-api-source](ingestion/01-api-source.md) |
| 1.2 · HDFS, Parquet và manifest | Chính | [ingestion/02-hdfs-manifest](ingestion/02-hdfs-manifest.md) |
| 1.3 · Replay yêu cầu và tọa độ mô phỏng | Chính | [ingestion/03-request-replay](ingestion/03-request-replay.md) |
| 1.4 · Kafka và phát pickup sau điều phối | Chính | [ingestion/04-kafka-publisher](ingestion/04-kafka-publisher.md) |
| Sơ đồ · Simulator trả khách sau pickup | Chính | [ingestion/05-dropoff-simulator](ingestion/05-dropoff-simulator.md) |
| Sơ đồ · Archive sự kiện và snapshot cho batch | Chính | [ingestion/06-event-archive](ingestion/06-event-archive.md) |
| 2.1 · Chuẩn hóa và router ba loại sự kiện | Mạnh | [pipeline/01-normalization-routing](pipeline/01-normalization-routing.md) |
| 2.2 · Batch 1: hai baseline lịch sử | Mạnh | [pipeline/02-historical-baselines](pipeline/02-historical-baselines.md) |
| 2.2 · Batch 2: gradient trong ngày | Mạnh | [pipeline/03-demand-gradients](pipeline/03-demand-gradients.md) |
| 2.3 · Stream 1: cảnh báo baseline | Mạnh | [pipeline/04-demand-alerts](pipeline/04-demand-alerts.md) |
| 2.3 · Stream 3: ghép và điều phối | Mạnh | [pipeline/05-ride-matching](pipeline/05-ride-matching.md) |
| Sơ đồ · API quản trị và nhận quyết định người dùng | Mạnh: hợp đồng; nhóm phân công API | [pipeline/06-service-api](pipeline/06-service-api.md) |
| 2.2 · Batch 3: trung bình lượt đón trong ngày | Mạnh | [pipeline/07-daily-pickups](pipeline/07-daily-pickups.md) |
| 2.3 · Stream 2: lưu lượng và spike | Mạnh | [pipeline/08-traffic-spikes](pipeline/08-traffic-spikes.md) |
| 2.2 · Tổng hợp cuối sau ba batch | Mạnh | [pipeline/09-batch-summary](pipeline/09-batch-summary.md) |
| 3.1 · Schema phân tích và vận hành | Bảo | [warehouse/01-schema](warehouse/01-schema.md) |
| 3.2 · Nạp bảng tra và dữ liệu vùng | Bảo | [warehouse/02-zone-lookup](warehouse/02-zone-lookup.md) |
| 3.2 · Công bố sau ba batch | Bảo | [warehouse/03-batch-publication](warehouse/03-batch-publication.md) |
| 3.2 · Stream counts, phút và serving view | Bảo | [warehouse/04-demand-storage](warehouse/04-demand-storage.md) |
| 3.2 · Giao dịch điều phối và pickup outbox | Bảo | [warehouse/05-matching-transactions](warehouse/05-matching-transactions.md) |
| Sơ đồ · Outbox, hoàn tất và retention vận hành | Bảo | [warehouse/06-schedule-retention](warehouse/06-schedule-retention.md) |
| 3.3 · Đối soát batch–stream và dashboard | Bảo | [warehouse/07-reconciliation](warehouse/07-reconciliation.md) |

## Ranh giới module

- `src/nyc_taxi_pipeline/ingestion/`: API nguồn, HDFS, manifest, replay, producer, simulator và archive.
- `src/nyc_taxi_pipeline/pipeline/`: chuẩn hóa, ba batch và tổng hợp cuối, router, ba nhánh stream, service API/client.
- `src/nyc_taxi_pipeline/warehouse/`: kết nối, repository, DDL/SQL, lịch phát và retention; SQL nằm trong `warehouse/sql/`.
- `src/nyc_taxi_pipeline/shared/`: schema sự kiện, quy tắc thời gian và đồng hồ chung. Mạnh chủ trì events/windows; Chính chủ trì clock; đổi hợp đồng phải trao đổi cả ba người.
- `scripts/`: đọc cấu hình và gọi module. Xem các CLI của Chính trong [bàn giao Ingestion v2](../ingestion-v2.md).

`shared` không import ba nhánh. Ingestion và pipeline dùng các giao diện warehouse, không tự viết SQL rải rác. Warehouse không import job Spark hoặc producer; nhận DTO/tham số thuần. Các repository có thể chạy trong tiến trình riêng khi triển khai, không bắt buộc mọi thứ chung tiến trình.

## Vị trí cũ → mới

| Khung cũ | Vị trí đích |
| --- | --- |
| ingestion/ingest.py | ingestion/source_api.py, zone_sources.py, hdfs_store.py |
| processing/pickup.py | pipeline/normalization.py và shared/windows.py |
| pipelines/historical_baseline.py | pipeline/batch/historical_baseline.py |
| pipelines/streaming_counts.py | pipeline/stream/router.py và demand_alerts.py |
| database/schema.sql, load/, queries/ | src/nyc_taxi_pipeline/warehouse/sql/ |
| database/load/replay_counts.sql | warehouse/sql/load/demand_windows.sql |

Các file cũ chỉ có chú thích và đã được thay bằng khung theo thiết kế mới; không duy trì hai nguồn SQL. Hướng dẫn nghiệp vụ nằm trong Markdown, mỗi file code chỉ giữ TODO và đường dẫn guide.

## Phối hợp bàn giao

Chính bàn giao manifest/schema/ID/clock; Bảo bàn giao DDL và chữ ký repository; Mạnh bàn giao chuẩn hóa, kết quả batch, payload alert và thao tác ghép. Mỗi người có thể làm trên dữ liệu giả theo hợp đồng mà chưa cần toàn tuyến. Khi thay trường/khóa, cập nhật contracts.md và guide của bên nhận cùng thay đổi.

Cấu hình chung gồm nguồn và kỳ dữ liệu, HDFS, Kafka, PostgreSQL/PostGIS, múi giờ, watermark, trigger, checkpoint, K, phạm vi baseline, bán kính, thời gian chờ/gia hạn, đồng hồ và retention. Xem mục 4.3 của thiết kế tổng thể. Thời lượng gia hạn và thời hạn lưu phải được điền trước demo; không tự mặc định.

## Thay đổi ngày 18/11/2026

Có 22 hướng dẫn: 6 ingestion, 9 pipeline, 7 warehouse. Bổ sung Batch 3, Stream 2 lưu lượng/spike, bước tổng hợp cuối sau ba batch và pickup publisher. Ghép chuyển thành Stream 3; giữ tên file 05-ride-matching.md để không phá liên kết cũ.

Bucket 15 giây dùng cảnh báo, bản ghi phút dùng upsert tổng phát sinh, serving giữ 60 phút; PostgreSQL giữ toàn bộ lịch sử phút. Batch/stream view 15 phút tách riêng để đối soát. Cấu hình k/M bắt buộc trước chạy. Trạng thái hiện tại vẫn là đặc tả và khung, chưa tích hợp code các nhánh thành viên.
