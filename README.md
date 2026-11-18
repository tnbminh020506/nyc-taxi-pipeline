# NYC Taxi Pipeline

Workspace chia việc cho hệ thống nhu cầu đặt xe và ghép xe NYC: **ba batch** (hai baseline lịch sử, gradient trong ngày, trung bình lượt đón trong ngày) và **ba stream** (cảnh báo baseline, lưu lượng/spike, ghép/điều phối). Đơn vị nhu cầu là yêu cầu đặt xe; ghép hai yêu cầu vẫn tính hai yêu cầu.

Bắt đầu từ [bản đồ giao việc](docs/guide/README.md), [sơ đồ tổng thể](docs/guide/giao-viec-demo-taxi.md) và [hợp đồng chung](docs/guide/contracts.md). Hướng dẫn chi tiết nằm trong Markdown; các file Python, SQL, YAML hiện là **khung giao việc, chưa có chương trình chạy hoàn chỉnh**.

## Phân công và khu vực code

| Nhánh | Phụ trách | Guide | Code |
| --- | --- | --- | --- |
| Ingestion | Chính | [6 công việc](docs/guide/README.md#danh-mục) trong docs/guide/ingestion | [ingestion](src/nyc_taxi_pipeline/ingestion) |
| Pipeline | Mạnh | [9 công việc](docs/guide/README.md#danh-mục) trong docs/guide/pipeline | [pipeline](src/nyc_taxi_pipeline/pipeline) |
| Warehouse | Bảo | [7 công việc](docs/guide/README.md#danh-mục) trong docs/guide/warehouse | [warehouse](src/nyc_taxi_pipeline/warehouse) |

API/client có module và hợp đồng riêng; người triển khai do nhóm phân công. Shared chứa schema, quy tắc thời gian và đồng hồ dùng chung. SQL thuộc warehouse/sql; scripts chỉ làm điểm chạy gọi module.

## Bản đồ công việc theo bước

| Bước README | Hướng dẫn và bàn giao |
| --- | --- |
| 1.1 Lấy dữ liệu | [API và nguồn vùng](docs/guide/ingestion/01-api-source.md): JSON nguồn, lookup và ranh giới |
| 1.2 Lưu HDFS | [HDFS/manifest](docs/guide/ingestion/02-hdfs-manifest.md): raw, Parquet và manifest ready |
| 1.3 Tạo sự kiện | [Replay](docs/guide/ingestion/03-request-replay.md): yêu cầu kèm điểm đến và tọa độ ổn định |
| 1.4 Kafka | [Publisher](docs/guide/ingestion/04-kafka-publisher.md): taxi-events và ack |
| Sơ đồ: trả khách và archive | [Simulator](docs/guide/ingestion/05-dropoff-simulator.md), [HDFS archive](docs/guide/ingestion/06-event-archive.md) |
| 2.1 Quy tắc chung | [Chuẩn hóa/router](docs/guide/pipeline/01-normalization-routing.md): kiểm tra, khử trùng, phân luồng |
| 2.2 Batch | [Hai baseline](docs/guide/pipeline/02-historical-baselines.md), [gradient](docs/guide/pipeline/03-demand-gradients.md), [trung bình đón trong ngày](docs/guide/pipeline/07-daily-pickups.md), [tổng hợp cuối](docs/guide/pipeline/09-batch-summary.md) |
| 2.3 Stream | [Cảnh báo](docs/guide/pipeline/04-demand-alerts.md), [lưu lượng/spike](docs/guide/pipeline/08-traffic-spikes.md), [ghép/vòng đời](docs/guide/pipeline/05-ride-matching.md), [hợp đồng API/client](docs/guide/pipeline/06-service-api.md) |
| 3.1 Tạo bảng | [Schema PostgreSQL/PostGIS](docs/guide/warehouse/01-schema.md) |
| 3.2 Nạp và ghi dữ liệu | [Lookup](docs/guide/warehouse/02-zone-lookup.md), [công bố batch](docs/guide/warehouse/03-batch-publication.md), [số đếm](docs/guide/warehouse/04-demand-storage.md), [giao dịch ghép](docs/guide/warehouse/05-matching-transactions.md), [lịch/retention](docs/guide/warehouse/06-schedule-retention.md) |
| 3.3 Đối soát | [SQL đối soát](docs/guide/warehouse/07-reconciliation.md) |
| 4.1 Bộ mẫu — cả nhóm tự làm | [Fixture](data/fixtures/README.md), [khung test](tests/test_pipeline_contract.py) |
| 4.2 Chạy và bàn giao — cả nhóm tự làm | [Báo cáo](docs/mvp-report.md), [cấu hình](configs/demo.example.yaml) |

## Nguyên tắc tích hợp

Chính công bố history/replay trên HDFS; Bảo chuẩn bị schema và repository; Mạnh đọc history, tính và công bố batch ban đầu. Stream giữ cặp baseline lúc mở cửa sổ, cảnh báo khi vừa vượt ngưỡng và chờ Kafka ack trước ghi tổng tương ứng. Chốt cửa sổ chỉ tạo đầu vào cho batch định kỳ, không tự đổi baseline bằng SQL. Gradient cung cấp ngữ cảnh, không là điều kiện cảnh báo riêng.

Nhánh ghép dùng hai bán kính 500 m, kiểm tra đồng ý đã có từ phía người dùng và chốt qua giao dịch kho. Publisher phát một pickup mỗi trip sau commit; simulator phát trả khách theo từng request từ lịch bền vững; archive lưu raw/journal lên HDFS trước khi kho được phép dọn chi tiết. Thời gian trao đổi có múi giờ, nghiệp vụ theo America/New_York.

Mục 4.1–4.2 và các file fixture/test/báo cáo của cả nhóm giữ nguyên trong lần tổ chức này; chúng còn mô tả thiết kế cũ và cần nhóm cập nhật trước nghiệm thu. Không dùng chúng làm hợp đồng mới thay cho guide.

## Các nhịp xử lý và đối soát

- Ba batch chạy mỗi 15 phút; sau cả ba mới tổng hợp/công bố batch view. Batch 3 lấy trung bình các khung có lượt đón trong ngày hiện tại, không lấy cùng slot qua nhiều ngày.
- Stream kiểm tra spike mỗi 15 giây: số request mới > k lần khoảng trước và > M. Cùng phiên–zone–phút thì upsert tổng vào cùng bản ghi, không cộng lại khi retry.
- Serving view giữ 60 phút gần nhất; PostgreSQL giữ toàn bộ bản ghi phút. Bucket phút chứa số phát sinh, không phải tổng rolling.
- Batch đọc HDFS sự kiện đặt/đón/trả thực tế, stream tính ngay trên cùng sự kiện. Hai view 15 phút giữ riêng, đối soát sau khi đủ dữ liệu và đã chốt. Ví dụ 5 request, một cặp ghép, ba solo, tất cả trả trong khung: 5 đặt — 4 trip đón — 5 khách trả — 4 trip hoàn tất.

Xem [hợp đồng](docs/guide/contracts.md) để thống nhất khóa, zone, event time, độ phủ và phiên bản công bố.
