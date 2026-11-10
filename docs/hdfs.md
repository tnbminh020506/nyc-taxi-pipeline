# HDFS dùng chung: lưu trước, replay sau

Đã hợp nhất yêu cầu của Minh từ `origin/main` (`0d1b775`) vào `demo/ingestion`. Nguồn chính vẫn là TLC Parquet/DuckDB, tỉ lệ theo số dòng hợp lệ vẫn khoảng 60/40. HDFS là nơi bàn giao chính thức; Kafka tiếp tục làm tầng sự kiện **sau khi dữ liệu đã lưu và công bố trên HDFS**.

```text
Parquet tháng TLC đã cache
    → HDFS raw/trips (file nguyên bản, checksum, replication=2)
    → DuckDB xử lý bản cache có cùng bytes đã kiểm chứng
    → HDFS: history + replay + CSV lookup + provenance
    → manifest HDFS bất biến, công bố sau cùng
          ├─ history URI → Spark Batch (Mạnh)
          ├─ lookup URI → nạp PostgreSQL (Bảo)
          └─ replay URI → Python đọc/cache có checksum → Kafka → Spark Stream
```

Không phải thay Kafka bằng HDFS: HDFS lưu dữ liệu bền vững và dùng chung; Kafka nhận các sự kiện phát lại từ tập đã lưu.

## Khởi động cụm demo

Cần Docker Engine/Compose với Linux containers. Windows hiện tại chưa có Docker/WSL; không tự cài hay đổi cấu hình hệ thống. Có thể chạy Compose trên máy Linux của nhóm; CI cũng khởi động cụm thật để kiểm tra.

```powershell
docker compose -p taxi-hdfs -f docker-compose.hdfs.yml up -d --wait --wait-timeout 240
docker compose -p taxi-kafka-hdfs -f docker-compose.ingestion.yml up -d --wait --wait-timeout 180
```

Một NameNode và **hai DataNode**, replication 2, mỗi node có named volume riêng. Image Apache Hadoop 3.4.2. NameNode UI/WebHDFS tại `http://127.0.0.1:9870`; RPC trong mạng Docker là `hdfs://namenode:8020`. Python dùng WebHDFS, không cần Java/libhdfs trên Windows; DataNode 9864/9865 có mapping host trong config.

Các container đều nằm trên **một máy Docker**: kiểm tra được phân phối block/bản sao giữa DataNode và việc một DataNode ngừng, nhưng không chứng minh chịu được mất cả máy host. Đây là cấu hình lab đơn giản, không phải cụm production có HA/Kerberos. Endpoint chỉ bind loopback trên host. Khi triển khai nhiều máy thật, nhóm phải cấu hình địa chỉ NameNode/DataNode truy cập được, xác thực và mạng phù hợp.

## Chuẩn bị và nhập dữ liệu

Giữ file TLC nguyên bản đã tải trong `output/tlc-source-cache`, gồm `yellow_tripdata_2025-01.parquet` và `taxi_zone_lookup.csv`. Không tải lại để chạy demo nhiều lần. `configs/hdfs.example.yaml` đọc cache; thiếu file thì thất bại rõ ràng, không tự gọi TLC.

```powershell
.\.venv\Scripts\python.exe scripts/run_ingestion.py --config configs/hdfs.example.yaml
```

Bộ demo: history 06/01/2025 08:00–10:00, replay 20/01/2025 08:00–10:00 giờ New York. Đã đo từ cache: 8.461 và 5.651 dòng hợp lệ (~59,96%/40,04%). Mã phiên HDFS riêng `tlc-hdfs-3-2-001`.

`run_ingestion.py` ở chế độ HDFS thực hiện:

1. Kiểm tra Parquet nguồn trong cache, upload **nguyên bản, đủ cột** lên `raw/trips/<sha256>/<filename>`; kiểm tra độ dài, đọc lại SHA-256 và các block có ít nhất hai vị trí DataNode. Nguồn cùng checksum được tái sử dụng sau kiểm tra, không ghi đè.
2. DuckDB xử lý bản cache có cùng nội dung đã lưu HDFS; bộ nhớ engine vẫn 512MB. Ghi output staging local để upload, không nhận staging là bàn giao HDFS.
3. Upload history/replay, lookup và provenance vào thư mục publication UUID riêng. Từng file ghi vào tên tạm, đọc lại kiểm tra SHA-256/số bản sao, rồi RENAME sang tên cuối.
4. Chỉ sau khi mọi file đạt mới upload/RENAME manifest v1 vào URI mới `/nyc-taxi/manifests/<publication_id>.json`. Đây là file **bất biến**, không có bước xóa manifest cũ rồi thay bằng file mới.
5. Ghi receipt local `data/hdfs-publication.local.json` trạng thái ready cùng URI/hash manifest. Lỗi ở bất kỳ bước nào ghi failed; replay dùng receipt sẽ dừng. File staging/remote chưa công bố có thể còn để chẩn đoán, không tự xóa dữ liệu cũ.

`raw_path` của trip trỏ đến thư mục provenance HDFS; `original_sources`/`hdfs_originals` ghi URI, hash, bytes của Parquet tháng gốc thực sự được lưu. `parquet_path` và lookup `raw_path` là URI HDFS. Manifest vẫn version 1, giữ các cột/schema và khóa vai trò–nguồn–phạm vi đã thống nhất. `replication` là yêu cầu; publisher kiểm tra số vị trí block thực tế trước khi công bố, không chỉ đọc giá trị cấu hình.

## Đọc lại và bàn giao

```powershell
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config configs/hdfs.example.yaml --dry-run --output output/hdfs-events.jsonl
# Khi Spark stream đã sẵn sàng:
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config configs/hdfs.example.yaml --create-topic
```

Python đọc manifest **từ HDFS** theo URI trong receipt, kiểm tra checksum; không đọc lại local manifest của producer. Parquet replay được tải theo luồng vào cache local, kiểm tra SHA-256 rồi đọc bằng PyArrow/SQLite theo batch. Cache chỉ tối ưu đọc lại; HDFS vẫn là nguồn bàn giao. Payload `source_file` luôn là URI HDFS, không lộ đường dẫn file tạm trên máy phát. `event_id` vẫn dựa vào nguồn/phạm vi/checksum/vị trí dòng, giữ ổn định khi phát lại.

Để Mạnh/Bảo hoặc máy khác dùng mà không cần receipt của Chính, đưa URI manifest publisher đã in ra vào một bản config:

```yaml
storage:
  kind: hdfs
  fs_uri: hdfs://namenode:8020
  webhdfs_url: http://127.0.0.1:9870
  manifest_uri: hdfs://namenode:8020/nyc-taxi/manifests/REPLACE_WITH_PUBLICATION_ID.json
  replication: 2
  # Giữ data_node_map theo configs/hdfs.example.yaml nếu đọc từ host.
```

Consumer pin một phiên bản manifest; lượt publish mới sinh URI mới và không làm hỏng phiên đang chạy. Spark dùng HDFS client đọc trực tiếp `parquet_path`; nó phải ở mạng resolve/truy cập được cả NameNode và DataNode. Bảo dùng CSV HDFS; nếu nạp bằng `psql \copy`, tải CSV về file tạm trên máy chạy psql trước.

## Kiểm thử

Unit cục bộ kiểm tra manifest URI, giữ nguồn, chặn fallback local sau lỗi upload, checksum manifest bị sửa và chặn ready khi thiếu bản sao. Đây là kiểm thử mô phỏng giao thức, **không phải bằng chứng cụm HDFS đã chạy**.

CI `hdfs-roundtrip` dựng Hadoop thật với hai DataNode và Kafka thật, rồi chạy:

```powershell
.\.venv\Scripts\python.exe scripts/test_hdfs_pipeline.py --rows 1000000 --fault-test --report output/hdfs-acceptance/report.json
```

Input stress **tổng hợp 1 triệu dòng**, không gọi TLC: kiểm tra raw nguyên bản, toàn bộ output/checksum, số dòng 60/40, nhiều block và hai replica. Sau đó một snapshot nhỏ kiểm tra HDFS → replay → Kafka, gửi lại giữ ID; xóa staging/receipt/cache của producer để chứng minh đọc từ HDFS; dừng một DataNode để kiểm tra đọc qua bản sao còn lại; từ chối publish mới khi chưa đủ hai replica, rồi khởi động lại và kiểm tra phục hồi. Script fault-test chỉ quản lý service `datanode-2` của Compose project `taxi-hdfs` do hướng dẫn này tạo.

Kết quả thực tế sẽ được ghi trong [báo cáo nghiệm thu HDFS](hdfs-validation.md). Bằng chứng DuckDB/TLC 48,7 triệu dòng trước đây không được coi là bằng chứng đã lưu lượng đó trên HDFS.

Tham khảo: [Apache WebHDFS 3.4.2](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-hdfs/WebHDFS.html), [thiết lập cụm Hadoop](https://hadoop.apache.org/docs/r3.4.2/hadoop-project-dist/hadoop-common/ClusterSetup.html).
