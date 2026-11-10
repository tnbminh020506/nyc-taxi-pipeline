# Nghiệm thu HDFS — 10/11/2026

**Đạt trên cụm HDFS thật:** một NameNode, hai DataNode, replication 2 và Kafka thật chạy bằng Docker trên GitHub-hosted Ubuntu runner. Không phải mock server. [Workflow thành công](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42779664661) · [JSON gốc từ runner](evidence/hdfs-2026-11-10.json).

Đã merge `origin/main` commit `0d1b775` của Minh vào nhánh `demo/ingestion`, giải quyết xung đột tài liệu để giữ cả yêu cầu HDFS lẫn nguồn TLC/DuckDB và tỉ lệ 60/40 đã chốt. Code được nghiệm thu tại `c8d875a46e7a8c85e71edb51984a016a02843e29`.

## Dữ liệu và khối lượng

Input stress là **1.000.000 dòng tổng hợp**, gồm timestamp, mã vùng và cột payload bổ sung để kiểm tra giữ nguyên toàn bộ Parquet nguồn. Không gọi TLC trong lượt này (`tlc_requests: 0`). Không dùng số liệu này để khẳng định đã lưu 48,7 triệu dòng NYC thật trên HDFS; bằng chứng DuckDB/TLC trước đây là nghiệm thu riêng.

| Đối tượng lưu HDFS | Số dòng | Dung lượng logic | Block trong bài test |
| --- | ---: | ---: | ---: |
| Parquet nguồn nguyên bản | 1.000.000 | 21.174.441 bytes | 21 |
| history | 600.300 | 2.920.631 bytes | 3 |
| replay | 399.700 | 1.944.303 bytes | 2 |

Chia theo số dòng hợp lệ: **60,03%/39,97%**, không có dòng lỗi. Tất cả các block history/replay có ít nhất **hai vị trí lưu trên hai DataNode khác nhau**; raw nguồn cũng qua kiểm tra số replica thực tế. Block size 1 MiB được dùng riêng cho nghiệm thu để tạo nhiều block; config demo mặc định dùng 64 MiB. Dung lượng logic không nhân số bản sao.

Thời gian lượt ingestion/publish lớn là **6,630 giây** trên runner: bao gồm upload/checksum/replication của raw nguồn, DuckDB tạo output, upload/đọc lại kiểm tra và công bố manifest. Chưa tính sinh dữ liệu stress/chọn mốc chia hoặc ca Kafka và fault-test phía sau. Đây không phải benchmark tốc độ tải Internet hay SLA cho mọi máy/loại dữ liệu.

## Các hành vi đã xác minh

- Parquet nguồn được lưu **nguyên bản, đủ cột** trước khi DuckDB xử lý; SHA-256 đọc lại từ HDFS khớp file nguồn. History/replay được download từ HDFS, checksum và số dòng đều khớp đầu ra staging.
- File upload dùng tên tạm, kiểm tra đầy đủ rồi RENAME; manifest v1 có URI HDFS bất biến và được công bố cuối cùng. Publisher không ghi đè nội dung snapshot đã có.
- Snapshot nhỏ 6 history/4 replay được đọc từ HDFS, phát **4 sự kiện + gửi lại 1** qua Kafka, consumer nhận đủ **5 message với 4 event_id**. Cùng snapshot phát lại giữ ID và payload; `source_file` là URI HDFS.
- Xóa staging Parquet, local manifest/receipt và cache replay của producer trong thư mục test; consumer mới pin URI manifest vẫn đọc được đúng 4 sự kiện từ HDFS.
- Dừng **DataNode 2**, đợi NameNode xác nhận thiếu node, xóa cache replay rồi đọc lại: vẫn nhận đúng dữ liệu qua replica còn sống.
- Thử publish mới khi chỉ còn một DataNode: bị từ chối vì chưa đủ hai replica, receipt `failed`, không công bố manifest ready mới và không fallback sang local.
- Khởi động lại DataNode 2: kiểm tra hai replica phục hồi thành công.
- **57 unit/HTTP/contract test đạt trên cả Windows và Ubuntu**, thêm **2 ca Kafka thật** của các backend cũ vẫn đạt. Unit mới có ca manifest bị sửa, checksum/URI không hợp lệ và chặn cache-path traversal.

Log fault-test có lỗi `not replicated to 2 DataNodes` là **lỗi cố ý được kiểm tra**, không phải workflow thất bại. Kết quả tổng thể và toàn bộ job đều success. Job live TLC được skip đúng chính sách tránh gọi lại nguồn.

## Phạm vi bàn giao

- [Hướng dẫn cụm và pipeline HDFS](hdfs.md), `configs/hdfs.example.yaml`, `docker-compose.hdfs.yml`.
- [Publisher WebHDFS](../src/nyc_taxi_pipeline/ingestion/hdfs.py), [replay](../src/nyc_taxi_pipeline/ingestion/replay.py), [script nghiệm thu](../scripts/test_hdfs_pipeline.py).
- Cache TLC tháng 1/CSV đã có trên workspace Windows, sẵn cho config HDFS khi nhóm khởi động cụm. Không upload dataset lớn vào Git.

Máy Windows hiện chưa có Docker/WSL; cụm thật được chạy **trên CI**, không khẳng định đã khởi động HDFS trên Windows. Các URI trong JSON bằng chứng thuộc cụm CI tạm thời, đã được dừng sau nghiệm thu; đó không phải dịch vụ HDFS đang mở cho nhóm. Muốn dùng demo, nhóm chạy Compose và lấy URI manifest do chính lượt publish của mình tạo.

Hai DataNode Docker nằm trên **cùng một host**, nên chỉ kiểm tra phân phối block và lỗi từng DataNode; chưa chứng minh chịu được mất cả host hay NameNode HA. Chưa nghiệm thu Spark Batch/Structured Streaming/PostgreSQL toàn tuyến; phần đó vẫn do Mạnh/Bảo tích hợp từ các URI đã bàn giao.
