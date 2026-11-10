# Prompt tiếp tục phần ingestion của Chính vào ban đêm

Đọc file này như yêu cầu thực hiện công việc. Làm đến khi các tiêu chí dưới đây đạt hoặc có trở ngại cụ thể; ghi báo cáo để Chính đọc vào sáng hôm sau. File này là hướng dẫn cho một phiên agent được khởi chạy, không tự tạo lịch chạy.

## Bối cảnh và phạm vi

- Repository: `https://github.com/tnbminh020506/nyc-taxi-pipeline`, workspace hiện tại `D:\BigData\nyc-taxi-pipeline`.
- Làm trên nhánh **`demo/ingestion`**, đã được tạo từ `demo/mvp`. Kiểm tra `git status` trước khi sửa; giữ thay đổi có sẵn của người dùng. Không push/merge vào `main`, không reset/force-push.
- Chính phụ trách 1.1–1.4 trong `docs/guide/giao-viec-demo-taxi.md`: ingestion TLC bằng DuckDB, Parquet/manifest, replay và Kafka. Không triển khai thay phần Spark/PostgreSQL của Mạnh/Bảo.
- Nhóm trưởng đã đổi nguồn chính từ API SODA sang **Parquet tháng chính thức của TLC**. Không quay lại sửa v2/v3, không yêu cầu token Socrata.
- Yêu cầu Chính đã xác nhận: history lớn hơn replay, tỉ lệ khoảng **3:2 theo số bản ghi hợp lệ (60%/40%)**, không tính theo độ dài thời gian. Ghi số dòng thực tế; không gọi phép chia 1:1 là 3:2. Giữ hai kỳ không giao nhau, history trước replay. Không chia ngẫu nhiên các dòng làm lẫn lịch sử với tương lai.
- Bộ demo đã chọn: 06/01/2025 và 20/01/2025, cùng 08:00–10:00 New York, history **8.461**, replay **5.651** dòng hợp lệ (59,96%/40,04%), 8 cửa sổ 15 phút mỗi kỳ. `replay_id: tlc-demo-3-2-001`, tốc độ 100 sự kiện/giây (~57 giây + overhead broker). Dùng bộ này nếu không có yêu cầu mới; không tự mở rộng nguồn để làm lại việc đã đạt.

## Yêu cầu HDFS mới đã merge từ Minh

Đọc thêm `docs/hdfs.md`, `docs/hdfs-validation.md`, `configs/hdfs.example.yaml` và `ingestion/hdfs.py`. Luồng bắt buộc: Parquet nguồn nguyên bản lưu HDFS → DuckDB xử lý cache có checksum → output/lookup/provenance/manifest HDFS → replay đọc HDFS → Kafka. Không bỏ HDFS hoặc quay về local manifest khi upload lỗi. Chỉ ready sau SHA-256 và hai vị trí block thực tế. Giữ URI manifest bất biến để consumer pin phiên bản. Không coi hai DataNode Docker cùng host là chịu lỗi mất cả host. Không tự cài Docker/WSL lên Windows; dùng CI HDFS thật đã cấu hình, không mở live TLC.

## Đọc trước khi sửa

1. `docs/guide/giao-viec-demo-taxi.md`, `docs/ingestion.md`, `docs/duckdb-validation.md`.
2. `configs/demo.example.yaml`, các config local hiện có, `src/nyc_taxi_pipeline/ingestion/tlc.py`, `common.py`, `manifest.py`, `replay.py`.
3. `scripts/run_ingestion.py`, `scripts/replay_taxi_stream.py`, `scripts/benchmark_tlc.py`, `tests/test_tlc_ingestion.py` và `.github/workflows/ingestion.yml`.
4. Kiểm tra `git log`, báo cáo trong `docs/evidence/` và cache thực tế trước khi tái sử dụng số liệu cũ.

## Hạn chế truy cập nguồn

Đã quan sát HTTP 403 từ CloudFront trên mạng Windows sau các lượt gọi lặp. Chưa xác nhận nguyên nhân; không được khẳng định chắc chắn là do spam, hoặc rằng DuckDB sẽ sửa lỗi CDN.

- **Ưu tiên hoàn toàn offline/cache.** Kiểm tra `output/tlc-source-cache`, CSV lookup đã lưu và các mục manifest `ready`. Với đầu vào đã có, dùng `source.files` và đường dẫn lookup local. Batch/replay dùng Parquet đã xuất, không tải lại TLC mỗi lần demo.
- Benchmark mặc định dùng local; thiếu cache thì báo thiếu, không tự gọi mạng. Đọc metadata, đếm, lọc, chọn mốc chia và chạy lặp đều làm trên file local.
- Không chạy vòng lặp HTTPS để cố đạt 100%; không mở nhiều worker/runner để gọi cùng nguồn. Mỗi file tháng thiếu chỉ chuẩn bị một lượt tải chủ động, tuần tự, có giới hạn. Không tự bật job `run_live_tlc` của CI trong phiên ban đêm nếu người dùng chưa yêu cầu nghiệm thu mạng mới.
- Nếu đã được yêu cầu tải mới: dùng URL TLC chính thức; dừng khi 403, tôn trọng 429/Retry-After và cơ chế retry hữu hạn hiện có. Không đổi IP/proxy/hostname hoặc bỏ TLS để cố vượt chặn. Ghi lỗi và tiếp tục phần kiểm thử local làm được.
- Không tải lại cả năm: đã có bằng chứng xuất **48.722.567 dòng** và đối chiếu toàn bộ trên Ubuntu. Đây là số liệu lịch sử của cấu hình cũ, không phải kết quả tự động cho cấu hình vừa thay đổi.

## Công việc cần hoàn thành

1. Kiểm tra cấu hình demo hiện tại có history/replay khoảng 3:2 theo quy ước mới nhất đã chốt. Khi chưa có quy ước khác, ưu tiên số bản ghi hợp lệ thực tế, mục tiêu history khoảng 57–63% tổng; đồng thời báo cả độ dài hai kỳ. Không ép chính xác bằng cách bỏ ngẫu nhiên dữ liệu hoặc dùng LIMIT rồi vẫn ghi đủ độ phủ.
2. Chọn phạm vi vừa đủ demo, ưu tiên các cửa sổ 15 phút hoàn chỉnh và các thứ/khung giờ mà history có dữ liệu tham chiếu cho replay. Nếu không cân bằng được cả tỉ lệ lẫn khung nghiệp vụ, ghi rõ đánh đổi; không âm thầm thay bài toán.
3. Đồng bộ config demo, benchmark, hướng dẫn và local config liên quan. Mốc chia có timezone New York, history kết thúc trước hoặc đúng lúc replay bắt đầu. Giữ timestamp UTC trong Parquet/sự kiện và quy tắc DST hiện có.
4. Giữ schema bảy cột, manifest v1, checksum và payload Kafka. Chỉ công bố `ready` khi đọc hết các file cần thiết và ghi xong đầu ra. Dữ liệu lỗi vẫn có cờ, replay bỏ dòng lỗi; giữ hai chuyến trùng giá trị thành hai dòng riêng.
5. Kiểm thử có ý nghĩa: không giao kỳ, tỉ lệ được báo đúng từ số dòng, cache không phát sinh HTTP, checksum/ID khi phát lại ổn định, thiếu file/lỗi nguồn không tạo `ready`. Chạy unit/contract; chạy Kafka thật nếu broker hiện có hoặc để CI đã cấu hình kiểm tra. Không bật live TLC để chạy CI thông thường.
6. Nếu cần đo lại khối lượng lớn, chỉ dùng các file thật đã cache, một lượt lớn trước; chỉ lặp thêm khi có lỗi hoặc thay đổi cần xác minh. Ghi thời gian, RSS, spill, số dòng hợp lệ/lỗi và đối chiếu độc lập. Không nhận `COUNT(*)` metadata là bằng chứng đã xuất hàng triệu dòng.

## Lệnh tham khảo (PowerShell)

```powershell
Set-Location D:\BigData\nyc-taxi-pipeline
git status --short --branch
.\.venv\Scripts\python.exe -m pytest -q -m 'not kafka'

# Chỉ dùng nếu config này đã được cập nhật theo yêu cầu mới.
.\.venv\Scripts\python.exe scripts/run_ingestion.py --config configs/tlc-cached.local.yaml
.\.venv\Scripts\python.exe scripts/replay_taxi_stream.py --config configs/tlc-cached.local.yaml --dry-run --output output/overnight-events.jsonl

# Benchmark local mặc định không tải nguồn còn thiếu.
.\.venv\Scripts\python.exe scripts/benchmark_tlc.py --source-mode local --months 2 --large-repeats 1 --small-repeats 0 --report output/overnight-benchmark.json
```

## Bàn giao khi kết thúc phiên

- Ghi `docs/overnight-result.md`: thay đổi đã làm; ngày giờ; kỳ history/replay và cách đo 3:2; số dòng thực tế, tỉ lệ và số lỗi; lệnh kiểm thử, kết quả và đường dẫn bằng chứng; phần chưa xác minh và lý do.
- Không sửa các JSON nghiệm thu cũ để phù hợp cấu hình mới. Bằng chứng mới dùng tên file mới; dữ liệu lớn/cache/token không commit.
- Khi code và kiểm thử đạt, kiểm tra diff, commit và push **`demo/ingestion`** theo ủy quyền của Chính. Nếu có thay đổi code, chờ CI unit/Kafka và sửa lỗi thuộc phần mình; live TLC phải tiếp tục tắt nếu không được yêu cầu.
- Không để một job chạy vô hạn chờ sáng. Kết thúc bằng báo cáo tự đọc được, commit/nhánh đã push và các trở ngại thật nếu có. Không nhận hoàn thành toàn tuyến Spark/PostgreSQL khi mới kiểm thử ingestion.
