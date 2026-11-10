# Hướng dẫn triển khai pipeline taxi NYC

## Tổng quan luồng quy trình

Demo trả lời câu hỏi **“Khu vực nào ghi nhận nhiều chuyến đón khách hơn mức lịch sử trong cùng khung 15 phút?”**

Hệ thống lấy dữ liệu chuyến đi Yellow Taxi từ **NYC Open Data API**. Tầng **ingestion** lưu phản hồi gốc, hai tập Parquet `history` và `replay`, cùng manifest trên **HDFS dùng chung**.

Tầng **processing** dùng Spark Batch để tính số chuyến đón khách trung bình theo khu vực, thứ trong tuần và khung 15 phút. Với tập phát lại, Python tạo các sự kiện đón khách và gửi dần qua Kafka. Spark Structured Streaming đọc các sự kiện đó để đếm số chuyến theo khu vực trong từng cửa sổ 15 phút.

Tầng **data warehouse** lưu kết quả tổng hợp của hai nhánh vào các bảng PostgreSQL và dùng Taxi Zone Lookup để hiện tên khu vực. Người xem chọn một phiên phát lại và một cửa sổ để truy vấn số chuyến quan sát, mức lịch sử và phần trăm chênh lệch.

```mermaid
flowchart TD
    TLC["NYC Open Data API · Yellow Taxi"]
    TLC --> SOURCE["1.1 · Gọi API theo khoảng thời gian"]
    SOURCE --> RAW["1.2 · HDFS dùng chung<br/>JSON gốc, Parquet, CSV lookup và manifest"]

    RAW -->|"Chuyến đi lịch sử"| BATCH["2.2 · Spark Batch"]
    RAW -->|"Chuyến đi dành cho phát lại"| REPLAY["1.3 · Python Replay"]
    REPLAY --> KAFKA["1.4 · Kafka topic taxi-events"]
    KAFKA --> STREAM["2.3 · Spark Structured Streaming"]
    RULES["2.1 · Quy tắc chung về thời gian và dữ liệu"] -.-> BATCH
    RULES -.-> STREAM

    SCHEMA["3.1 · Tạo bảng PostgreSQL"] --> LOAD["3.2 · Nạp kết quả vào PostgreSQL"]
    LOOKUP["Taxi Zone Lookup · CSV"] --> RAW
    RAW -->|"Bảng tra tên khu vực"| LOAD
    BATCH -->|"Mức lịch sử"| LOAD
    STREAM -->|"Số chuyến theo cửa sổ"| LOAD
    LOAD --> SQL["3.3 · SQL so sánh theo khu vực"]
    SQL --> RESULT["Tên khu vực · Số chuyến · Mức lịch sử · % chênh lệch"]
```

| Phần | Phụ trách | Kết quả cần bàn giao |
| --- | --- | --- |
| 1. Data Ingestion | Chính | Dữ liệu gốc, Parquet và manifest trên HDFS, cùng sự kiện phát lại qua Kafka |
| 2. Data Processing | Mạnh | Mức lịch sử từ batch và số chuyến từ stream |
| 3. Data Warehouse | Bảo | Các bảng PostgreSQL, cách nạp và truy vấn demo |
| 4. Kiểm tra toàn tuyến | Nhóm trưởng cùng ba thành viên | Bộ mẫu chung, kết quả đối chiếu và hướng dẫn chạy |

## 1. Data Ingestion — Chính

**Mục tiêu giai đoạn.** Đưa dữ liệu từ NYC Open Data API vào HDFS dùng chung để các thành viên đọc được từ môi trường của mình. Chính lấy đúng các khoảng thời gian nhóm đã chốt, lưu phản hồi để đối chiếu, tạo Parquet và ghi rõ từng tập trong manifest. Với tập replay, Chính đọc Parquet từ HDFS và phát các sự kiện pickup qua Kafka để Mạnh có thể đọc dần.

### 1.1. Lấy dữ liệu chuyến đi qua NYC Open Data API

- **Mục đích.** Lấy các bản ghi chuyến đi từ nguồn API theo hai khoảng thời gian mà demo cần.
- **Đầu vào.** Endpoint SODA và mã dataset. Mẫu đề xuất là **2025 Yellow Taxi Trip Data (`irx2-vgm4`)**. Nhóm trưởng chọn hai khoảng không giao nhau cho `history` và `replay`. Dùng biến môi trường cho app token khi gọi SODA v3.
- **Việc cần làm.** Lọc theo `tpep_pickup_datetime` trong từng khoảng `[bắt đầu, kết thúc)`, lấy ít nhất `tpep_pickup_datetime` và `pulocationid`, phân trang với thứ tự ổn định và ghi số trang, số dòng và lỗi. Nếu chạm giới hạn lấy dữ liệu trước khi đọc hết phạm vi, ghi rõ là tập chưa đầy đủ.
- **Đầu ra.** Các trang JSON gốc, thông tin dataset, bộ lọc, vai trò từng tập và trạng thái tải để bàn giao cho bước 1.2.

[Dataset Yellow Taxi 2025](https://data.cityofnewyork.us/Transportation/2025-Yellow-Taxi-Trip-Data/irx2-vgm4/about_data) · [Tài liệu endpoint SODA](https://dev.socrata.com/docs/endpoints.html).

### 1.2. Lưu phản hồi gốc, Parquet và manifest trên HDFS

- **Mục đích.** Giữ dữ liệu API để đối chiếu, đồng thời tạo đầu vào đọc được cho batch và replay.
- **Đầu vào.** JSON và thông tin tải từ bước 1.1, CSV Taxi Zone Lookup và thư mục gốc trên HDFS mà nhóm đã thống nhất.
- **Việc cần làm.** Lưu JSON và CSV lookup nguyên bản trên HDFS. Tạo và lưu Parquet riêng cho `history` và `replay` theo schema đã thống nhất. Công bố manifest trên HDFS với mã nguồn, vai trò, khoảng thời gian, `raw_path`, `parquet_path` nếu có và trạng thái. Chỉ ghi `ready` khi đã tải đủ phạm vi và file đầu ra trên HDFS đã hoàn chỉnh.
- **Đầu ra.** URI của manifest trên HDFS. Trong manifest, `raw_path` trỏ đến phản hồi gốc hoặc CSV lookup. `parquet_path` là URI HDFS của tập `history` cho bước 2.2 hoặc tập `replay` cho bước 1.3. Bảo dùng `raw_path` của mục `zone_lookup` ở bước 3.2.

### 1.3. Đọc lại chuyến đi và tạo sự kiện đón khách

- **Mục đích.** Mô phỏng một nguồn gửi sự kiện theo thời gian để Spark có dữ liệu đến dần.
- **Đầu vào.** URI HDFS và phạm vi `replay` từ manifest ở bước 1.2, mã phiên phát lại và cấu hình tốc độ phát.
- **Việc cần làm.** Viết chương trình Python đọc các dòng theo thời điểm đón khách và gửi dần sang Kafka. Mỗi dòng trong bộ mẫu có một `event_id` ổn định khi gửi lại. Chỉ đưa vào sự kiện các thông tin đã biết lúc đón khách.
- **Đầu ra.** Các sự kiện JSON gửi đến bước 1.4. Mỗi sự kiện có `replay_id` (mã phiên), `event_id` (mã sự kiện), `pickup_at` (thời điểm đón), `pickup_zone_id` (mã vùng đón) và `source_file` (file Parquet replay để đối chiếu qua manifest).

### 1.4. Đưa sự kiện vào Kafka topic `taxi-events`

- **Mục đích.** Đệm sự kiện để chương trình phát lại và Spark có thể chạy theo tốc độ riêng.
- **Đầu vào.** Các sự kiện JSON do chương trình Python ở bước 1.3 gửi đến.
- **Việc cần làm.** Cấu hình Kafka và topic `taxi-events`. Phát một nhóm sự kiện mẫu và kiểm tra bên đọc thấy được đúng trường và đúng mã sự kiện.
- **Đầu ra.** Topic có dữ liệu để Spark đọc ở bước 2.3. Kafka giữ các sự kiện đã nhận, còn phép đếm chuyến được thực hiện ở Spark.

## 2. Data Processing — Mạnh

**Mục tiêu giai đoạn.** Chuyển các bản ghi chuyến đi thành hai bộ số liệu có thể so sánh. Mạnh dùng Spark Batch để tính mức lịch sử và Spark Structured Streaming để đếm số chuyến vừa phát lại. Hai nhánh cùng dùng giờ New York, cùng quy tắc chọn chuyến hợp lệ và cùng cửa sổ 15 phút để Bảo ghép đúng các kết quả tương ứng.

### 2.1. Thống nhất quy tắc thời gian và dữ liệu

- **Mục đích.** Để số chuyến trong hai nhánh được tính theo cùng cách.
- **Đầu vào.** Schema Parquet, JSON sự kiện mẫu và khoảng thời gian từ Chính. Đối chiếu ánh xạ từ các trường API `tpep_pickup_datetime` và `pulocationid` sang thời điểm đón và mã vùng chuẩn.
- **Việc cần làm.** Chuẩn hóa thời điểm đón và mã khu vực. Dùng giờ New York (`America/New_York`) để xác định thứ và khung 15 phút. Dùng timestamp có múi giờ khi gửi hoặc lưu. Quy ước cửa sổ gồm mốc bắt đầu và loại mốc kết thúc, nên chuyến đúng 08:15 thuộc khung 08:15–08:30. Ghi nhận và loại khỏi phép đếm bản ghi thiếu thời điểm đón hoặc có mã vùng sai định dạng.
- **Đầu ra.** Bộ quy tắc và cách ghi số liệu kiểm tra chất lượng dùng chung cho 2.2 và 2.3. Nhịp đọc dữ liệu của Spark được cấu hình riêng với độ dài cửa sổ 15 phút.

### 2.2. Tính mức lịch sử bằng Spark Batch

- **Mục đích.** Trả lời “Khu vực này thường có bao nhiêu chuyến vào thứ và khung giờ đang xét?”
- **Đầu vào.** File `history` đã sẵn sàng tại `parquet_path` trên HDFS, phạm vi trong manifest và quy tắc từ 2.1.
- **Việc cần làm.** Đếm chuyến theo từng khu vực và từng cửa sổ lịch sử. Sau đó lấy trung bình các cửa sổ có cùng thứ và khung giờ, ví dụ các thứ Hai từ 08:00 đến 08:15. Chỉ dùng các cửa sổ có độ phủ đã xác nhận, tính cả cửa sổ thực sự có 0 chuyến.
- **Đầu ra.** Một dòng cho mỗi khu vực, thứ và khung giờ, gồm `pickup_zone_id`, `iso_weekday`, `slot_15m`, `baseline_trip_count` và `reference_windows` (số cửa sổ dùng để tính). Bàn giao kèm phạm vi lịch sử cho bước 3.2.

### 2.3. Đếm chuyến phát lại bằng Spark Structured Streaming

- **Mục đích.** Cập nhật số chuyến của từng khu vực trong mỗi cửa sổ khi có sự kiện mới.
- **Đầu vào.** Topic `taxi-events` ở bước 1.4 và quy tắc từ 2.1.
- **Việc cần làm.** Đọc JSON, kiểm tra trường bắt buộc và loại lần gửi lại theo cặp `replay_id` và `event_id`. Phân cửa sổ theo giờ đón khách ghi trong sự kiện. Lưu checkpoint theo phiên để hỗ trợ đọc tiếp khi khởi động lại. Ghi số bản ghi hợp lệ, lỗi và sự kiện gửi trùng được phát hiện.
- **Đầu ra.** Số chuyến lũy kế của từng cửa sổ trong phiên, gồm `replay_id`, `pickup_zone_id`, `window_start`, `window_end` và `trip_count`. Ví dụ, khi cửa sổ tăng từ 2 lên 3 chuyến, gửi tổng mới là 3 cho bước 3.2.

## 3. Data Warehouse và truy vấn — Bảo

**Mục tiêu giai đoạn.** Biến kết quả của hai nhánh thành câu trả lời có thể xem bằng SQL. Bảo dựng ba bảng, nạp tên vùng, mức lịch sử và số đếm từ stream. Khi người dùng chọn một phiên và một cửa sổ, truy vấn ghép đúng vùng và khung giờ để hiển thị số chuyến, mức lịch sử và phần trăm chênh lệch.

### 3.1. Tạo các bảng PostgreSQL

- **Mục đích.** Xác định nơi lưu bảng tra cứu và hai loại kết quả xử lý.
- **Đầu vào.** Cấu trúc Zone Lookup ở 1.2 và các cột đầu ra đã thống nhất với Mạnh ở 2.2 và 2.3.
- **Việc cần làm.** Tạo bảng khu vực theo mã vùng, bảng mức lịch sử theo vùng–thứ–khung giờ và bảng số chuyến phát lại theo phiên–vùng–mốc đầu cửa sổ. Dùng bộ khóa tương ứng để mỗi kết quả chỉ có một dòng.
- **Đầu ra.** SQL khởi tạo ba bảng và mô tả các cột, kiểu dữ liệu, khóa để Mạnh và Bảo thống nhất cách nạp.

### 3.2. Nạp dữ liệu và cập nhật kết quả

- **Mục đích.** Đưa đầu ra của hai nhánh vào kho để SQL có thể truy vấn.
- **Đầu vào.** Các bảng ở 3.1, Zone Lookup ở 1.2, mức lịch sử ở 2.2 và số đếm cửa sổ ở 2.3.
- **Việc cần làm.** Nạp lookup từ URI HDFS trong manifest và nạp mức lịch sử. Nếu dùng `psql \copy`, tải CSV từ HDFS về file tạm trên máy chạy `psql` trước khi nạp. Khi nhận tổng mới từ stream, cập nhật dòng theo khóa phiên–vùng–cửa sổ. Kiểm tra nạp lại cùng một đầu ra để xác nhận số dòng và số chuyến giữ nguyên.
- **Đầu ra.** Ba bảng có dữ liệu trong PostgreSQL, sẵn sàng cho bước 3.3.

### 3.3. Truy vấn kết quả demo

- **Mục đích.** Cho biết số chuyến trong cửa sổ vừa phát lại chênh lệch thế nào so với mức lịch sử tương ứng.
- **Đầu vào.** Mã phiên replay, cửa sổ muốn xem và ba bảng từ 3.2.
- **Việc cần làm.** Ghép theo mã vùng, thứ và khung giờ New York. Tính tỷ lệ theo công thức `(số chuyến phát lại − mức lịch sử) / mức lịch sử × 100%`. Nếu thiếu mức lịch sử hoặc mức lịch sử bằng 0, vẫn hiện số chuyến và ghi lý do để trống tỷ lệ. Nếu lookup thiếu tên, vẫn hiện mã vùng.
- **Đầu ra.** Câu SQL có thể chạy lại và bảng kết quả gồm mã/tên vùng, số chuyến phát lại, mức lịch sử, số cửa sổ tham chiếu và tỷ lệ chênh lệch.

## 4. Kiểm tra toàn tuyến — Cả nhóm

### 4.1. Chạy trên bộ mẫu có kết quả biết trước

- **Mục đích.** Kiểm tra từng điểm bàn giao và kết quả cuối trước khi chạy trên mẫu TLC thật.
- **Đầu vào.** Các trang JSON mẫu theo schema API và CSV lookup có một vùng A. Hai cửa sổ lịch sử 08:00–08:15 vào hai thứ Hai trước kỳ replay có lần lượt 1 và 3 chuyến. Vào thứ Hai của kỳ replay có các chuyến lúc 08:01, 08:05, 08:14 và 08:15. Ca gửi trùng phát lại một sự kiện với cùng mã.
- **Việc cần làm.** Chuẩn bị HDFS dùng chung, Kafka và các bảng PostgreSQL. Nhập JSON mẫu và công bố Parquet, CSV, manifest trên HDFS. Kiểm tra Spark Batch và Python replay đọc được URI đã bàn giao, rồi chạy batch, khởi động stream, phát sự kiện và chạy SQL.
- **Đầu ra.** Mức lịch sử là **2 chuyến** trên hai cửa sổ. Stream ghi **3 chuyến** trong khung 08:00–08:15 và **1 chuyến** trong khung 08:15–08:30. SQL cho vùng A ở khung đầu trả **3 chuyến**, mức lịch sử **2**, chênh lệch **+50%**.

### 4.2. Chạy lại với dữ liệu lấy qua NYC Open Data API và bàn giao

- **Mục đích.** Xác nhận hệ thống có thể nhận dữ liệu nguồn đã chọn và tạo bảng phân tích.
- **Đầu vào.** Pipeline đã qua 4.1, cấu hình API, lookup và phạm vi lịch sử/replay do nhóm trưởng chốt.
- **Việc cần làm.** Chạy toàn tuyến với dữ liệu đã chốt. Mỗi người ghi lệnh chạy, đường dẫn mã, đầu vào, đầu ra thực tế và phần còn vướng. Ba thành viên có thể làm song song từ bộ mẫu 4.1 sau khi thống nhất schema file, JSON và bảng kết quả.
- **Đầu ra.** Một lượt chạy có thể lặp lại, ghi rõ mã dataset, bộ lọc API, kỳ lịch sử, kỳ replay và kết quả SQL. Nếu mới chạy được bộ mẫu kiểm tra, ghi riêng trạng thái của lượt lấy dữ liệu từ API.

Tài liệu tham khảo cho dữ liệu nguồn, bộ mẫu và báo cáo bàn giao. [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) · [Đặc tả bộ mẫu](../../data/fixtures/README.md) · [Báo cáo MVP](../mvp-report.md).
