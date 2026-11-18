# Thiết kế và giao việc pipeline taxi NYC

Cập nhật 18/11/2026. Đây là đặc tả triển khai; Python/SQL hiện là khung TODO, chưa phải hệ thống đã chạy. Đọc [bản đồ công việc](README.md) và [hợp đồng dữ liệu](contracts.md). Mục 4.1–4.2 do cả nhóm tự thực hiện; lần cập nhật này không triển khai test/demo toàn tuyến.

## 1. Luồng tổng thể

Có **ba batch**, chạy mỗi 15 phút, và **ba stream**. Request đo nhu cầu; trip đo lượt điều phối/đón; trả khách đếm theo request. Không gọi số trip là số xe phân biệt, không quản lý đội xe/vehicle_id trong phạm vi này.

```mermaid
flowchart TB
    NYC[NYC trip đầy đủ và nguồn vùng] --> ING[Chính: lấy nguồn, raw/Parquet/manifest]
    ING --> H[HDFS history ready]
    ING --> R[HDFS replay ready]
    R --> RP[Replay: phát ride_requested]
    RP --> K[Kafka taxi-events: đặt / đón / trả]
    K --> ROUTE[Mạnh: chuẩn hóa, khử trùng, phân loại]
    ROUTE --> S1[Stream 1: cảnh báo baseline nhu cầu]
    ROUTE --> S2[Stream 2: view 60 phút và spike 15 giây]
    ROUTE --> S3[Stream 3: ghép, điều phối, vòng đời]
    USER[Phía người dùng: đã đồng ý ghép / chốt riêng] --> S3
    S3 --> TX[Bảo: giao dịch trip + pickup outbox + lịch trả]
    TX --> PUB[Chính: publisher sự kiện đón]
    PUB --> K
    TX --> SIM[Simulator: dropoff theo lịch]
    SIM --> K
    K --> ARCH[Archive consumer]
    TX --> JOURNAL[Journal chuyển trạng thái]
    JOURNAL --> ARCH
    ARCH --> RAW[HDFS sự kiện gốc và receipt]
    RAW --> INPUT[Snapshot đầu vào batch cùng cutoff / độ phủ]
    H --> INPUT
    INPUT --> B1[Batch 1: hai baseline lịch sử]
    INPUT --> B2[Batch 2: gradient trong ngày]
    INPUT --> B3[Batch 3: trung bình trip đón trong ngày]
    B1 --> FINAL[Tổng hợp cuối sau ba batch: số đếm 15 phút + kết quả]
    B2 --> FINAL
    B3 --> FINAL
    INPUT -. sự kiện để tổng hợp số đếm .-> FINAL
    FINAL --> BV[PostgreSQL: batch view và phiên bản công bố]
    BV -. baseline / gradient .-> S1
    BV --> DASH[Dashboard: lưu lượng và thống kê]
    S1 --> ALERT[Kafka taxi-alerts]
    S2 --> ALERT
    S1 --> SV[PostgreSQL: stream view 15 phút]
    S2 --> MIN[PostgreSQL: bản ghi phút, giữ toàn bộ lịch sử]
    S2 --> SV
    MIN --> DASH
    S2 --> LIVE[Serving view: 60 phút gần nhất]
    BV --> CHECK[Đối soát cùng phiên / zone / cửa sổ / cutoff]
    SV --> CHECK
```

Ba batch không bắt buộc chạy nối tiếp nhau. Mỗi nhánh có thể tính số đếm trung gian, nhưng **bước tổng hợp/công bố số liệu cuối đặt sau cả ba nhánh**. Bộ tổng hợp đọc sự kiện/đầu vào trung gian của batch để ra số đếm; không suy ngược số đếm từ baseline hay gradient, không sao chép số đếm stream để tự đối soát với chính nó.

## 2. Ingestion và vòng đời sự kiện — Chính

### Nguồn và replay

history kết thúc trước hoặc tại đầu replay, hai phạm vi không chồng nhau. Lấy đủ pickup/dropoff, mã vùng, định danh dòng; lưu raw, Parquet, lookup và ranh giới vùng trên HDFS rồi mới công bố manifest ready. Phân trang ổn định, retry có giới hạn; tải thiếu không ready. CSV tên vùng không thay được polygon: nếu sinh tọa độ, dùng seed ổn định theo dòng nguồn và điểm nằm trong vùng, ghi rõ mô phỏng.

HDFS không tự đẩy dữ liệu: replay đọc từng trip để tạo một yêu cầu mô phỏng. Giữ điểm đến và thời lượng nguồn; không phát pickup/dropoff chỉ vì đã đọc được trip NYC. Trip đã xảy ra trong NYC là nguồn mô phỏng nhu cầu, không phải trip điều phối mới.

### Ba sự kiện

- `ride_requested`: một request, thời gian đặt và zone đón; dùng cho baseline, gradient và spike.
- `pickup_completed`: một trip bắt đầu phục vụ, gồm các request thành viên; zone đón đầu tiên, picked_up_at. Ghép hai request vẫn chỉ phát một lượt đón.
- `dropoff_completed`: trả xong một request thuộc trip, tại zone trả và completed_at. Ghép hai người có hai sự kiện trả; trip hoàn tất khi người cuối được trả.

Sau chốt trip, ghi pickup outbox trong cùng transaction rồi publisher phát ngay; không chờ thêm thời gian xe đến trong demo. Đã có đồng ý từ phía người dùng, không dựng thêm bước xin lại đồng ý trong nhánh điều phối. Lịch trả từng người lấy trip_started_at + source_duration_seconds của người đó; không lập lịch âm, không tự hoàn tất request đang chờ/hủy. Đây là mô phỏng, chưa tính đường đi vòng.

Kafka fan-out cho stream và archive; archive giữ cả ba loại sự kiện cùng thông tin topic/partition/offset. Journal quyết định ghép/gia hạn/từ chối cũng cần lưu. Chỉ receipt committed sau ghi HDFS bền vững; raw có thể trùng, xử lý phải khử trùng. Clock mô phỏng dùng chung và vẫn tiến khi không có request mới; dùng 1× khi tương tác thủ công.

## 3. Processing — Mạnh

### Quy tắc chung

Event time có múi giờ, ngày nghiệp vụ America/New_York, cửa sổ [start,end). Dùng cửa sổ cố định 15 phút cho đối soát, khoảng 15 giây không chồng nhau cho spike và bucket phút cho dashboard. Nhịp xử lý near real-time 15 giây không đồng nghĩa batch chạy 15 giây. Request hợp lệ về thời gian/vùng vẫn được đếm nếu tọa độ/thời lượng ghép lỗi.

Khử trùng theo event và khóa nghiệp vụ trong [contracts](contracts.md). Chốt sau watermark và xác nhận đủ độ phủ; khung không sự kiện chỉ là 0 khi đủ dữ liệu. Dữ liệu sau chốt ghi cách ly; không âm thầm sửa batch hoặc stream khác chính sách. Restart giữ checkpoint/dedup, baseline snapshot và tiến độ công bố; tách scope/phiên, không cộng lại history qua replay.

### Batch 1 — Hai baseline nhu cầu

Từ history khởi tạo và lịch sử sự kiện đã đủ dữ liệu, lấy số request theo zone–slot. all_days chọn các ngày trước; same_weekday chọn cùng thứ các tuần trước. Chỉ ngày trước ngày áp dụng, không lấy ngày đang xét/tương lai. Mỗi mức = tổng request / số cửa sổ tham chiếu, tính cả khung 0 đủ độ phủ; thiếu mẫu là NULL. Chốt cửa sổ không tự cập nhật baseline bằng SQL.

Ghi kết quả staging kèm scope, loại, slot/thứ, số mẫu, kỳ lịch sử, applies_on, cutoff, batch_version; bộ tổng hợp cuối công bố cặp nguyên tử. Stream mở cửa sổ mới chọn phiên bản phù hợp thời gian mô phỏng.

### Batch 2 — Gradient nhu cầu trong ngày

Dùng K khung 15 phút liên tiếp đã chốt, đủ độ phủ, **trong cùng ngày hiện tại** tại mỗi zone; K mặc định 4, tối thiểu 2. Với số request C_i: g_i = (C_i − C_(i−1)) / 15, đơn vị yêu cầu/phút. K khung cho K−1 gradient, không thay bằng đầu–cuối/hồi quy. Không nối qua khung thiếu hoặc qua ngày trước. Đầu ngày chưa đủ K thì trả trạng thái thiếu; khung 0 đủ độ phủ vẫn hợp lệ. Gradient cung cấp ngữ cảnh cho Stream 1, không tự là điều kiện phát cảnh báo.

### Batch 3 — Trung bình lưu lượng đón trong ngày

P(z,d,w) là số trip đón tại zone z trong khung w của ngày hiện tại d. A gồm các khung từ đầu ngày đến cutoff đã chốt, đủ dữ liệu và P > 0.

**daily_pickup_average = sum(P trong A) / count(A)**

Ví dụ 2, 0, 6, 4 → (2+6+4)/3 = 4 trip/khung có hoạt động. Không tính trung bình cùng slot qua nhiều ngày. Mỗi zone–ngày có một mức hiện tại, được tính lại mỗi 15 phút; lưu phiên bản/cutoff để nhìn lại tiến trình ngày. Không có khung dương thì NULL, active_window_count=0; ngày chưa kết thúc là tạm tính. Chỉ phục vụ dashboard, không thêm điều kiện cảnh báo. Giữ tổng trip và mẫu số để không nhầm trung bình có điều kiện với tổng lưu lượng.

### Tổng hợp cuối sau ba batch

Mỗi run đóng băng input snapshot, cutoff, archive offsets và tập cửa sổ đủ độ phủ. Chờ ba nhánh hoàn tất (kết quả thiếu dữ liệu hợp lệ khác lỗi job), sau đó tổng hợp số request, trip đón, khách trả, trip hoàn tất theo zone–khung 15 phút, ghép metadata của các kết quả, ghi staging và công bố batch view trong transaction. Một nhánh lỗi thì không công bố run hoàn chỉnh. Retry cùng run không cộng lại số liệu. Ba thống kê vẫn có bảng/grain riêng, không ép baseline và trung bình ngày thành số liệu có cùng grain với window.

Batch định kỳ đối soát đọc HDFS sự kiện thực tế; không lấy trip NYC gốc để đếm trip sau ghép. Trip hoàn tất được suy từ đủ dropoff của danh sách thành viên pickup, cần cả trạng thái/sự kiện trước cửa sổ hiện tại nếu trip đi qua nhiều cửa sổ.

### Stream 1 — Cảnh báo baseline

Đếm request trong khung 15 phút. Khi mở khung giữ bền vững cặp baseline; vượt khi prev_count <= baseline < current_count. Hai mức vượt cùng lần xử lý gộp một thông điệp, mỗi điều kiện có ID ổn định. Baseline 0 kích hoạt request đầu, thiếu một mức vẫn xét mức kia. Gắn gradient đã công bố với series_end không sau thời điểm dữ liệu xử lý. Kafka ack trước ghi request_count vượt ngưỡng tương ứng vào stream view 15 phút; retry cùng ID/tổng. Snapshot baseline không đổi giữa khung dù batch công bố mới.

### Stream 2 — Lưu lượng 60 phút và spike 15 giây

Số request mới R(t) trong [t−15s,t) so với R(t−15s) trong [t−30s,t−15s). Cảnh báo khi **R(t) > k × R(t−15s) và R(t) > M**, k>1, M>=0. Khoảng trước 0: phát nếu hiện tại >M, lý do tăng từ 0, tỷ lệ NULL. Khoảng trước thiếu dữ liệu thì không giả 0. Chỉ một cảnh báo nghiệp vụ theo phiên–zone–khoảng; retry cùng alert_id. Dữ liệu muộn trong giới hạn giữ các bucket mở để kiểm tra lại, chỉ phát nếu bucket chưa có cảnh báo và đủ dữ liệu; không thu hồi cảnh báo đã gửi, lưu count/cutoff tại lúc phát. Sau chốt cách ly theo chính sách chung.

Mỗi lần xử lý có thể upsert cùng bản ghi `(replay_id, zone_id, minute_start)` theo **event time**, cập nhật tổng phát sinh của phút. Ví dụ các delta 2,1,3,2 cho tổng 2→3→6→8; bốn lần ghi không tạo bốn snapshot cùng phút. Dùng tổng tuyệt đối và update_version để retry/stale write không cộng trùng hoặc làm tổng lùi. Cảnh báo vẫn dùng bucket 15 giây, không dùng tổng phút tăng dần. Bucket phút giữ riêng request_count, pickup_trip_count, dropoff_request_count, completed_trip_count; số liệu phút được phép ghi độc lập, quy tắc ack của Stream 1 áp dụng request_count 15 phút tương ứng.

Serving view đón khách giữ 60 phút gần nhất; tại mốc phút t: V(z,t) = tổng pickup_trip_count trong [t−60 phút,t). Lấy tổng phút mới, loại phút rời cửa sổ; không cộng các tổng rolling lại với nhau. Phút đang mở hiển thị tạm tính riêng; dashboard 60 phút chốt theo mốc phút để không đòi chia một bucket ở biên. Lúc 09:01 xuất view [08:01,09:01). Bao gồm mọi zone trong lookup và zone hợp lệ quan sát được chưa có tên; zero chỉ khi đủ coverage.

**PostgreSQL giữ toàn bộ bản ghi phút, không xóa sau 60 phút.** Chỉ state/serving view giới hạn 60 phút, cộng vùng đệm watermark khi triển khai. Không bắt buộc bảng snapshot rolling thứ hai: dashboard truy vấn lịch sử bucket phút để dựng tổng 60 phút. Dữ liệu đến muộn trước chốt cập nhật đúng bucket cũ và view liên quan; version/finality cho biết bản ghi tạm hay cuối.

### Stream 3 — Ghép và điều phối

Cache lưu request đang chờ; cache phục vụ tìm nhanh, PostgreSQL là nơi xác nhận trạng thái bền vững. Hai request cùng phiên, cả hai khoảng cách đón/đến <=500m; geography mét, không dùng zone làm điều kiện. Thứ tự ổn định: người chờ lâu nhất, khoảng cách đón, đến, requested_at rồi ID. Cặp đã có đồng ý hợp lệ từ phía người dùng: kiểm tra bằng chứng/version/hạn rồi chốt ngay, không xin lại đồng ý. Dưới khóa tạo trip, thành viên, pickup outbox và lịch dropoff trong một transaction. Trip ghép có một pickup, hai dropoff; đi riêng một pickup, một dropoff. Không chỉ dựa cache để phát pickup.

Hạn riêng ban đầu 3 phút; đề xuất không reset. Hết hạn chờ gia hạn, không tự tạo trip riêng; người còn hạn tiếp tục tìm. Thời lượng gia hạn cấu hình, số lần không giới hạn. Từ chối chủ động lưu cặp không thứ tự, không đề xuất lại trong phiên; hết hạn không được coi là từ chối. API quản trị/client tiếp nhận quyết định đã có từ phía người dùng, không triển khai giao diện thương mại trong phạm vi này. Dropoff cập nhật từng người, đủ cả hai mới hoàn tất trip; không tăng lại nhu cầu.

## 4. Kho, đối soát và cấu hình — Bảo phối hợp cả nhóm

### Bảng và công bố

Bảng nguồn/sự kiện, grain, khóa và repository tại [contracts](contracts.md). batch_window_counts và stream_window_counts lưu riêng; tuyệt đối không cộng hai view. stream_minute_counts là lịch sử phút của dashboard. Các baseline, gradient và daily_pickup_averages có phiên bản; batch_runs quản lý trạng thái công bố toàn run. Stream ghi số đếm đang mở không làm baseline đổi ngay.

Chỉ dọn dữ liệu vận hành kết thúc sau khi tổng hợp bền vững, raw/journal đã có receipt HDFS, lịch đã xử lý và dedup còn bảo vệ retry. Không dọn request chờ/gia hạn; không áp dụng retention vận hành lên lịch sử dashboard phút hoặc batch/stream view trong phạm vi hiện tại. Không tăng biến số xe rảnh khi trả khách.

### Đối soát số đếm

So cùng scope/replay, zone, window_start/end và tập sự kiện đủ độ phủ. Batch chờ archive đã commit, stream đã xử lý cùng phạm vi offset và cửa sổ đã chốt; scheduler tới phút 15 không tự chứng minh đã đủ dữ liệu. Cửa sổ chưa đủ là pending, không mismatch. Event muộn sau chốt được loại/cách ly thống nhất hai phía theo chính sách event-time/watermark và metadata admission lưu trong snapshot để batch tái lập; không lấy số đếm stream làm đầu vào đối chứng. View 60 phút chỉ so với batch đếm đúng 60 phút tại cùng as_of.

Ví dụ zone A, 08:00–08:15: năm request A–E; A–B ghép, C–E chốt riêng; tất cả đón và trả trong cửa sổ. Batch và stream đều phải ra **5 request, 4 trip đón, 5 khách trả, 4 trip hoàn tất, 1 trip ghép**. Khi trả khác zone phải đối soát mỗi zone theo loại đại lượng, không gán tất cả về zone đón. Hai view bằng nhau xác nhận phép đếm nhất quán, chưa thay kiểm tra ghép/cảnh báo.

### 4.1–4.2. Kiểm thử và tích hợp do cả nhóm thực hiện

Khởi tạo baseline từ history trước, sau đó replay và chạy ba stream, archive, publisher và simulator. Batch định kỳ đọc sự kiện archive độc lập, chạy ba phép tính rồi tổng hợp/công bố cuối. Các nhánh cùng dùng clock và scope. Fixture/test/báo cáo hiện có là tài liệu cũ cần cả nhóm tự cập nhật; chưa sửa hoặc chạy nghiệm thu toàn tuyến ở lần cập nhật docs này.

### 4.3. Cấu hình

| Tham số | Quy tắc |
| --- | --- |
| business_timezone | America/New_York; DST theo chính sách nguồn rõ ràng |
| batch_interval / business_window | 15 phút |
| stream_trigger / spike_interval | 15 giây; thời gian xử lý có thể làm kết quả đến trễ |
| minute_bucket | 1 phút, upsert theo event time |
| serving_horizon | 60 phút; PostgreSQL giữ toàn bộ lịch sử phút |
| trend_window_count | K mặc định 4, >=2; không đi qua ngày trước |
| spike_multiplier / spike_min_requests | k>1, M>=0; phải cấu hình trước chạy, không tự đặt ngưỡng |
| baseline_history_range | Số ngày/tuần hoặc khoảng; không dùng ngày áp dụng/tương lai |
| pickup/dropoff matching radius | Mỗi bán kính <=500m |
| initial_wait / extension_duration | Ban đầu 3 phút; gia hạn cần cấu hình |
| replay clock / seed | Clock chung; seed tọa độ ổn định |
| watermark / retry horizon | Cấu hình; admission/coverage được ghi để đối soát |
| operational retention | Chỉ dọn khi đủ điều kiện archive/dedup; loại trừ lịch sử phân tích |

Khu vực triển khai: ingestion của Chính; pipeline của Mạnh; warehouse/SQL của Bảo; shared là hợp đồng chung. Scripts chỉ gọi module. Xem [bản đồ giao việc](README.md).
