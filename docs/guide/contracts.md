# Hợp đồng chung giữa các module

Đây là hợp đồng đích để triển khai, chưa phải schema đã được chạy kiểm chứng. Tên bảng bám thiết kế tổng thể; các tên trường chi tiết dưới đây là quy ước khởi đầu để ba nhánh dùng nhất quán. Khi đổi schema, cập nhật bên phát, bên nhận và schema_version cùng nhau.

## Thời gian, định danh và phạm vi

- Timestamp trao đổi dùng ISO 8601 có offset/UTC; PostgreSQL dùng timestamptz. Ngày/thứ/slot tính theo America/New_York. Cửa sổ 15 phút [start,end); iso_weekday 1–7, slot_15m 0–95. Hai instant trùng giờ địa phương khi DST vẫn là hai cửa sổ khác nhau theo window_start tuyệt đối. Chính và Mạnh phải chốt cách xử lý giờ nguồn mơ hồ/không tồn tại; không tự gán UTC cho giờ địa phương.
- replay_id nhận diện phiên; scope_id nhận diện tập dữ liệu được tính toán (history hoặc phiên được chọn). Giữ ánh xạ scope với nguồn và phạm vi. Batch không tự trộn phiên; một cửa sổ nguồn không được đóng góp lần nữa chỉ vì replay bằng ID mới.
- source_record_id phân biệt dòng nguồn kể cả khi giá trị giống nhau. request_id giữ nguyên trong vòng đời; event_id giữ nguyên khi retry nhưng khác giữa request và dropoff. Khử trùng event theo (replay_id,event_id), nhu cầu theo (replay_id,request_id).
- Đồng hồ mô phỏng chung cung cấp now và tốc độ; vẫn tiến khi nguồn không phát. Timeout, gia hạn, trip_started_at và due_at dùng cùng clock. Mốc xử lý thực có thể ghi riêng để đo độ trễ.

## Nguồn và manifest: Chính → Mạnh/Bảo

Parquet dùng source_record_id, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id, source_file; tọa độ nếu có. Thời gian nguồn phải được chuẩn hóa theo chính sách đã chốt. Mạnh chuyển pickup_at thành requested_at cho dữ liệu nhu cầu lịch sử.

Manifest có dataset_role (history/replay/zone_lookup/zone_boundaries), source_id/source_version, raw_path, parquet_path, range_start/end, row_count, coverage và status. Khoảng và Parquet có thể trống với nguồn lookup. Coverage phải mô tả phạm vi thời gian/vùng đã xác nhận, không chỉ là một cờ tải xong. Chỉ ready sau khi các đầu ra đã ghi hoàn chỉnh; history kết thúc không muộn hơn đầu replay. URI là HDFS mà tiến trình nhận đọc được.

## Kafka: Chính → router, archive

| Trường | Kiểu/quy tắc |
| --- | --- |
| schema_version | Số nguyên; phiên bản khởi đầu đề xuất 1 |
| event_type | ride_requested hoặc dropoff_completed |
| event_id, replay_id | Chuỗi không rỗng |
| event_at | Timestamp có múi giờ của sự kiện nghiệp vụ |
| source_kind | nyc_replay hoặc simulator; phân biệt nguồn mô phỏng |
| source_file, source_record_id | Định danh nguồn nếu có, không dùng vị trí thay đổi sau mỗi lần chạy |

ride_requested bổ sung request_id, requested_at, pickup_zone_id, dropoff_zone_id, pickup_lat/lon, dropoff_lat/lon, source_pickup_at/source_dropoff_at hoặc source_duration_seconds, coordinates_simulated. Tọa độ WGS84 (lat −90..90, lon −180..180), thời lượng không âm. Trường ghép lỗi/thiếu có thể null cùng lý do: không làm mất nhu cầu có requested_at và vùng đón hợp lệ.

dropoff_completed bổ sung trip_id, request_id, completed_at, dropoff_zone_id, dropoff_lat/lon và scheduled_event_id. event_at = completed_at. Mỗi request được phục vụ có một hoàn tất nghiệp vụ; trip ghép có hai request và hai hoàn tất. Không tăng nhu cầu khi nhận dropoff.

## Nhu cầu và batch: Mạnh ↔ Bảo

| Đầu ra | Khóa và nội dung bàn giao |
| --- | --- |
| demand_windows | (scope_id, zone_id, window_start); replay_id nếu có, window_end, request_count, coverage, is_final, update_version, snapshot hai baseline |
| historical_baseline | (scope_id, batch_version, baseline_type, zone_id, slot_15m, weekday_key, applies_on); baseline_type all_days/same_weekday, weekday_key đề xuất 0 cho all_days và 1–7 cho same_weekday để tránh khóa nullable; request_sum, reference_windows, baseline_value, history_start/end, cutoff |
| demand_gradients | (scope_id, batch_version, zone_id, series_end, pair_index); previous/current_window_start, previous/current_count, gradient, K, cutoff; truy vấn trả nguyên chuỗi K khung/K−1 cặp |

Baseline chỉ dùng ngày trước applies_on; không có mẫu thì baseline_value NULL, reference_windows=0. Khung đủ độ phủ không request là 0; thiếu độ phủ không phải 0. Batch công bố hai loại cùng phiên bản nguyên tử, không tính lại baseline trong giao dịch chốt. Gradient chỉ từ các khung đã chốt liên tiếp; stream đọc series_end không sau thời điểm dữ liệu xử lý.

Giao diện repository đề xuất: read_final_windows(scope,cutoff), publish_baselines(version,rows), publish_gradients(version,rows), read_baseline_pair(scope,zone,window), read_latest_trend(scope,zone,as_of), upsert_window(total,version,snapshot), finalize_window(key,coverage). Đây là tên giao diện dự kiến, các file hiện chưa định nghĩa hàm. Không truyền Spark DataFrame vào nghiệp vụ API/ghép; adapter JDBC/staging xử lý riêng đầu ra batch.

## Cảnh báo: Mạnh → consumer điều phối/Bảo

Payload có replay_id, zone_id, window_start/end, request_count và conditions (mỗi phần tử có alert_id, baseline_type, baseline_value, reference_windows, publication_version). alert_id ổn định theo (replay_id,zone_id,window_start,baseline_type). Cùng lần vượt hai mức có hai conditions trong một thông điệp. Kèm trend gồm counts, gradients, K, version, series_end hoặc trend_unavailable_reason.

Consumer phải khử trùng từng alert_id. Kafka ack trước upsert tổng tương ứng; crash ở giữa có thể gửi lại cùng ID. Không tuyên bố exactly-once xuyên Kafka/PostgreSQL. Snapshot baseline và tiến độ cảnh báo phải bền vững để restart không đổi điều kiện hoặc bỏ sót lần vượt.

## Ghép và lịch hoàn tất: Mạnh/API ↔ Bảo ↔ Chính

Request khóa (replay_id,request_id), có hai điểm, thời lượng nguồn, requested_at, expires_at, trạng thái và version. Đề xuất tham chiếu đúng hai request cùng phiên; cặp từ chối chuẩn hóa ID theo thứ tự. Tọa độ đưa vào PostGIS theo (lon,lat), geography(Point,4326); hai điều kiện bán kính đều <=500 mét. Không dùng zone làm điều kiện ghép.

Giao diện kho đề xuất: find_candidates, reserve_pair, record_consent, confirm_pair, reject_pair, extend_request, start_solo, expire_requests, complete_request. Thao tác nhận thời gian mô phỏng, expected_version và idempotency_key khi cần. Kho kiểm tra lại trạng thái/hạn dưới khóa, trả trạng thái/version hiện tại nếu xung đột. Hai bằng chứng đồng ý gắn từng request mới xác nhận ghép.

Trip, trip_requests và scheduled_events tạo trong cùng giao dịch. Lịch có event_id duy nhất, replay_id, trip_id, request_id, due_at, payload, trạng thái/claim/ack. due_at = trip_started_at + thời lượng của request. Simulator dùng claim_due_events và acknowledge_sent; dropoff được consumer xử lý mới đánh dấu request hoàn tất. Trip ghép chờ cả hai người.

Hạn ban đầu 3 phút độc lập; đề xuất không reset hạn. Hết hạn chuyển chờ phản hồi gia hạn, không tự đi riêng. Thời lượng gia hạn chưa chốt, phải cấu hình; số lần không giới hạn. Từ chối cặp và hết hạn là hai sự kiện khác nhau.

## Archive và retention: Chính ↔ Bảo

Archive receipt có archive_id, HDFS path, nguồn/phiên, phạm vi topic-partition-offset hoặc danh sách event/journal ID, trạng thái committed. Chỉ ghi receipt sau khi dữ liệu bền vững. Raw có thể trùng; downstream khử trùng theo ID. Journal chuyển trạng thái API phải được chuyển sang HDFS trước dọn, cùng sự kiện request/dropoff.

Dọn dữ liệu chỉ khi tổng hợp bền vững, trip/request kết thúc, lịch xong, archive đã xác nhận và ledger còn bảo vệ retry. Thời hạn ledger phải bao phủ khoảng retry/replay được hỗ trợ; không dọn request còn chờ/gia hạn. Kho cung cấp read_archive_receipts/record_archive_receipt và kiểm tra điều kiện dọn trong transaction.
