# Hợp đồng chung giữa các module

Đây là hợp đồng đích để triển khai, chưa phải schema đã được chạy kiểm chứng. Tên bảng bám thiết kế tổng thể; các tên trường chi tiết dưới đây là quy ước khởi đầu để ba nhánh dùng nhất quán. Khi đổi schema, cập nhật bên phát, bên nhận và schema_version cùng nhau.

## Thời gian, định danh và phạm vi

- Timestamp trao đổi dùng ISO 8601 có offset/UTC; PostgreSQL dùng timestamptz. Ngày/thứ/slot tính theo America/New_York. Cửa sổ 15 phút [start,end); iso_weekday 1–7, slot_15m 0–95. Hai instant trùng giờ địa phương khi DST vẫn là hai cửa sổ khác nhau theo window_start tuyệt đối. Chính và Mạnh phải chốt cách xử lý giờ nguồn mơ hồ/không tồn tại; không tự gán UTC cho giờ địa phương.
- replay_id nhận diện phiên; scope_id nhận diện tập dữ liệu được tính toán (history hoặc phiên được chọn). Giữ ánh xạ scope với nguồn và phạm vi. Batch không tự trộn phiên; một cửa sổ nguồn không được đóng góp lần nữa chỉ vì replay bằng ID mới.
- source_record_id phân biệt dòng nguồn kể cả khi giá trị giống nhau. request_id giữ nguyên trong vòng đời; event_id giữ nguyên khi retry nhưng khác giữa request, pickup và dropoff. Khử trùng event theo (replay_id,event_id), nhu cầu theo (replay_id,request_id).
- Đồng hồ mô phỏng chung cung cấp now và tốc độ; vẫn tiến khi nguồn không phát. Timeout, gia hạn, trip_started_at và due_at dùng cùng clock. Mốc xử lý thực có thể ghi riêng để đo độ trễ.

## Nguồn và manifest: Chính → Mạnh/Bảo

Parquet dùng source_record_id, pickup_at, dropoff_at, pickup_zone_id, dropoff_zone_id, source_file; tọa độ nếu có. Thời gian nguồn phải được chuẩn hóa theo chính sách đã chốt. Mạnh chuyển pickup_at thành requested_at cho dữ liệu nhu cầu lịch sử.

Manifest có dataset_role (history/replay/zone_lookup/zone_boundaries), source_id/source_version, raw_path, parquet_path, range_start/end, row_count, coverage và status. Khoảng và Parquet có thể trống với nguồn lookup. Coverage phải mô tả phạm vi thời gian/vùng đã xác nhận, không chỉ là một cờ tải xong. Chỉ ready sau khi các đầu ra đã ghi hoàn chỉnh; history kết thúc không muộn hơn đầu replay. URI là HDFS mà tiến trình nhận đọc được.

## Kafka và các đơn vị nghiệp vụ

Hợp đồng đích schema_version=2 cho mô hình ba sự kiện. Bản schema cũ cần adapter rõ ràng hoặc cách ly; không đoán pickup từ payload cũ. Chung: event_id, replay_id, scope_id, event_type, event_at có offset/UTC, source_kind (nyc_replay/simulator/dispatch), source_file/source_record_id nếu có. Kafka key đề xuất theo replay_id/request_id cho request, replay_id/trip_id cho vòng đời trip. Không dựa vào thứ tự giữa hai key để bảo đảm pickup luôn đến trước dropoff.

| Loại | Trường nghiệp vụ | Khóa duy nhất để đếm | Zone / event time |
| --- | --- | --- | --- |
| ride_requested | request_id, requested_at, pickup_zone_id, dropoff_zone_id, pickup_lat/lon, dropoff_lat/lon, source_duration_seconds hoặc thời gian nguồn, coordinates_simulated | (replay_id,request_id) | pickup_zone_id / requested_at |
| pickup_completed | trip_id, request_ids (1 hoặc 2), picked_up_at, pickup_zone_id, pickup_lat/lon, trip_mode (solo/shared), outbox_id | (replay_id,trip_id) | zone đón đầu tiên / picked_up_at |
| dropoff_completed | trip_id, request_id, completed_at, dropoff_zone_id, dropoff_lat/lon, scheduled_event_id | (replay_id,trip_id,request_id) | dropoff_zone_id / completed_at |

Tất cả loại có event_at bằng thời gian nghiệp vụ tương ứng. Lat/lon WGS84; đưa vào PostGIS theo (lon,lat). Thiếu/lỗi dữ liệu ghép không loại nhu cầu nếu thời gian/vùng đón hợp lệ. source_duration_seconds không âm. Trip không đồng nghĩa xe phân biệt; một pickup cho cả trip ghép. Số khách trả đếm theo request, không đếm theo trip.

Trip hoàn tất là kết quả dẫn xuất khi có dropoff của đủ request_ids. completed_trip_count tăng một lần tại max(completed_at); quy ước zone là zone của lượt trả cuối, nếu cùng timestamp chọn request_id theo thứ tự từ điển làm tie-break ổn định. shared_trip_count đếm tại pickup, không tại dropoff. Batch cần đọc thành viên/trạng thái từ sự kiện trước cutoff kể cả pickup thuộc cửa sổ/ngày trước. Stream buffer sự kiện chưa đủ quan hệ, không tự tạo trip từ dropoff đến sớm.

## Grain và bảng phân tích — Mạnh ↔ Bảo

| Bảng/view đích | Khóa / nội dung |
| --- | --- |
| batch_runs | (scope_id,batch_version); input_snapshot_id, cutoff, archive_offset_ranges, admission_policy_version, coverage, từng trạng thái B1/B2/B3, publication_status |
| batch_window_counts | (scope_id,batch_version,zone_id,window_start); replay_id nếu có, window_end, request_count, pickup_trip_count, dropoff_request_count, completed_trip_count, shared_trip_count, coverage, is_final |
| stream_window_counts | (scope_id,zone_id,window_start); các count cùng tên, window_end, coverage, is_final, snapshot hai baseline; version riêng cho writer request và writer vòng đời |
| stream_minute_counts | (replay_id,zone_id,minute_start); request_count, pickup_trip_count, dropoff_request_count, completed_trip_count, shared_trip_count, update_version, coverage, is_final |
| historical_baseline | (scope_id,batch_version,baseline_type,zone_id,slot_15m,weekday_key,applies_on); weekday_key=0 cho all_days, 1–7 cho same_weekday; request_sum, reference_windows, baseline_value, history_start/end, cutoff |
| demand_gradients | (scope_id,batch_version,zone_id,business_date,series_end,pair_index); previous/current_window_start và count, gradient, K, cutoff; trả đủ K khung và K−1 cặp |
| daily_pickup_averages | (scope_id,batch_version,zone_id,business_date); cutoff, pickup_trip_sum, active_window_count, average_pickup_trips, is_day_final |

Thay bảng demand_windows dự kiến trước đây bằng hai view 15 phút có nguồn rõ. Không lấy stream_window_counts làm đáp án cho batch_window_counts; không cộng hai bảng. Chọn đúng một batch_version đã công bố khi truy vấn, không cộng nhiều phiên bản. Snapshot hai baseline của Stream 1 giữ nguyên khi batch thay đổi.

Batch 1 tính cả khung 0 đủ coverage; không mẫu trả NULL, không dùng ngày applies_on. Batch 2 chỉ K khung liên tiếp cùng ngày hiện tại, tính cả 0 đủ coverage; thiếu K/thiếu khung trả insufficient_data. Batch 3 chỉ khung P>0 trong ngày hiện tại, average_pickup_trips=pickup_trip_sum/active_window_count; mẫu số 0 trả NULL. Cả ba tính mỗi 15 phút, staging trước, tổng hợp/công bố cuối sau cả ba. Thiếu dữ liệu hợp lệ không làm run thất bại; lỗi thực thi thì không công bố bộ kết quả hoàn chỉnh.

Repository đề xuất: begin_batch_run(snapshot), stage_baselines, stage_gradients, stage_daily_pickups, publish_batch_run(version,window_counts), read_baseline_pair, read_latest_trend. publish_batch_run kiểm tra ba trạng thái và staging, công bố nguyên tử. history NYC phục vụ baseline khởi tạo; event archive phục vụ batch định kỳ và đếm sau điều phối; ghi rõ scope, không trộn/replay lại cùng nguồn lần hai.

## Bản ghi phút và serving view 60 phút

`stream_minute_counts` là tổng phát sinh trong phút, không phải snapshot tổng rolling 60 phút. Ví dụ delta 15 giây là 2,1,3,2 → ghi tổng 2,3,6,8 vào cùng khóa. Các khoảng lấy theo event time; sự kiện đúng 09:01:00 thuộc phút mới, dù lần xử lý lúc đó đang kết thúc dữ liệu phút cũ. Không tạo thêm một dòng cho mỗi trigger.

Dùng `upsert_minute_total(key, totals, update_version)` gán tổng tuyệt đối từ state bền vững, bỏ version cũ; retry giữ version và tổng. Mỗi phiên/bucket có một writer logic; checkpoint giữ thế hệ/version qua restart. Nếu triển khai delta thay thế, bắt buộc ledger batch_id để chỉ áp dụng một lần. Bucket không sự kiện vẫn có 0 khi coverage đủ, thiếu dữ liệu có trạng thái riêng.

Serving tại mốc phút t đọc tổng pickup_trip_count của [t−60 phút,t), bao gồm toàn bộ zone; giới hạn state 60 phút cộng buffer watermark. Phút đang mở là provisional riêng. PostgreSQL giữ toàn bộ lịch sử phút, không TTL 60 phút. Có thể tính snapshot dashboard bằng SUM bucket phút theo as_of, không bắt buộc thêm bảng rolling snapshot. Đến muộn trước final cập nhật bucket cũ; truy vấn dựng lại view bị ảnh hưởng từ dữ liệu đã cập nhật.

`upsert_stream_request_total` chỉ ghi request_count và baseline snapshot sau Kafka ack của cảnh báo nếu có. `upsert_stream_lifecycle_totals` chỉ ghi pickup/dropoff/completed/shared, không ghi đè request_count. Mỗi writer có version riêng, transaction ghép cột tại cùng khóa. Chốt row chỉ khi cả hai writer hoàn tất cùng phạm vi và coverage. Bản ghi phút của Stream 2 độc lập với barrier cảnh báo baseline.

## Hai loại cảnh báo

- Baseline: alert_type=baseline_crossing; replay_id, zone_id, window_start/end, request_count, conditions chứa alert_id/type/value/reference_windows/publication_version. ID ổn định theo phiên–zone–window–baseline_type. Kèm trend counts/gradients/K/version/series_end hoặc lý do thiếu. Điều kiện prev<=baseline<current; giữ snapshot bền vững. Kafka ack trước ghi request_count 15 phút tương ứng.
- Spike: alert_type=demand_spike; ID theo phiên–zone–interval_start, current_interval_start/end, previous_interval_start/end, current_count, previous_count, k, M, ratio nullable, reason, evaluated_at, input_cutoff. So hai khoảng 15 giây liên tiếp đủ dữ liệu, current>k*previous AND current>M. previous=0 thì ratio NULL/reason=increase_from_zero; thiếu coverage thì chưa đánh giá. State giữ bucket trong watermark, đánh giá lại nếu cập nhật nhưng không phát lại bucket đã cảnh báo. Payload giữ số liệu thực tế tại lần phát; không thu hồi cảnh báo khi dữ liệu muộn làm tỷ lệ đổi.

Consumer khử trùng alert_id. Không tuyên bố exactly-once xuyên Kafka/PostgreSQL; crash có thể phát lại cùng ID.

## Điều phối, pickup outbox và lịch trả

Cache Stream 3 không là nguồn quyết định bền vững. Kho giữ request theo (replay_id,request_id), state/version, hạn và consent_reference từ phía người dùng. Cặp đồng ý đã có trước khi điều phối; `commit_dispatch` xác minh bằng chứng cho hai thành viên và tính hợp lệ dưới khóa, không yêu cầu UI hỏi thêm lần nữa. Các giao diện find_candidates/reserve_pair, reject_pair, extend_request, start_solo và expire_requests vẫn giữ nguyên mục tiêu.

Cả hai bán kính <=500m, geography; không lọc theo zone. Khóa theo thứ tự ID để tránh deadlock, chốt duy nhất một trip cho request. Cùng transaction tạo trips/trip_requests, một pickup outbox event với request_ids và lịch trả từng người. Publisher Chính đọc outbox qua schedule_repository, phát pickup rồi acknowledge; crash sau gửi thì retry cùng event_id. Kho bảo đảm dropoff không được phát trước khi pickup có ack; consumer vẫn chịu được arrival khác thứ tự. trip_started_at=picked_up_at trong demo, due_at=trip_started_at+thời lượng nguồn từng request. Không quản lý xe rảnh/vehicle_id.

Từ chối cặp chuẩn hóa ID không thứ tự; hết hạn không ghi cặp bị từ chối. Hạn ban đầu 3 phút độc lập, không reset khi tạo đề xuất; gia hạn không giới hạn số lần với duration cấu hình. Không tự đi riêng khi im lặng. Các chuyển trạng thái dùng expected_version và idempotency_key; retry trả cùng trip, không tạo pickup hoặc lịch thứ hai.

## Archive, độ phủ và đối soát

Archive receipt gồm archive_id, HDFS path, scope/replay, event/journal IDs hoặc topic-partition-offset ranges, status committed. HDFS giữ đủ ba loại, journal và manifest coverage. Batch run khóa snapshot immutable để không thấy file đang ghi. Event sau chốt cần admission ledger/policy metadata (event ID, event time, ingest offset/time, watermark decision, reason) để batch áp dụng cùng tập được chấp nhận với stream; ledger chỉ xác định đầu vào, không cung cấp số đếm đối chứng.

Đối soát bằng full outer join batch/stream trên scope, zone, window_start/end của phiên bản/cutoff đã chọn; count thiếu khi coverage chưa đủ là pending, không mặc định 0. Chỉ equal/mismatch khi cả hai final và archive/consumer offsets xác nhận đủ. Hiển thị delta riêng của năm count, baseline snapshot/version, status và lý do. Baseline phần trăm NULL nếu thiếu/0; không mất zone chưa có lookup.

Ví dụ 5 request, một cặp ghép và ba trip riêng, tất cả đón/trả trong một zone/window: request=5, pickup_trip=4, dropoff_request=5, completed_trip=4, shared_trip=1 ở cả hai view. Pickup ở khung trước/dropoff ở khung sau cần state xuyên khung, không buộc mọi loại cùng tăng một lúc. Baseline là trung bình lịch sử, không yêu cầu bằng số request hiện tại.

## Retention

Chỉ dọn chi tiết vận hành kết thúc sau khi số liệu bền vững, raw/journal có receipt, lịch đã xử lý và dedup bảo vệ retry. Không dọn request chờ/gia hạn hoặc trip chưa xong. Retention này không áp dụng stream_minute_counts hay lịch sử batch/stream phân tích: PostgreSQL giữ toàn bộ theo yêu cầu hiện tại. Cache/serving chỉ giữ 60 phút không kéo theo xóa dữ liệu kho.
