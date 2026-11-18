# 3.2 · Giao dịch điều phối và pickup outbox

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Khóa cặp đã có đồng ý, tạo trip và sự kiện đón bền vững; cache không thay transaction.

## Hợp đồng dữ liệu và bàn giao

commit_dispatch(consent_references,expected_version,idempotency_key); trip và thành viên, một pickup outbox chứa request_ids, lịch dropoff từng người. Tọa độ đón đầu tiên chọn thứ tự phục vụ ổn định và lưu lại.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

ST_DWithin geography cho cả hai bán kính 500m, không lọc zone. Kiểm tra consent/hạn/state dưới khóa theo ID ổn định rồi ghi trip/members/outbox/schedule/journal cùng transaction. Solo cùng cơ chế một thành viên. Reject chuẩn hóa cặp, timeout khác từ chối; retry trả cùng trip. Không gọi Kafka trong transaction như thể có atomic commit xuyên hệ thống.

## Khu vực code

- [warehouse/matching_repository.py](../../../src/nyc_taxi_pipeline/warehouse/matching_repository.py)
- [warehouse/sql/operations/matching.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/operations/matching.sql)

## Tiêu chí hoàn thành

Đã có consent không hỏi lại; 2 request một trip/một pickup/hai lịch; crash/retry không tạo thêm; concurrent timeout/solo/ghép chỉ một kết quả.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
