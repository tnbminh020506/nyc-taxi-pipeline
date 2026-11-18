# Sơ đồ · API quản trị và nhận quyết định người dùng

**Phụ trách:** Mạnh phụ trách hợp đồng; nhóm phân công người triển khai. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

API/client điều phối, quản trị; tiếp nhận đồng ý đã có, không xây quy trình thương mại xin lại xác nhận.

## Hợp đồng dữ liệu và bàn giao

replay_id, request_id/proposal_id, consent_reference cho từng người, expected_version, idempotency_key; phản hồi trạng thái/version/trip hiện tại. Thao tác gia hạn, từ chối, solo vẫn nhận quyết định rõ ràng.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Xác minh bằng chứng và hạn dưới giao dịch kho, gọi commit_dispatch/start_solo; không tự phát pickup trước commit. Hai người đã đồng ý thì chốt ngay khi đủ điều kiện. Gia hạn duration cấu hình, không giới hạn số lần; im lặng không thành solo. Client đọc dashboard từ phút/rolling view và batch3, không quyết định ghép bằng baseline.

## Khu vực code

- [pipeline/service/api.py](../../../src/nyc_taxi_pipeline/pipeline/service/api.py)
- [pipeline/service/client.py](../../../src/nyc_taxi_pipeline/pipeline/service/client.py)

## Tiêu chí hoàn thành

Không hỏi lại đồng ý; lỗi consent hoặc version bị từ chối; retry cùng kết quả; gia hạn/solo/timeout chỉ một chuyển trạng thái hợp lệ.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
