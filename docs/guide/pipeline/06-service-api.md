# Sơ đồ · API và client xác nhận dịch vụ

**Phụ trách:** Mạnh phụ trách hợp đồng; người triển khai API/client do nhóm phân công. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Tách API/client khỏi Spark; định nghĩa thao tác và nối giao dịch của Bảo. Không triển khai kiểm thử/demo toàn tuyến 4.1–4.2 ở đây.

## Hợp đồng dữ liệu và bàn giao

Các thao tác đề xuất: xem proposal, ghi đồng ý từng request, xác nhận cặp, từ chối, gia hạn, chốt đi riêng. Payload có replay_id, request_id/proposal_id, idempotency_key, expected_version; phản hồi có trạng thái/version hiện tại, trip_id nếu đã chốt. Bằng chứng đồng ý gắn từng request.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

API kiểm tra hai đồng ý, trạng thái và hạn bằng đồng hồ chung rồi gọi transaction kho. Chốt chuyến và lịch trong cùng giao dịch, retry trả cùng trip. Từ chối lưu cặp không thứ tự; gia hạn không giới hạn số lần, thời lượng cấu hình chưa chốt. Im lặng không đồng ý và không tự tạo trip. Client chỉ hiển thị trạng thái và gửi quyết định, không quyết định ghép bằng baseline.

## Khu vực code

- [pipeline/service/api.py](../../../src/nyc_taxi_pipeline/pipeline/service/api.py)
- [pipeline/service/client.py](../../../src/nyc_taxi_pipeline/pipeline/service/client.py)

## Tiêu chí hoàn thành

Giao dịch nhận đủ bằng chứng hai người; gọi lại trả cùng kết quả; xác nhận quá hạn bị từ chối; gia hạn/đi riêng/timeout cạnh tranh chỉ một chuyển trạng thái có hiệu lực.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
