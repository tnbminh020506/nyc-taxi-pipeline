# Kiểm tra độ ổn định NYC API — 09/11/2026

## Định nghĩa trước khi chạy nghiệm thu

Nguồn là dataset `irx2-vgm4` thật; không dùng fixture. Mọi request trong phép đo
đều đi tới NYC, không đọc lại trang cache để tăng tỉ lệ thành công.

1. Đo ban đầu: 5 vòng, mỗi vòng 8 request v2 gửi nối tiếp, không retry; so sánh
   `$query` với các tham số `$select/$where/$order/$limit/$offset`. Có nghỉ 1 giây.
2. Thử v3 trực tiếp với POST đúng định dạng, ghi phản hồi nếu thiếu credential.
3. Sau sửa: 20 lượt pipeline mới trong 4 batch nối tiếp, mỗi batch 5 lượt.
   18 lượt đầu dùng các khoảng một phút khác nhau; hai lượt cuối dùng 15 phút,
   `page_size=100` để kiểm tra phân trang thực tế. Không chạy các batch song song.
4. Mỗi lượt phải tải đủ hai kỳ, giữ JSON, tạo Parquet, so khớp COUNT(*) độc lập,
   không thiếu/trùng ID nguồn và tạo đúng số sự kiện replay hợp lệ.

Tiêu chí cho **pipeline**: 20/20 lượt đạt, không request nào còn lỗi sau retry,
mỗi lượt một phút không quá 180 giây; lượt 15 phút không quá 600 giây. Các ngưỡng
này là cho phép thử nhỏ trên runner, không cam kết tốc độ tải cả năm.
Tỉ lệ thành công **ngay lần đầu** được báo riêng; không gọi máy chủ ổn định chỉ
vì retry đã phục hồi lỗi. Một mẫu 20 lượt không chứng minh xác suất 100% lâu dài.

## Kết quả ban đầu

[Phép đo gốc](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42629105763).

| Cách gửi v2 | Thành công ngay | 500 | 503 | Lỗi kết nối |
| --- | --- | --- | --- | --- |
| `$query` | 12/20 (60%) | 2 | 6 | 0 |
| Tham số rời | 8/20 (40%) | 0 | 11 | 1 |
| Tổng | 20/40 (50%) | 2 | 17 | 1 |

Cỡ mẫu nhỏ và khác thời điểm; không kết luận tham số rời luôn kém hơn. Mẫu này
không cho thấy đổi kiểu ghép query giải quyết lỗi. Giữ `$query` đã được kiểm chứng.
Phản hồi 503 có `errorCode=service-unavailable`; 500 có `internal-error` và mã
yêu cầu của Socrata. Đây là phản hồi dịch vụ, không phải JSON giả từ bộ mẫu.

V3 trên runner: HTTP 403, JSON `code=authentication_required`, thông báo
`This request must be authenticated or have an application token`.
Không có token nên chưa thể nghiệm thu v3 hợp lệ; không coi đây là lỗi có thể
loại bỏ bằng thay cú pháp hay bỏ kiểm tra token. Trên Windows, cả hai endpoint
trả HTML 403, khác với phản hồi xác thực JSON ở runner.

## Các thay đổi cần kiểm chứng

- Retry hữu hạn với khoảng chờ tối thiểu, backoff tăng dần, jitter và Retry-After.
  Cấu hình đo: tối đa 9 lần retry, bắt đầu 2 giây, backoff tối đa 12 giây cộng
  jitter tối đa 1 giây; request mới cách nhau tối thiểu 1 giây. 401/403 không retry.
- Ghi số request logic, lịch sử HTTP của từng lần retry, thời gian và lỗi cuối.
- Chỉ resume các lượt chưa ready có cùng nguồn/phạm vi/query/endpoint/page size.
  Trang đã lưu phải khớp checksum; trang hỏng được tải lại. Parquet được dựng lại
  từ đầu để không nối trùng dòng sau sự cố. Có `--no-resume` để tải mới hoàn toàn.
- Resume giả định dữ liệu nguồn không thay đổi trong một đợt tải; nếu nguồn đã
  cập nhật, chạy `--no-resume`. Không ghi ready khi chưa hết phạm vi.
- Phép nghiệm thu luôn tắt resume, kiểm tra ID nguồn và số dòng bằng API thật.

Kết quả sau sửa sẽ được ghi từ toàn bộ các batch, bao gồm cả lượt thất bại,
thời gian và số lần retry; không chọn riêng các lượt xanh để báo cáo.

## Đợt 1: chưa đạt tiêu chí 20/20

[Đủ 20 lượt](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42630619973),
commit `325e2a0`, cấu hình 9 retry, backoff tối đa 12 giây, chưa có ngân sách retry
toàn lời gọi. Kết quả: **19/20 lượt đạt**, 108/109 lời gọi API đạt sau retry;
60/109 thành công ngay (55,05%). Ghi nhận 93 phản hồi 503 và 7 phản hồi 500 trong
các lần thử. Lookup 20/20 được tách riêng, không cộng vào tỉ lệ taxi API.

Lượt 14 tải được dữ liệu nhưng COUNT(*) history thất bại sau 10 lần thử, khoảng
107 giây cho lời gọi đó. Hai lượt 15 phút kiểm tra nhiều trang đều đạt, lần lượt
1.575 và 1.605 dòng tổng hai kỳ, mất 202,504 và 265,280 giây.
Tổng số dòng đã đối chiếu COUNT(*) ở các lượt: 4.842 (bao gồm dữ liệu lặp lại khi đo).
Xem [JSON đầy đủ](evidence/api-campaign-1-2026-11-09.json).

## Kiểm tra giả thuyết tái dùng kết nối

[24 request, không retry](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42635690218):
keep-alive 8/12 đạt, Connection: close 8/12 đạt. Mẫu này không chứng minh đóng mọi
kết nối cải thiện tỉ lệ thành công; không bật mặc định chế độ đó.

## Đợt 2: sửa tầng phục hồi HTTP, giữ nguyên tiêu chí

- Một tầng retry ở ngoài bước đọc trọn response body, thay cho retry ẩn bên trong
  adapter. Có test thật bằng HTTP server ngắt body giữa chừng rồi phục hồi.
- Sau lỗi tạm thời, đóng pool để lần thử sau nối lại; vẫn giữ keep-alive khi thành công.
  Đây là biện pháp khôi phục kết nối, không khẳng định đã sửa được backend NYC.
- Tối đa 15 retry, ngân sách giữa các lần thử 120 giây, min delay 2 giây,
  backoff tối đa 6 giây cộng jitter 0–1 giây. Không retry auth/TLS; tôn trọng Retry-After.
- Đo lại 20 lượt mới, không cache; vẫn yêu cầu toàn bộ lượt, đối chiếu và thời gian
  đạt như tiêu chí ban đầu. Đợt 1 thất bại được giữ nguyên trong lịch sử.
