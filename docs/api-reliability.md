> Báo cáo lịch sử API. Từ 10/11/2026 hướng chính chuyển sang [DuckDB/TLC](ingestion.md) theo nhóm trưởng; kết quả dưới đây không phải nghiệm thu nguồn mới.

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

Kết quả bên dưới được tổng hợp từ toàn bộ các batch, bao gồm cả lượt thất bại,
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

### Kết quả đợt 2: đạt tiêu chí của phép đo

[Lượt đo đủ 20 lần](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42637243338),
commit `ab95694`, GitHub-hosted Ubuntu, 09/11/2026.

| Chỉ tiêu | Kết quả |
| --- | --- |
| Pipeline mới, không dùng trang cache | **20/20 đạt** |
| Lời gọi taxi API hoàn tất sau phục hồi | **110/110 (100%)** |
| Thành công ngay lần đầu | **61/110 (55,45%)** |
| Lỗi từng lần thử đã phục hồi | 76 × 503, 4 × 500, 3 × ReadTimeout |
| Lookup (tách riêng) | 20/20 |
| Dòng Parquet đối chiếu với COUNT(*) | 4.936, có lặp dữ liệu giữa các lượt đo |
| Trung vị thời gian một lời gọi API, gồm retry | 13,471 giây |
| P95 thời gian một lời gọi API | 38,285 giây |
| Trung vị toàn lượt | 64,251 giây |
| Lượt một phút nhanh/chậm nhất | 32,805 / 158,740 giây |
| Hai lượt 15 phút, page_size=100 | 239,665 / 363,136 giây |

Mỗi lượt so số dòng JSON/Parquet với COUNT(*) độc lập, kiểm tra ID nguồn không
trùng/thiếu và tạo đúng số event replay hợp lệ. Các lượt một phút đều dưới 180
giây, hai lượt 15 phút dưới 600 giây, đúng ngưỡng đã đặt trước. Chạy nối tiếp,
không thay lỗi bằng dữ liệu mẫu hoặc đọc lại trang từ lượt trước.
Xem [toàn bộ số liệu và lịch sử retry](evidence/api-campaign-2-2026-11-09.json).

**Kết luận:** tầng phục hồi của pipeline đạt tiêu chí trong mẫu 20 lượt này.
Nguồn NYC vẫn bất ổn ở lần gọi đầu; không gọi đây là API không còn lỗi, không
cam kết 100% cho các lần chạy tương lai. Hai đợt đo diễn ra khác thời điểm,
không đủ để kết luận mọi khác biệt tỉ lệ là do thay đổi code.

Gọi trực tiếp từ môi trường Windows này vẫn chưa đạt: phép thử có bộ lọc cuối
cùng nhận HTTP 403 sau 15,719 giây và không retry lỗi quyền truy cập. Luồng đã
nghiệm thu là GitHub Actions → tải artifact → kiểm tra/import → replay cục bộ.
Đây là đường lấy dữ liệu sử dụng được hiện tại, không phải bằng chứng đã sửa
được chính sách truy cập của NYC trên mạng máy này. V3 còn cần credential hợp lệ.

## Mẫu tải dùng thực tế với trang lớn hơn

[Mẫu demo 15 phút](https://github.com/tnbminh020506/nyc-taxi-pipeline/actions/runs/42642938761),
`configs/api-demo.example.yaml`, page_size=1000, cả hai kỳ 08:00–08:15 giờ New York:

- History 06/01/2025: **1.029 dòng**, 2 trang; replay 20/01/2025: **484 dòng**, 1 trang.
- Lookup 265 vùng. Hai COUNT(*) khớp Parquet; không có dòng lỗi, tạo 484 event replay.
- Hoàn tất **60,749 giây**: 5 lời gọi API logic, 6 lần thử thực tế.
- Đã tải về Windows, kiểm tra checksum và tạo lại 484 event_id giống bản trên runner.
  Dùng `scripts/import_api_artifact.py` để ánh xạ manifest mà không gọi lại NYC.

[Báo cáo mẫu demo](evidence/api-demo-2026-11-09.json). Thời gian này chỉ là một mẫu
với trang lớn và phạm vi khác nhẹ so với hai ca stress; không phải phép chứng minh
speedup có kiểm soát. Trang lớn giảm số lần gọi cần thiết, nhưng chưa benchmark
tải cả năm. Khi mở rộng khoảng dữ liệu, vẫn cần giới hạn trang, resume và chấp nhận
nguồn có thể chậm/gián đoạn. Sau khi tải xong, chạy batch/replay từ file đã lưu.

## Cách chạy lại phép đo

```powershell
gh workflow run api-reliability.yml --ref demo/ingestion -f mode=acceptance
# Sau khi hoàn tất, tải đủ 4 artifact rồi tổng hợp (không chọn riêng batch xanh):
gh run download <RUN_ID> --dir output/reliability-<RUN_ID>
python scripts/summarize_api_reliability.py output/reliability-<RUN_ID>
```

Các workflow gọi NYC là thủ công, cùng một nhóm concurrency để tránh chạy dồn.
Artifact giữ 14 ngày cho phép đo độ ổn định; JSON tổng hợp ở `docs/evidence/` được
commit để giữ lại kết quả khi artifact hết hạn. Source JSON/Parquet nằm trong
artifact và bản tải cục bộ, không đưa toàn bộ dữ liệu phát sinh vào Git.
