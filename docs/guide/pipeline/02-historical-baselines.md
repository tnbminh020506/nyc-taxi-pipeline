# 2.2 · Batch 1: hai baseline lịch sử

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Tính baseline nhu cầu ban đầu từ history; định kỳ từ lịch sử hợp lệ, không đọc stream counts làm đáp án đối chứng.

## Hợp đồng dữ liệu và bàn giao

Nhận snapshot nguồn/coverage và phạm vi lịch sử; staging all_days/same_weekday với request_sum, reference_windows, baseline_value, cutoff, applies_on, batch_version.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Cùng slot các ngày trước hoặc cùng thứ tuần trước; gồm khung 0 đủ dữ liệu, không lấy ngày áp dụng. Trung bình thập phân; thiếu mẫu NULL. Không trộn/chồng history và replay. Chỉ stage, trả trạng thái B1 cho tổng hợp cuối công bố sau ba nhánh.

## Khu vực code

- [pipeline/batch/historical_baseline.py](../../../src/nyc_taxi_pipeline/pipeline/batch/historical_baseline.py)

## Tiêu chí hoàn thành

Hai mức có mẫu số/truy vết; không dùng tương lai; retry không tăng mẫu; chưa tự công bố trước barrier tổng hợp.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
