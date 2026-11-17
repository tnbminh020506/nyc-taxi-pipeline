# 2.2 · Batch 1: hai baseline

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Tạo demand_windows lịch sử và tính hai baseline, chạy khởi tạo và định kỳ 15 phút.

## Hợp đồng dữ liệu và bàn giao

Nhận history ready hoặc snapshot cửa sổ đã chốt/đủ độ phủ. Trả all_days và same_weekday theo vùng/slot, thứ khi áp dụng; request_sum, reference_windows, baseline_value thập phân, history_start/end, cutoff, applies_on, scope_id, batch_version.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

all_days chọn cùng slot các ngày trước; same_weekday chọn cùng thứ các tuần trước. Chỉ lấy ngày trước ngày áp dụng. Tính cả 0 có độ phủ; thiếu mẫu trả chưa xác định. Không trộn phiên hoặc chồng lặp history/replay. Ghi staging, gọi warehouse/batch_repository.py công bố nguyên bộ trong giao dịch. Replay quá khứ chọn phiên bản phù hợp, không dùng dữ liệu tương lai.

## Khu vực code

- [pipeline/batch/historical_baseline.py](../../../src/nyc_taxi_pipeline/pipeline/batch/historical_baseline.py)

## Tiêu chí hoàn thành

Hai loại có mẫu số/phạm vi truy vết được; không làm tròn trung bình thành số nguyên; nạp lại không tăng mẫu; stream không thấy nửa bộ; chốt cửa sổ chưa tự đổi baseline.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
