# 3.2 · Snapshot và công bố kết quả batch

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Cung cấp giao diện đọc snapshot và công bố baseline/gradient cho Mạnh.

## Hợp đồng dữ liệu và bàn giao

Đọc cửa sổ chốt đủ độ phủ tại cutoff, cùng scope_id. Nhận staging baseline/gradient cùng batch_version; trả publication_version. Baseline thiếu mẫu được biểu diễn NULL/trạng thái thiếu, khác 0.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Kiểm tra duy nhất khóa, số mẫu, phạm vi và cutoff; công bố cặp baseline nguyên bộ trong transaction. Lưu phiên bản để replay quá khứ và đối soát; đọc theo applies_on và cutoff hợp lệ. Gradient có phiên bản/mốc riêng, không gán là tức thời. Công bố lại cùng version idempotent; lỗi rollback không đổi phiên bản đang đọc.

## Khu vực code

- [warehouse/batch_repository.py](../../../src/nyc_taxi_pipeline/warehouse/batch_repository.py)
- [warehouse/sql/load/baseline.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/baseline.sql)
- [warehouse/sql/load/gradients.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/load/gradients.sql)

## Tiêu chí hoàn thành

Stream không đọc lẫn hai lần chạy; staging lỗi không được công bố; snapshot không nhận cửa sổ chưa chốt; không cộng lại cùng history/phiên.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
