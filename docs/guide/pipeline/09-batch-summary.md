# 2.2 · Tổng hợp cuối sau ba batch

**Phụ trách:** Mạnh. **Trạng thái:** khung giao việc, chưa triển khai logic.

## Công việc và phạm vi phụ trách

Điều phối một run mỗi 15 phút; sau B1/B2/B3 mới tổng hợp số đếm và công bố PostgreSQL.

## Hợp đồng dữ liệu và bàn giao

Input immutable snapshot/cutoff/archive offsets/admission policy. Nhận ba trạng thái/staging; tạo batch_window_counts gồm request,pickup_trip,dropoff_request,completed_trip,shared_trip và metadata coverage/version.

Chi tiết khóa, schema và thời gian: [hợp đồng chung](../contracts.md). Luồng: [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Không suy số đếm từ trung bình/gradient và không sao chép stream counts. Dùng sự kiện chung của run để tính độc lập, state thành viên từ pickup trước cửa sổ nếu cần. Chờ ba nhánh thành công hoặc insufficient_data hợp lệ; lỗi thì không publish. Gọi publish_batch_run nguyên tử, retry cùng version. Khởi tạo baseline history trước replay, tách scope; batch định kỳ đọc sự kiện thực tế sau điều phối.

## Khu vực code

- [pipeline/batch/window_summary.py](../../../src/nyc_taxi_pipeline/pipeline/batch/window_summary.py)
- [pipeline/batch/orchestrator.py](../../../src/nyc_taxi_pipeline/pipeline/batch/orchestrator.py)

## Tiêu chí hoàn thành

Ví dụ 5/4/5/4/1 đúng; một nhánh lỗi không lộ nửa run; archive chưa đủ đánh pending; có thể đối soát độc lập mỗi count cùng cutoff.

Bàn giao ví dụ đầu vào–đầu ra và log kiểm tra module; không coi khung TODO là chức năng hoàn chỉnh. Kiểm thử/demo toàn tuyến 4.1–4.2 do cả nhóm tự thực hiện.
