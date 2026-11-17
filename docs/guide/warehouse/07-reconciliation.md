# 3.3 · SQL đối soát

**Phụ trách:** Bảo. **Trạng thái:** khung giao việc, chưa triển khai.

## Công việc và phạm vi phụ trách

Cung cấp truy vấn kiểm chứng từng đầu ra; cả nhóm tự ghép demo 4.1–4.2.

## Hợp đồng dữ liệu và bàn giao

Nhận scope_id/replay_id, vùng, cửa sổ hoặc request/trip ID. Trả số yêu cầu, hai baseline đã dùng/số mẫu/version, điều kiện vượt, gradient K và K−1 phần tử, khoảng cách/hạn/trạng thái ghép, chuỗi vòng đời và receipt HDFS.

Đối chiếu [hợp đồng chung](../contracts.md) và [thiết kế tổng thể](../giao-viec-demo-taxi.md).

## Hướng triển khai

Đối soát nhu cầu bằng snapshot cửa sổ, không join baseline hiện hành thay thế. Phần trăm = 100*(count-baseline)/baseline; 0/thiếu trả NULL kèm lý do. Left join lookup để giữ vùng chưa có tên. Truy vấn ghép hiển thị cả khoảng cách đón/đến; vòng đời nối request → proposal → trip → lịch → hoàn tất → archive → điều kiện dọn.

## Khu vực code

- [warehouse/sql/queries/compare_pickup_demand.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/compare_pickup_demand.sql)
- [warehouse/sql/queries/demand_trends.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/demand_trends.sql)
- [warehouse/sql/queries/ride_lifecycle.sql](../../../src/nyc_taxi_pipeline/warehouse/sql/queries/ride_lifecycle.sql)

## Tiêu chí hoàn thành

Đọc được các kết quả theo đúng phiên; giữ dòng thiếu lookup/baseline; baseline mới không đổi kết quả đối soát cũ; thấy đủ chuỗi gradient và dấu vết hoàn tất/lưu trữ.

Bàn giao schema/chữ ký gọi đã chốt, ví dụ đầu vào–đầu ra nhỏ và log kiểm tra riêng của module. Không coi việc có file khung là đã hoàn thành chức năng.
