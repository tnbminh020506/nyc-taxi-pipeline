-- Bước 3.2 | Bảo | Nạp Taxi Zone Lookup.
--
-- Đầu vào
-- - CSV lookup do Chính bàn giao qua mục zone_lookup có trạng thái ready ở bước 1.2.
-- - Tên cột thực tế và đường dẫn raw_path mà tiến trình nạp đọc được.
-- - Bảng taxi_zones từ database/schema.sql.
--
-- Đầu ra
-- - Một dòng cho mỗi pickup_zone_id, kèm zone_name và borough.
-- - Báo số dòng đã nạp và số dòng lỗi.
--
-- Yêu cầu
-- - Kiểm tra ánh xạ LocationID, Zone, Borough với file mẫu. Cập nhật theo pickup_zone_id.
-- - Nạp lại cùng lookup giữ nguyên số dòng và nội dung.

-- Mẫu câu lệnh UPSERT chuẩn dùng trong tiến trình nạp Python (warehouse/loader.py)
-- hoặc sau khi đọc từ bảng tạm staging_taxi_zones(location_id, borough, zone):
INSERT INTO taxi_zones (pickup_zone_id, zone_name, borough)
VALUES (%(pickup_zone_id)s, %(zone_name)s, %(borough)s)
ON CONFLICT (pickup_zone_id) DO UPDATE
SET
    zone_name = EXCLUDED.zone_name,
    borough   = EXCLUDED.borough;
