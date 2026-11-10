# Nghiệm thu HDFS

Đã triển khai publisher WebHDFS, snapshot manifest bất biến, giữ nguyên Parquet nguồn, kiểm tra SHA-256 và hai replica; replay đọc shared manifest từ HDFS trước khi phát Kafka. Unit cục bộ đã đạt; cụm thật đang chờ CI vì máy Windows chưa có Docker/WSL.

Báo cáo này sẽ được cập nhật bằng JSON từ job HDFS thật. Không coi mock hoặc bằng chứng DuckDB trước đây là nghiệm thu phân tán.
