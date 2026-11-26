"""Read monthly TLC records in memory and report observed timestamp coverage."""

import logging
import platform
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .tlc import DEFAULT_URL, connect

LOG = logging.getLogger(__name__)
REQUIRED_COLUMNS = {"tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid"}


def _table(connection, query, parameters):
    result = connection.execute(query, parameters)
    names = [column[0] for column in result.description]
    return [dict(zip(names, row)) for row in result.fetchall()]


def inspect_month(connection, source, month, *, sample_rows=2):
    """Scan pickup timestamps, count other months explicitly, and show source rows."""
    columns = _table(connection, "DESCRIBE SELECT * FROM read_parquet(?)", [source])
    names = {item["column_name"].lower() for item in columns}
    missing = REQUIRED_COLUMNS - names
    if missing:
        raise ValueError(f"Missing source columns: {sorted(missing)}")
    periods = _table(connection, """
        SELECT strftime(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP), '%Y-%m') AS pickup_month,
               count(*) AS row_count,
               min(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP))::VARCHAR AS first_pickup,
               max(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP))::VARCHAR AS last_pickup
        FROM read_parquet(?)
        GROUP BY pickup_month
        ORDER BY pickup_month NULLS LAST
    """, [source])
    expected = next((item for item in periods if item["pickup_month"] == month), None)
    total = sum(item["row_count"] for item in periods)
    samples = _table(connection, """
        SELECT tpep_pickup_datetime::VARCHAR AS pickup_at,
               tpep_dropoff_datetime::VARCHAR AS dropoff_at,
               PULocationID AS pickup_zone_id, DOLocationID AS dropoff_zone_id
        FROM read_parquet(?)
        WHERE strftime(TRY_CAST(tpep_pickup_datetime AS TIMESTAMP), '%Y-%m') = ?
        LIMIT ?
    """, [source, month, sample_rows])
    in_month = expected["row_count"] if expected else 0
    return {
        "month": month, "source": source, "status": "present" if in_month else "missing_records",
        "source_rows": total, "expected_month_rows": in_month,
        "other_month_or_invalid_rows": total - in_month,
        "first_pickup_in_month": expected["first_pickup"] if expected else None,
        "last_pickup_in_month": expected["last_pickup"] if expected else None,
        "timestamp_months": periods, "samples": samples,
    }


def audit_months(years, months, *, root, source_directory=None, sample_rows=2):
    """Check every requested source and keep failures in the resulting evidence."""
    years, months = sorted(set(years)), sorted(set(months))
    if (not years or not months or any(type(year) is not int or not 2009 <= year <= 2100 for year in years)
            or any(type(month) is not int or not 1 <= month <= 12 for month in months)
            or len(years) * len(months) > 120):
        raise ValueError("Select 1..120 year/month combinations (years 2009..2100, months 1..12)")
    if type(sample_rows) is not int or not 1 <= sample_rows <= 5:
        raise ValueError("sample_rows must be 1..5")
    started = time.monotonic()
    remote = source_directory is None
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "source_mode": "remote_https" if remote else "existing_local_files",
        "platform": platform.platform(), "python": platform.python_version(), "duckdb": duckdb.__version__,
        "years": years, "months": months, "checks": [], "dataset_files_written": 0,
        "hdfs_publication_performed": False,
        "timestamp_basis": "TLC source wall time in America/New_York",
        "method": "scan pickup timestamps, group by actual year/month, then read sample records",
    }
    config = {"_root": Path(root), "duckdb": {"max_temp_directory_size": "0MB"}}
    try:
        with connect(config, remote=remote) as connection:
            connection.execute("SET enable_external_file_cache = false")
            if remote:
                connection.execute("SET http_timeout = 30")
                connection.execute("SET http_retries = 1")
            for year in years:
                for number in months:
                    month = f"{year:04d}-{number:02d}"
                    source = DEFAULT_URL.format(month=month) if remote else str(
                        Path(source_directory).resolve() / f"yellow_tripdata_{month}.parquet")
                    item_started = time.monotonic()
                    try:
                        check = inspect_month(connection, source, month, sample_rows=sample_rows)
                    except Exception as error:
                        check = {"month": month, "source": source, "status": "error",
                                 "error_type": type(error).__name__, "error": str(error)}
                    check["elapsed_seconds"] = round(time.monotonic() - item_started, 3)
                    report["checks"].append(check)
                    LOG.info("%s %s expected_month_rows=%s", month, check["status"], check.get("expected_month_rows"))
    except Exception as error:
        report["setup_error"] = {"type": type(error).__name__, "error": str(error)}
    report.update({
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "requested_month_count": len(years) * len(months),
        "present_month_count": sum(item["status"] == "present" for item in report["checks"]),
        "total_source_rows": sum(item.get("source_rows", 0) for item in report["checks"]),
        "total_expected_month_rows": sum(item.get("expected_month_rows", 0) for item in report["checks"]),
    })
    report["success"] = report["present_month_count"] == report["requested_month_count"]
    return report


def coverage_markdown(report):
    mode = report["source_mode"]
    lines = [
        "# Kết quả kiểm tra dữ liệu NYC theo năm và tháng", "",
        f"Lượt chạy bắt đầu {report['started_at']}, kết thúc {report['finished_at']}.",
        f"Nguồn kiểm tra là `{mode}`, trên `{report['platform']}`.",
        f"Đọc được bản ghi đúng kỳ cho **{report['present_month_count']}/{report['requested_month_count']} tháng**.",
        f"Tổng dòng nguồn **{report['total_source_rows']:,}**, dòng có pickup đúng tháng **{report['total_expected_month_rows']:,}**.",
        "", "DuckDB quét timestamp trong bộ nhớ và đọc các bản ghi mẫu. Lượt kiểm tra ghi số liệu báo cáo, "
        "với 0 tệp dataset mới và 0 lần công bố HDFS. HTTPS truyền các phần Parquet cần cho phép quét vào bộ nhớ. "
        "Độ phủ ở đây xác nhận có bản ghi đúng từng kỳ trong nguồn TLC. Các ngày, giờ và vùng thực tế "
        "cần được xét tiếp theo hợp đồng manifest khi chọn tập đầu vào batch/replay.", "",
        "| Tháng nguồn | Trạng thái | Tổng dòng | Đúng tháng | Lệch tháng / lỗi giờ | Pickup đầu trong tháng | Pickup cuối trong tháng |",
        "| --- | --- | ---: | ---: | ---: | --- | --- |",
    ]
    for item in report["checks"]:
        lines.append("| " + " | ".join(str(item.get(key, "—")) for key in (
            "month", "status", "source_rows", "expected_month_rows", "other_month_or_invalid_rows",
            "first_pickup_in_month", "last_pickup_in_month")) + " |")
    for item in report["checks"]:
        lines.extend(["", f"## {item['month']}", "", f"Nguồn được đọc là `{item['source']}`.", ""])
        if item["status"] == "error":
            lines.extend(["```text", item["error"], "```"])
            continue
        lines.extend([
            "| Pickup | Dropoff | Vùng đón | Vùng trả |",
            "| --- | --- | ---: | ---: |",
            *["| " + " | ".join(str(row[key]) for key in
               ("pickup_at", "dropoff_at", "pickup_zone_id", "dropoff_zone_id")) + " |" for row in item["samples"]],
            "", "Phân bố timestamp thực tế trong tệp nguồn.", "",
            "| Kỳ pickup | Số dòng |", "| --- | ---: |",
            *[f"| {period['pickup_month']} | {period['row_count']} |" for period in item["timestamp_months"]],
        ])
    if "setup_error" in report:
        lines.extend(["", "```text", report["setup_error"]["error"], "```"])
    lines.extend(["", "Nguồn chính thức [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).", ""])
    return "\n".join(lines)
