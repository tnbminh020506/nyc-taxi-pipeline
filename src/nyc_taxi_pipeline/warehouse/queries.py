"""Bước 3.3 & Truy vấn Điều hành Mở rộng | Bảo | Thực thi truy vấn phân tích bằng Python.

Cung cấp các hàm Python gọi trực tiếp các câu lệnh SQL trong ``database/queries/*.sql``:
1. ``compare_pickup_demand`` (Bước 3.3 - Core MVP): So sánh nhu cầu đón khách theo cửa sổ 15 phút.
2. ``query_dispatch_matrix``: Ma trận điều phối taxi (Xác định vùng thiếu xe SURGE & vùng còn điều phối được xe SURPLUS).
3. ``query_zone_and_borough_density``: Mật độ đón khách theo từng vùng (%) và tỷ trọng theo Quận (%).
4. ``query_borough_density_summary``: Bảng tổng hợp mật độ & sản lượng gộp theo Quận (Borough).
5. ``query_airport_and_transit_hubs``: Giám sát cửa ngõ Sân bay (JFK, LGA, EWR) & Nhà ga lớn.
6. ``query_trip_volume_summary``: Thống kê tổng sản lượng chuyến đi của phiên replay.
7. ``query_weekly_peak_hours``: Bản đồ nhiệt các khung giờ 15 phút cao điểm nhất trong tuần từ dữ liệu lịch sử.
8. ``query_time_series_trend``: Diễn biến biến động qua các khung 15 phút liên tiếp của phiên replay.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from nyc_taxi_pipeline.warehouse.db import DATABASE_DIR, managed_connection
from nyc_taxi_pipeline.warehouse.loader import _parse_iso_timestamp

QUERIES_DIR = DATABASE_DIR / "queries"


def _normalize_value(val: Any) -> Any:
    """Chuẩn hóa giá trị trả về từ cursor PostgreSQL sang kiểu Python thân thiện với JSON/Test."""
    if isinstance(val, Decimal):
        # Giữ nguyên số nguyên nếu phần thập phân bằng 0 (ví dụ 50.00 -> 50.0 hoặc float),
        # nhưng chuyển sang float để dễ serialize JSON và so sánh số học trong Python.
        return float(val)
    if isinstance(val, datetime):
        return val.isoformat()
    return val


def _execute_query_file(
    conn_or_dsn: Optional[Union[str, Any]],
    sql_filename: str,
    params: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    sql_path = QUERIES_DIR / sql_filename
    sql_text = sql_path.read_text(encoding="utf-8")
    with managed_connection(conn_or_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text, dict(params))
            col_names = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
    return [
        {col_names[i]: _normalize_value(row[i]) for i in range(len(col_names))}
        for row in rows
    ]


# ---------------------------------------------------------------------------
# 1. Truy vấn cốt lõi Bước 3.3 (Core MVP)
# ---------------------------------------------------------------------------
def compare_pickup_demand(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
    window_start: Union[str, datetime],
) -> List[Dict[str, Any]]:
    """Bước 3.3: So sánh số chuyến quan sát trong cửa sổ 15 phút với mức chuẩn lịch sử.

    Đầu ra mỗi dòng gồm đúng 7 cột hợp đồng:
    - ``pickup_zone_id``
    - ``zone_name`` (hiển thị mã vùng nếu thiếu tên trong lookup)
    - ``observed_trip_count``
    - ``baseline_trip_count``
    - ``reference_windows``
    - ``change_pct`` (100 * (observed - baseline) / baseline, hoặc None nếu thiếu/bằng 0)
    - ``change_unavailable_reason`` ('missing_baseline', 'zero_baseline' hoặc None)
    """
    ws_dt = _parse_iso_timestamp(window_start)
    return _execute_query_file(
        conn_or_dsn,
        "compare_pickup_demand.sql",
        {"replay_id": replay_id.strip(), "window_start": ws_dt.isoformat()},
    )


# ---------------------------------------------------------------------------
# 2. Truy vấn Ma trận Điều phối Taxi (Khu vực còn điều phối được xe vs Điểm nóng)
# ---------------------------------------------------------------------------
def query_dispatch_matrix(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
    window_start: Union[str, datetime],
) -> List[Dict[str, Any]]:
    """Phân loại khu vực thành SURGE_NEED_TAXIS, SURPLUS_CAN_REALLOCATE (còn điều phối được xe),
    BALANCED, NEW_ACTIVITY và gợi ý điều chuyển xe trong cùng Quận (borough).
    """
    ws_dt = _parse_iso_timestamp(window_start)
    return _execute_query_file(
        conn_or_dsn,
        "taxi_dispatch_matrix.sql",
        {"replay_id": replay_id.strip(), "window_start": ws_dt.isoformat()},
    )


# ---------------------------------------------------------------------------
# 3. Truy vấn Mật độ Taxi theo Khu vực & Quận (Zone & Borough Density)
# ---------------------------------------------------------------------------
def query_zone_and_borough_density(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
    window_start: Union[str, datetime],
) -> List[Dict[str, Any]]:
    """Tính tỷ trọng đón khách của từng vùng trên toàn thành phố (city_share_pct)
    và tỷ trọng trong Quận (zone_in_borough_share_pct).
    """
    ws_dt = _parse_iso_timestamp(window_start)
    return _execute_query_file(
        conn_or_dsn,
        "zone_and_borough_density.sql",
        {"replay_id": replay_id.strip(), "window_start": ws_dt.isoformat()},
    )


def query_borough_density_summary(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
    window_start: Union[str, datetime],
) -> List[Dict[str, Any]]:
    """Tổng hợp mật độ và tỷ trọng chuyến đón gộp theo từng Quận (Borough)."""
    zone_rows = query_zone_and_borough_density(conn_or_dsn, replay_id, window_start)
    if not zone_rows:
        return []
    city_total = int(zone_rows[0].get("city_total_trips") or 0)
    by_borough: Dict[str, Dict[str, Any]] = {}
    for row in zone_rows:
        b_name = str(row.get("borough") or "Unknown")
        entry = by_borough.setdefault(
            b_name,
            {
                "borough": b_name,
                "active_zones": 0,
                "borough_total_trips": 0,
                "city_total_trips": city_total,
                "borough_share_pct": 0.0,
            },
        )
        entry["active_zones"] += 1
        entry["borough_total_trips"] += int(row.get("observed_trip_count") or 0)

    result = list(by_borough.values())
    for item in result:
        if city_total > 0:
            item["borough_share_pct"] = round(100.0 * item["borough_total_trips"] / city_total, 2)
    result.sort(key=lambda x: (-x["borough_total_trips"], x["borough"]))
    return result


# ---------------------------------------------------------------------------
# 4. Truy vấn Cửa ngõ Giao thông, Sân bay & Nhà ga lớn
# ---------------------------------------------------------------------------
def query_airport_and_transit_hubs(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
    window_start: Union[str, datetime],
) -> List[Dict[str, Any]]:
    """Giám sát riêng lưu lượng tại các sân bay (JFK, LGA, EWR) và đầu mối giao thông trọng điểm."""
    ws_dt = _parse_iso_timestamp(window_start)
    return _execute_query_file(
        conn_or_dsn,
        "airport_and_transit_hubs.sql",
        {"replay_id": replay_id.strip(), "window_start": ws_dt.isoformat()},
    )


# ---------------------------------------------------------------------------
# 5. Truy vấn Tổng hợp Quy mô Sản lượng Chuyến đi của Phiên Replay
# ---------------------------------------------------------------------------
def query_trip_volume_summary(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
) -> List[Dict[str, Any]]:
    """Tổng hợp số cửa sổ đã quan sát, số vùng hoạt động, tổng số chuyến và cửa sổ cao điểm nhất."""
    return _execute_query_file(
        conn_or_dsn,
        "trip_volume_summary.sql",
        {"replay_id": replay_id.strip()},
    )


# ---------------------------------------------------------------------------
# 6. Truy vấn Bản đồ Nhiệt Khung giờ Cao điểm trong Tuần (Lịch sử)
# ---------------------------------------------------------------------------
def query_weekly_peak_hours(
    conn_or_dsn: Optional[Union[str, Any]],
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Tìm các khung giờ 15 phút trong tuần có tổng mức chuẩn lịch sử toàn thành phố cao nhất."""
    return _execute_query_file(
        conn_or_dsn,
        "weekly_peak_hours.sql",
        {"limit": int(limit)},
    )


# ---------------------------------------------------------------------------
# 7. Truy vấn Diễn biến Biến động theo Chuỗi Thời gian (Time-Series Trend)
# ---------------------------------------------------------------------------
def query_time_series_trend(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
    pickup_zone_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Lấy diễn biến số chuyến qua các cửa sổ 15 phút liên tiếp của phiên replay."""
    return _execute_query_file(
        conn_or_dsn,
        "time_series_trend.sql",
        {"replay_id": replay_id.strip(), "pickup_zone_id": pickup_zone_id},
    )


# ---------------------------------------------------------------------------
# Chạy trọn bộ báo cáo điều hành (Dashboard Bundle)
# ---------------------------------------------------------------------------
def run_all_dashboard_queries(
    conn_or_dsn: Optional[Union[str, Any]],
    replay_id: str,
    window_start: Union[str, datetime],
    limit: int = 10,
    trend_zone_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Thực thi tất cả các truy vấn (Core MVP + Điều phối + Mật độ + Tổng hợp) trong 1 lần gọi."""
    with managed_connection(conn_or_dsn) as conn:
        return {
            "parameters": {
                "replay_id": replay_id,
                "window_start": _parse_iso_timestamp(window_start).isoformat(),
            },
            "compare_pickup_demand": compare_pickup_demand(conn, replay_id, window_start),
            "dispatch_matrix": query_dispatch_matrix(conn, replay_id, window_start),
            "zone_density": query_zone_and_borough_density(conn, replay_id, window_start),
            "borough_density_summary": query_borough_density_summary(conn, replay_id, window_start),
            "airport_and_transit_hubs": query_airport_and_transit_hubs(conn, replay_id, window_start),
            "trip_volume_summary": query_trip_volume_summary(conn, replay_id),
            "weekly_peak_hours": query_weekly_peak_hours(conn, limit=limit),
            "time_series_trend": query_time_series_trend(conn, replay_id, pickup_zone_id=trend_zone_id),
        }


def format_table(rows: Sequence[Mapping[str, Any]], max_rows: Optional[int] = None) -> str:
    """Định dạng danh sách dict thành bảng Markdown dễ đọc trên Terminal và Báo cáo."""
    if not rows:
        return "(Không có dữ liệu trả về)"
    display_rows = list(rows[:max_rows]) if max_rows is not None and max_rows > 0 else list(rows)
    headers = list(display_rows[0].keys())
    widths = {h: len(str(h)) for h in headers}
    str_rows: List[Dict[str, str]] = []
    for r in display_rows:
        sr: Dict[str, str] = {}
        for h in headers:
            v = r.get(h)
            text = "NULL" if v is None else str(v)
            sr[h] = text
            if len(text) > widths[h]:
                widths[h] = len(text)
        str_rows.append(sr)

    header_line = "| " + " | ".join(h.ljust(widths[h]) for h in headers) + " |"
    sep_line = "| " + " | ".join("-" * widths[h] for h in headers) + " |"
    body_lines = [
        "| " + " | ".join(sr[h].ljust(widths[h]) for h in headers) + " |"
        for sr in str_rows
    ]
    footer = (
        f"\n... (Hiển thị {len(display_rows)}/{len(rows)} dòng)"
        if max_rows is not None and len(rows) > len(display_rows)
        else ""
    )
    return "\n".join([header_line, sep_line, *body_lines]) + footer
