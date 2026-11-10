# Bước 3.3 | Bảo | Điểm chạy truy vấn kho dữ liệu PostgreSQL bằng Python.
#
# Đầu vào
# - Chuỗi kết nối PostgreSQL (--dsn hoặc biến môi trường POSTGRES_DSN).
# - Loại truy vấn (--query: compare, dispatch, density, borough, hubs, volume, peaks, trend, all).
# - Tham số --replay-id và --window-start (mốc thời gian ISO 8601 có múi giờ).
#
# Đầu ra
# - Bảng kết quả Markdown hoặc JSON trên stdout (và lưu ra file nếu truyền --output).
"""Thực thi các truy vấn phân tích nhu cầu & điều phối taxi trên PostgreSQL bằng Python.

Ví dụ chạy truy vấn bắt buộc Bước 3.3 (compare_pickup_demand)::

    python scripts/query_warehouse.py \
        --query compare \
        --replay-id taxi-fixture-001 \
        --window-start 2025-01-20T08:00:00-05:00

Ví dụ chạy toàn bộ 7 truy vấn điều hành (bao gồm vùng còn điều phối được xe, mật độ vùng/quận...)::

    python scripts/query_warehouse.py \
        --query all \
        --replay-id taxi-fixture-001 \
        --window-start 2025-01-20T08:00:00-05:00

Nếu muốn tự động khởi tạo bảng + nạp bộ dữ liệu mẫu Bước 4.1 rồi chạy truy vấn kiểm tra ngay::

    python scripts/query_warehouse.py --seed-fixture --query all
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.warehouse import (  # noqa: E402
    compare_pickup_demand,
    format_table,
    get_dsn,
    init_schema,
    load_baseline,
    load_zone_lookup,
    query_airport_and_transit_hubs,
    query_borough_density_summary,
    query_dispatch_matrix,
    query_time_series_trend,
    query_trip_volume_summary,
    query_weekly_peak_hours,
    query_zone_and_borough_density,
    run_all_dashboard_queries,
    upsert_replay_counts,
)

QUERY_CHOICES = (
    "compare",
    "dispatch",
    "density",
    "borough",
    "hubs",
    "volume",
    "peaks",
    "trend",
    "all",
)

DEFAULT_FIXTURE_REPLAY_ID = "taxi-fixture-001"
DEFAULT_FIXTURE_WINDOW_START = "2025-01-20T08:00:00-05:00"


def seed_demo_fixture(dsn: str) -> Dict[str, Any]:
    """Khởi tạo 3 bảng và nạp bộ mẫu nghiệm thu Bước 4.1 (kèm các ca biên) vào PostgreSQL."""
    init_schema(dsn)

    zones = [
        {"pickup_zone_id": 161, "zone_name": "Midtown Center", "borough": "Manhattan"},
        {"pickup_zone_id": 230, "zone_name": "Times Sq/Theatre District", "borough": "Manhattan"},
        {"pickup_zone_id": 132, "zone_name": "JFK Airport", "borough": "Queens"},
        {"pickup_zone_id": 138, "zone_name": "LaGuardia Airport", "borough": "Queens"},
    ]
    baselines = [
        # Vùng A = 161 (Bộ mẫu 4.1: thứ Hai iso_weekday=1, slot 08:00-08:15 = 32, baseline = 2, reference_windows = 2)
        {
            "pickup_zone_id": 161,
            "iso_weekday": 1,
            "slot_15m": 32,
            "baseline_trip_count": "2",
            "reference_windows": 2,
            "history_start": "2025-01-06T08:00:00-05:00",
            "history_end": "2025-01-13T08:15:00-05:00",
        },
        # Vùng 230 (Times Sq): lịch sử 10 chuyến, thực tế giảm còn 4 (-60% -> SURPLUS_CAN_REALLOCATE: còn điều phối được xe)
        {
            "pickup_zone_id": 230,
            "iso_weekday": 1,
            "slot_15m": 32,
            "baseline_trip_count": "10",
            "reference_windows": 2,
            "history_start": "2025-01-06T08:00:00-05:00",
            "history_end": "2025-01-13T08:15:00-05:00",
        },
        # Vùng 132 (JFK Airport): baseline = 0 để kiểm tra ca zero_baseline
        {
            "pickup_zone_id": 132,
            "iso_weekday": 1,
            "slot_15m": 32,
            "baseline_trip_count": "0",
            "reference_windows": 2,
            "history_start": "2025-01-06T08:00:00-05:00",
            "history_end": "2025-01-13T08:15:00-05:00",
        },
    ]
    replay_rows = [
        # Cửa sổ 08:00 - 08:15
        {
            "replay_id": DEFAULT_FIXTURE_REPLAY_ID,
            "pickup_zone_id": 161,
            "window_start": "2025-01-20T08:00:00-05:00",
            "window_end": "2025-01-20T08:15:00-05:00",
            "trip_count": 3,  # +50% so với baseline 2
        },
        {
            "replay_id": DEFAULT_FIXTURE_REPLAY_ID,
            "pickup_zone_id": 230,
            "window_start": "2025-01-20T08:00:00-05:00",
            "window_end": "2025-01-20T08:15:00-05:00",
            "trip_count": 4,  # -60% so với baseline 10 -> Khu vực dư xe (SURPLUS_CAN_REALLOCATE)
        },
        {
            "replay_id": DEFAULT_FIXTURE_REPLAY_ID,
            "pickup_zone_id": 132,
            "window_start": "2025-01-20T08:00:00-05:00",
            "window_end": "2025-01-20T08:15:00-05:00",
            "trip_count": 5,  # baseline = 0 -> zero_baseline
        },
        {
            "replay_id": DEFAULT_FIXTURE_REPLAY_ID,
            "pickup_zone_id": 999,  # Vùng chưa có tên trong taxi_zones & chưa có baseline -> missing_baseline
            "window_start": "2025-01-20T08:00:00-05:00",
            "window_end": "2025-01-20T08:15:00-05:00",
            "trip_count": 2,
        },
        # Cửa sổ 08:15 - 08:30 (1 chuyến cho vùng A = 161 theo đúng đặc tả 4.1)
        {
            "replay_id": DEFAULT_FIXTURE_REPLAY_ID,
            "pickup_zone_id": 161,
            "window_start": "2025-01-20T08:15:00-05:00",
            "window_end": "2025-01-20T08:30:00-05:00",
            "trip_count": 1,
        },
    ]

    z_res = load_zone_lookup(dsn, zones)
    b_res = load_baseline(dsn, baselines)
    r_res = upsert_replay_counts(dsn, replay_rows)
    return {"zones": z_res, "baseline": b_res, "replay_counts": r_res}


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bước 3.3: Truy vấn kho dữ liệu PostgreSQL bằng Python")
    p.add_argument(
        "--dsn",
        help="Chuỗi kết nối PostgreSQL (mặc định đọc từ biến môi trường POSTGRES_DSN)",
    )
    p.add_argument(
        "--query",
        choices=QUERY_CHOICES,
        default="compare",
        help=(
            "Loại truy vấn cần chạy: "
            "compare (Bước 3.3 Core MVP), "
            "dispatch (Ma trận điều phối & khu vực còn điều phối được xe), "
            "density (Mật độ theo vùng), "
            "borough (Tỷ trọng theo Quận), "
            "hubs (Sân bay & nhà ga), "
            "volume (Tổng sản lượng chuyến), "
            "peaks (Giờ cao điểm lịch sử trong tuần), "
            "trend (Chuỗi thời gian 15p), "
            "all (Chạy tất cả báo cáo)"
        ),
    )
    p.add_argument(
        "--replay-id",
        default=DEFAULT_FIXTURE_REPLAY_ID,
        help=f"Mã phiên replay cần xem (mặc định: {DEFAULT_FIXTURE_REPLAY_ID})",
    )
    p.add_argument(
        "--window-start",
        default=DEFAULT_FIXTURE_WINDOW_START,
        help=f"Mốc đầu cửa sổ 15 phút có múi giờ (mặc định: {DEFAULT_FIXTURE_WINDOW_START})",
    )
    p.add_argument("--zone-id", type=int, help="Lọc theo mã vùng (dùng cho truy vấn trend)")
    p.add_argument("--limit", type=int, default=10, help="Giới hạn số dòng cho truy vấn peaks (mặc định 10)")
    p.add_argument(
        "--format",
        choices=("table", "json"),
        default="table",
        help="Định dạng hiển thị kết quả: table (Bảng Markdown) hoặc json (mặc định: table)",
    )
    p.add_argument("--output", help="Đường dẫn file để lưu kết quả truy vấn (JSON hoặc Markdown)")
    p.add_argument(
        "--seed-fixture",
        action="store_true",
        help="Tự động khởi tạo bảng và nạp dữ liệu mẫu Bước 4.1 trước khi chạy truy vấn",
    )
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    dsn = get_dsn(args.dsn)

    if args.seed_fixture:
        seed_demo_fixture(dsn)

    if args.query == "compare":
        result: Any = compare_pickup_demand(dsn, args.replay_id, args.window_start)
        title = f"1. SO SÁNH NHU CẦU ĐÓN KHÁCH (CORE MVP 3.3) | replay_id={args.replay_id} | window_start={args.window_start}"
    elif args.query == "dispatch":
        result = query_dispatch_matrix(dsn, args.replay_id, args.window_start)
        title = f"2. MA TRẬN ĐIỀU PHỐI TAXI (ĐIỂM NÓNG & VÙNG CÒN ĐIỀU PHỐI ĐƯỢC XE) | window_start={args.window_start}"
    elif args.query == "density":
        result = query_zone_and_borough_density(dsn, args.replay_id, args.window_start)
        title = f"3. MẬT ĐỘ TAXI THEO KHU VỰC (%) | window_start={args.window_start}"
    elif args.query == "borough":
        result = query_borough_density_summary(dsn, args.replay_id, args.window_start)
        title = f"4. TỶ TRỌNG & MẬT ĐỘ TAXI THEO QUẬN (BOROUGH SHARE %) | window_start={args.window_start}"
    elif args.query == "hubs":
        result = query_airport_and_transit_hubs(dsn, args.replay_id, args.window_start)
        title = f"5. GIÁM SÁT SÂN BAY & CỬA NGÕ GIAO THÔNG | window_start={args.window_start}"
    elif args.query == "volume":
        result = query_trip_volume_summary(dsn, args.replay_id)
        title = f"6. TỔNG HỢP SẢN LƯỢNG CHUYẾN ĐI CỦA PHIÊN | replay_id={args.replay_id}"
    elif args.query == "peaks":
        result = query_weekly_peak_hours(dsn, limit=args.limit)
        title = f"7. TOP {args.limit} KHUNG GIỜ CAO ĐIỂM LỊCH SỬ TRONG TUẦN"
    elif args.query == "trend":
        result = query_time_series_trend(dsn, args.replay_id, pickup_zone_id=args.zone_id)
        title = f"8. DIỄN BIẾN CHUỖI THỜI GIAN 15 PHÚT | replay_id={args.replay_id}"
    else:
        result = run_all_dashboard_queries(
            dsn,
            args.replay_id,
            args.window_start,
            limit=args.limit,
            trend_zone_id=args.zone_id,
        )
        title = "BÁO CÁO TỔNG HỢP TRUNG TÂM ĐIỀU HÀNH TAXI NYC"

    if args.format == "json":
        output_text = json.dumps(result, ensure_ascii=False, indent=2)
    else:
        if args.query == "all" and isinstance(result, dict):
            sections = [f"# {title}\n"]
            section_titles = [
                ("compare_pickup_demand", "1. So sánh nhu cầu đón khách (Bước 3.3 - Core MVP)"),
                ("dispatch_matrix", "2. Ma trận điều phối taxi (Điểm nóng & Khu vực còn điều phối được xe)"),
                ("zone_density", "3. Mật độ taxi theo từng Khu vực (%)"),
                ("borough_density_summary", "4. Tỷ trọng mật độ theo Quận (Borough Share %)"),
                ("airport_and_transit_hubs", "5. Giám sát Sân bay & Cửa ngõ giao thông"),
                ("trip_volume_summary", "6. Tổng sản lượng chuyến đi của phiên Replay"),
                ("weekly_peak_hours", "7. Khung giờ cao điểm lịch sử trong tuần"),
                ("time_series_trend", "8. Diễn biến biến động qua các khung 15 phút"),
            ]
            for key, sec_title in section_titles:
                rows = result.get(key, [])
                sections.append(f"## {sec_title}\n\n{format_table(rows)}\n")
            output_text = "\n".join(sections)
        else:
            output_text = f"### {title}\n\n{format_table(result)}"

    print(output_text)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output_text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())

