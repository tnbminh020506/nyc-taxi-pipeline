# Bước 3.3 | Bảo | Standalone Demo Runner không cần PostgreSQL bên ngoài (dùng DuckDB in-memory).
#
# Mục đích:
# - Tự sinh dữ liệu ngẫu nhiên (synthetic/random data) theo đúng 100% schema và các ràng buộc của 3 bảng.
# - Chạy trực tiếp toàn bộ 8 câu lệnh SQL phân tích trong database/queries/.
# - In ra màn hình bảng kết quả Markdown và lưu vào file báo cáo để kiểm chứng logic.
"""Standalone Demo: Tự sinh dữ liệu ngẫu nhiên và chạy toàn bộ truy vấn kho dữ liệu."""
from __future__ import annotations

import argparse
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
DATABASE_DIR = REPO_ROOT / "database"
QUERIES_DIR = DATABASE_DIR / "queries"
SRC_DIR = REPO_ROOT / "src"

def format_table(rows: List[Dict[str, Any]], max_rows: int = 10) -> str:
    """Định dạng danh sách dict thành bảng Markdown dễ đọc."""
    if not rows:
        return "(Không có dữ liệu trả về)"
    display_rows = rows[:max_rows]
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
        if len(rows) > len(display_rows)
        else ""
    )
    return "\n".join([header_line, sep_line, *body_lines]) + footer

SAMPLE_BOROUGHS = ["Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"]
SAMPLE_ZONES = [
    (161, "Midtown Center", "Manhattan"),
    (230, "Times Sq/Theatre District", "Manhattan"),
    (132, "JFK Airport", "Queens"),
    (138, "LaGuardia Airport", "Queens"),
    (61, "Crown Heights North", "Brooklyn"),
    (79, "East Village", "Manhattan"),
    (142, "Lincoln Square East", "Manhattan"),
    (237, "Upper East Side South", "Manhattan"),
    (48, "Clinton East", "Manhattan"),
    (1, "Newark Airport", "EWR"),
    (24, "Bloomingdale", "Manhattan"),
    (43, "Central Park", "Manhattan"),
]


def generate_synthetic_data(
    num_zones: int = 15,
    seed: int = 42,
    replay_id: str = "demo-replay-test-01",
    base_time_str: str = "2025-01-20T08:00:00-05:00",
) -> Dict[str, List[Dict[str, Any]]]:
    """Tự động sinh ngẫu nhiên dữ liệu cho 3 bảng đúng 100% schema và các ràng buộc."""
    random.seed(seed)
    # Parse base timestamp
    dt = datetime.fromisoformat(base_time_str)

    # 1. Sinh bảng taxi_zones
    zones: List[Dict[str, Any]] = []
    used_ids = set()
    for zid, zname, bname in SAMPLE_ZONES[:num_zones]:
        zones.append({"pickup_zone_id": zid, "zone_name": zname, "borough": bname})
        used_ids.add(zid)

    while len(zones) < num_zones:
        zid = random.randint(2, 265)
        if zid not in used_ids:
            used_ids.add(zid)
            zones.append({
                "pickup_zone_id": zid,
                "zone_name": f"Zone {zid}",
                "borough": random.choice(SAMPLE_BOROUGHS),
            })

    # 2. Sinh bảng historical_baseline
    # Cửa sổ 08:00 - 08:15 (slot 32, thứ Hai iso_weekday = 1)
    baselines: List[Dict[str, Any]] = []
    h_start = "2025-01-06T08:00:00-05:00"
    h_end = "2025-01-13T08:15:00-05:00"
    for z in zones:
        zid = z["pickup_zone_id"]
        # Cố ý để 1 vùng baseline = 0 và 1 vùng không có baseline để test ca biên
        if zid == 132:
            base_count = 0.0
        elif zid == 24:
            continue  # Bỏ qua để tạo ca missing_baseline
        else:
            base_count = round(random.uniform(5.0, 50.0), 2)

        baselines.append({
            "pickup_zone_id": zid,
            "iso_weekday": 1,
            "slot_15m": 32,
            "baseline_trip_count": base_count,
            "reference_windows": random.randint(2, 4),
            "history_start": h_start,
            "history_end": h_end,
        })

    # 3. Sinh bảng replay_pickup_counts (gồm 3 khung 15 phút liên tiếp: 08:00, 08:15, 08:30)
    replays: List[Dict[str, Any]] = []
    for step in range(3):
        win_start = dt + timedelta(minutes=15 * step)
        win_end = win_start + timedelta(minutes=15)
        w_start_str = win_start.isoformat()
        w_end_str = win_end.isoformat()

        for z in zones:
            zid = z["pickup_zone_id"]
            # Tạo độ lệch có chủ ý: có vùng tăng mạnh (+50%), có vùng giảm mạnh (-60%)
            if zid == 161 and step == 0:
                trip_cnt = 30  # Giả sử baseline là 20 -> tăng 50% (SURGE)
            elif zid == 230 and step == 0:
                trip_cnt = 4   # Giả sử baseline là 15 -> giảm (SURPLUS - còn điều phối được)
            else:
                trip_cnt = random.randint(0, 60)

            replays.append({
                "replay_id": replay_id,
                "pickup_zone_id": zid,
                "window_start": w_start_str,
                "window_end": w_end_str,
                "trip_count": trip_cnt,
            })

    # Thêm 1 bản ghi có mã vùng 999 chưa có tên trong taxi_zones để test fallback ID
    replays.append({
        "replay_id": replay_id,
        "pickup_zone_id": 999,
        "window_start": dt.isoformat(),
        "window_end": (dt + timedelta(minutes=15)).isoformat(),
        "trip_count": 8,
    })

    return {"taxi_zones": zones, "historical_baseline": baselines, "replay_pickup_counts": replays}


def run_standalone_demo(
    num_zones: int = 12,
    seed: int = 42,
    replay_id: str = "demo-replay-test-01",
    window_start: str = "2025-01-20T08:00:00-05:00",
) -> str:
    """Khởi tạo DuckDB, load schema, nạp dữ liệu ngẫu nhiên và chạy toàn bộ queries."""
    con = duckdb.connect(database=":memory:")

    # 1. Tạo 3 bảng
    con.execute("""
        CREATE TABLE taxi_zones (
            pickup_zone_id INTEGER PRIMARY KEY,
            zone_name VARCHAR,
            borough VARCHAR
        );
        CREATE TABLE historical_baseline (
            pickup_zone_id INTEGER,
            iso_weekday INTEGER,
            slot_15m INTEGER,
            baseline_trip_count DOUBLE,
            reference_windows INTEGER,
            history_start TIMESTAMPTZ,
            history_end TIMESTAMPTZ,
            PRIMARY KEY (pickup_zone_id, iso_weekday, slot_15m)
        );
        CREATE TABLE replay_pickup_counts (
            replay_id VARCHAR,
            pickup_zone_id INTEGER,
            window_start TIMESTAMPTZ,
            window_end TIMESTAMPTZ,
            trip_count BIGINT,
            PRIMARY KEY (replay_id, pickup_zone_id, window_start)
        );
    """)

    # 2. Nạp dữ liệu sinh ngẫu nhiên
    data = generate_synthetic_data(num_zones=num_zones, seed=seed, replay_id=replay_id, base_time_str=window_start)
    for z in data["taxi_zones"]:
        con.execute("INSERT INTO taxi_zones VALUES (?, ?, ?)", [z["pickup_zone_id"], z["zone_name"], z["borough"]])
    for b in data["historical_baseline"]:
        con.execute(
            "INSERT INTO historical_baseline VALUES (?, ?, ?, ?, ?, ?, ?)",
            [b["pickup_zone_id"], b["iso_weekday"], b["slot_15m"], b["baseline_trip_count"], b["reference_windows"], b["history_start"], b["history_end"]]
        )
    for r in data["replay_pickup_counts"]:
        con.execute(
            "INSERT INTO replay_pickup_counts VALUES (?, ?, ?, ?, ?)",
            [r["replay_id"], r["pickup_zone_id"], r["window_start"], r["window_end"], r["trip_count"]]
        )

    # 3. Chạy từng truy vấn và gom kết quả thành Markdown report
    report_lines = [
        "# KẾT QUẢ TEST STANDALONE KHO DỮ LIỆU POSTGRESQL (TỰ SINH RANDOM DATA)",
        f"> **Thời điểm chạy test:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"> **Phiên replay:** `{replay_id}` | **Cửa sổ khảo sát:** `{window_start}`",
        f"> **Số lượng vùng khảo sát ngẫu nhiên:** {num_zones} khu vực\n",
        "---",
    ]

    # Map các file truy vấn và cách chạy
    query_files = [
        ("1. BÁO CÁO CỐT LÕI (CORE MVP 3.3): SO SÁNH NHU CẦU ĐÓN KHÁCH", "compare_pickup_demand.sql"),
        ("2. MA TRẬN ĐIỀU PHỐI TAXI (ĐIỂM NÓNG SURGE & VÙNG CÒN XE ĐIỀU PHỐI SURPLUS)", "taxi_dispatch_matrix.sql"),
        ("3. MẬT ĐỘ ĐÓN KHÁCH THEO TỪNG VÙNG (%)", "zone_and_borough_density.sql"),
        ("4. GIÁM SÁT SÂN BAY & CỬA NGÕ HUYẾT MẠCH", "airport_and_transit_hubs.sql"),
        ("5. TỔNG HỢP SẢN LƯỢNG CHUYẾN ĐI CỦA PHIÊN REPLAY", "trip_volume_summary.sql"),
        ("6. DIỄN BIẾN CHUỖI THỜI GIAN QUA CÁC CỬA SỔ 15 PHÚT", "time_series_trend.sql"),
        ("7. TOP KHUNG GIỜ CAO ĐIỂM LỊCH SỬ TRONG TUẦN", "weekly_peak_hours.sql"),
    ]

    for title, fname in query_files:
        sql_path = QUERIES_DIR / fname
        raw_sql = sql_path.read_text(encoding="utf-8")

        # Thay thế cú pháp biến %(name)s của psycopg2 sang giá trị hoặc ? của DuckDB để test
        adapted_sql = raw_sql.replace("%(replay_id)s", f"'{replay_id}'")
        adapted_sql = adapted_sql.replace("%(window_start)s", f"'{window_start}'")
        adapted_sql = adapted_sql.replace("%(pickup_zone_id)s", "NULL")
        adapted_sql = adapted_sql.replace("%(limit)s", "5")
        adapted_sql = adapted_sql.replace("ISODOW", "isodow")
        adapted_sql = adapted_sql.replace("btrim", "trim")
        adapted_sql = adapted_sql.replace("::TIMESTAMPTZ", "")
        adapted_sql = adapted_sql.replace("::INTEGER", "")
        adapted_sql = adapted_sql.replace("::NUMERIC", "")
        adapted_sql = adapted_sql.replace("COALESCE(NULLIF(trim(z.zone_name), ''), o.pickup_zone_id)", "COALESCE(NULLIF(trim(z.zone_name), ''), CAST(o.pickup_zone_id AS VARCHAR))")
        adapted_sql = adapted_sql.replace("COALESCE(NULLIF(trim(z.zone_name), ''), r.pickup_zone_id)", "COALESCE(NULLIF(trim(z.zone_name), ''), CAST(r.pickup_zone_id AS VARCHAR))")
        adapted_sql = adapted_sql.replace("b.slot_15m / 4", "FLOOR(b.slot_15m / 4)")
        adapted_sql = adapted_sql.replace("b.slot_15m %% 4", "(b.slot_15m % 4)")
        adapted_sql = adapted_sql.replace("%%", "%")
        adapted_sql = adapted_sql.replace("COALESCE(z.zone_name, '') ILIKE ANY (ARRAY['%Airport%', '%Penn Station%', '%Grand Central%', '%Times Sq%'])", "(z.zone_name ILIKE '%Airport%' OR z.zone_name ILIKE '%Penn Station%' OR z.zone_name ILIKE '%Grand Central%' OR z.zone_name ILIKE '%Times Sq%')")

        df = con.execute(adapted_sql).df()
        # Chuyển df sang list[dict]
        rows = df.to_dict(orient="records")

        report_lines.append(f"## {title}")
        report_lines.append(format_table(rows, max_rows=10))
        report_lines.append("")

    con.close()
    return "\n".join(report_lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Standalone Test Runner tự sinh dữ liệu và in kết quả")
    parser.add_argument("--zones", type=int, default=12, help="Số vùng sinh ngẫu nhiên (mặc định: 12)")
    parser.add_argument("--seed", type=int, default=42, help="Seed sinh ngẫu nhiên (mặc định: 42)")
    parser.add_argument("--output", default="docs/standalone-test-results.md", help="Đường dẫn lưu file báo cáo")
    args = parser.parse_args()

    result_md = run_standalone_demo(num_zones=args.zones, seed=args.seed)
    print(result_md)

    out_file = Path(args.output)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(result_md, encoding="utf-8")
    print(f"\n[THÀNH CÔNG] Đã lưu toàn bộ kết quả test vào file: {out_file.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
