"""Kiểm thử đơn vị và hợp đồng cho tầng Data Warehouse (Bước 3.1–3.3 · Bảo).

Có thể chạy bằng cả ``pytest tests/test_warehouse.py`` hoặc ``python -m unittest tests/test_warehouse.py``.
"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi_pipeline.warehouse import (  # noqa: E402
    compare_pickup_demand,
    format_table,
    init_schema,
    load_baseline,
    load_zone_lookup,
    query_borough_density_summary,
    query_dispatch_matrix,
    upsert_replay_counts,
)
from nyc_taxi_pipeline.warehouse.db import DATABASE_DIR  # noqa: E402


class FakeCursor:
    """Cursor giả lập PostgreSQL phục vụ unit test không phụ thuộc tiến trình DB bên ngoài."""

    def __init__(self, state: Dict[str, Any]) -> None:
        self.state = state
        self.description: Optional[List[Tuple[str]]] = None
        self._rows: List[Tuple[Any, ...]] = []

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    def execute(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        self.state["executed_sql"].append((sql, params))
        norm = sql.strip()
        if "CREATE TABLE IF NOT EXISTS taxi_zones" in norm:
            self.state["schema_created"] = True
        elif norm.startswith("TRUNCATE TABLE historical_baseline"):
            self.state["historical_baseline"].clear()
        elif "INSERT INTO taxi_zones" in norm and params:
            zid = int(params["pickup_zone_id"])
            self.state["taxi_zones"][zid] = {
                "pickup_zone_id": zid,
                "zone_name": params["zone_name"],
                "borough": params["borough"],
            }
        elif "INSERT INTO historical_baseline" in norm and params:
            key = (int(params["pickup_zone_id"]), int(params["iso_weekday"]), int(params["slot_15m"]))
            self.state["historical_baseline"][key] = dict(params)
        elif "INSERT INTO replay_pickup_counts" in norm and params:
            key = (str(params["replay_id"]), int(params["pickup_zone_id"]), str(params["window_start"]))
            self.state["replay_pickup_counts"][key] = dict(params)
        elif "AS change_unavailable_reason" in norm and params:
            self._run_compare_demand(params)
        elif "AS dispatch_recommendation" in norm and params:
            self._run_dispatch_matrix(params)
        elif "AS city_share_pct" in norm and params:
            self._run_zone_density(params)
        else:
            self.description = [("ok",)]
            self._rows = [(1,)]

    def _run_compare_demand(self, params: Dict[str, Any]) -> None:
        cols = [
            "pickup_zone_id",
            "zone_name",
            "observed_trip_count",
            "baseline_trip_count",
            "reference_windows",
            "change_pct",
            "change_unavailable_reason",
        ]
        self.description = [(c,) for c in cols]
        replay_id = params["replay_id"]
        window_start = params["window_start"]
        out: List[Dict[str, Any]] = []
        for (rid, zid, wstart), r in self.state["replay_pickup_counts"].items():
            if rid != replay_id or wstart != window_start:
                continue
            z = self.state["taxi_zones"].get(zid)
            zone_name = z["zone_name"] if z and z.get("zone_name") else str(zid)
            b = self.state["historical_baseline"].get((zid, 1, 32))
            obs = int(r["trip_count"])
            if b is None:
                base_val = None
                ref_win = None
                chg = None
                reason = "missing_baseline"
            else:
                base_val = float(b["baseline_trip_count"])
                ref_win = int(b["reference_windows"])
                if base_val == 0:
                    chg = None
                    reason = "zero_baseline"
                else:
                    chg = round(100.0 * (obs - base_val) / base_val, 2)
                    reason = None
            out.append(
                {
                    "pickup_zone_id": zid,
                    "zone_name": zone_name,
                    "observed_trip_count": obs,
                    "baseline_trip_count": base_val,
                    "reference_windows": ref_win,
                    "change_pct": chg,
                    "change_unavailable_reason": reason,
                }
            )
        out.sort(
            key=lambda x: (
                x["change_pct"] is None,
                -(x["change_pct"] or 0.0),
                -x["observed_trip_count"],
                x["pickup_zone_id"],
            )
        )
        self._rows = [tuple(item[c] for c in cols) for item in out]

    def _run_dispatch_matrix(self, params: Dict[str, Any]) -> None:
        cols = [
            "pickup_zone_id",
            "zone_name",
            "borough",
            "observed_trip_count",
            "baseline_trip_count",
            "net_trip_diff",
            "change_pct",
            "dispatch_action",
            "dispatch_recommendation",
        ]
        self._run_compare_demand(params)
        self.description = [(c,) for c in cols]
        rows = []
        for r in self._rows:
            zid, zname, obs, base, _, chg, _ = r
            borough = (self.state["taxi_zones"].get(zid) or {}).get("borough", "Unknown")
            if base is None or base == 0:
                action = "NEW_ACTIVITY"
                rec = "Phát sinh nhu cầu mới ngoài lịch sử"
            elif chg >= 30.0:
                action = "SURGE_NEED_TAXIS"
                rec = "Điểm nóng thiếu xe - Cần tiếp nhận xe chi viện"
            elif chg <= -30.0:
                action = "SURPLUS_CAN_REALLOCATE"
                rec = "Điều chuyển xe sang vùng thiếu xe trong cùng Quận"
            else:
                action = "BALANCED"
                rec = "Duy trì đội xe hiện tại"
            rows.append((zid, zname, borough, obs, base, obs - (base or 0), chg, action, rec))
        self._rows = rows

    def _run_zone_density(self, params: Dict[str, Any]) -> None:
        cols = [
            "pickup_zone_id",
            "zone_name",
            "borough",
            "observed_trip_count",
            "borough_total_trips",
            "city_total_trips",
            "zone_in_borough_share_pct",
            "city_share_pct",
        ]
        self.description = [(c,) for c in cols]
        replay_id = params["replay_id"]
        window_start = params["window_start"]
        items = [
            r
            for (rid, _, wstart), r in self.state["replay_pickup_counts"].items()
            if rid == replay_id and wstart == window_start
        ]
        city_total = sum(int(i["trip_count"]) for i in items)
        borough_totals: Dict[str, int] = {}
        for i in items:
            zid = int(i["pickup_zone_id"])
            bname = (self.state["taxi_zones"].get(zid) or {}).get("borough", "Unknown")
            borough_totals[bname] = borough_totals.get(bname, 0) + int(i["trip_count"])
        out = []
        for i in sorted(items, key=lambda x: (-int(x["trip_count"]), int(x["pickup_zone_id"]))):
            zid = int(i["pickup_zone_id"])
            obs = int(i["trip_count"])
            zinfo = self.state["taxi_zones"].get(zid) or {}
            zname = zinfo.get("zone_name", str(zid))
            bname = zinfo.get("borough", "Unknown")
            btot = borough_totals[bname]
            out.append(
                (
                    zid,
                    zname,
                    bname,
                    obs,
                    btot,
                    city_total,
                    round(100.0 * obs / btot, 2) if btot else 0.0,
                    round(100.0 * obs / city_total, 2) if city_total else 0.0,
                )
            )
        self._rows = out

    def fetchall(self) -> List[Tuple[Any, ...]]:
        return list(self._rows)


class FakeConnection:
    def __init__(self) -> None:
        self.state: Dict[str, Any] = {
            "schema_created": False,
            "taxi_zones": {},
            "historical_baseline": {},
            "replay_pickup_counts": {},
            "executed_sql": [],
        }

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.state)

    def close(self) -> None:
        pass


class TestWarehouseContract(unittest.TestCase):
    def test_sql_files_exist_and_non_empty(self) -> None:
        expected_files = [
            DATABASE_DIR / "schema.sql",
            DATABASE_DIR / "load" / "zone_lookup.sql",
            DATABASE_DIR / "load" / "baseline.sql",
            DATABASE_DIR / "load" / "replay_counts.sql",
            DATABASE_DIR / "queries" / "compare_pickup_demand.sql",
            DATABASE_DIR / "queries" / "taxi_dispatch_matrix.sql",
            DATABASE_DIR / "queries" / "zone_and_borough_density.sql",
            DATABASE_DIR / "queries" / "airport_and_transit_hubs.sql",
            DATABASE_DIR / "queries" / "trip_volume_summary.sql",
            DATABASE_DIR / "queries" / "weekly_peak_hours.sql",
            DATABASE_DIR / "queries" / "time_series_trend.sql",
        ]
        for path in expected_files:
            self.assertTrue(path.exists(), f"Thiếu file SQL: {path}")
            content = path.read_text(encoding="utf-8").strip()
            self.assertGreater(len(content), 50, f"File SQL rỗng hoặc chưa hoàn thiện: {path}")

    def test_zone_lookup_from_manifest_and_idempotency(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            csv_path = tmp_path / "taxi_zone_lookup.csv"
            csv_path.write_text(
                "LocationID,Borough,Zone\n"
                "161,Manhattan,Midtown Center\n"
                "230,Manhattan,Times Sq/Theatre District\n"
                "0,Invalid,BadZone\n",
                encoding="utf-8",
            )
            manifest_path = tmp_path / "manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "datasets": [
                            {
                                "dataset_role": "zone_lookup",
                                "source_id": "taxi-zone-lookup",
                                "raw_path": str(csv_path),
                                "status": "ready",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            conn = FakeConnection()
            init_schema(conn)
            self.assertTrue(conn.state["schema_created"])

            res1 = load_zone_lookup(conn, manifest_path=manifest_path)
            self.assertEqual(res1["loaded_rows"], 2)
            self.assertEqual(res1["invalid_rows"], 1)
            self.assertEqual(len(conn.state["taxi_zones"]), 2)

            res2 = load_zone_lookup(conn, manifest_path=manifest_path)
            self.assertEqual(res2["loaded_rows"], 2)
            self.assertEqual(len(conn.state["taxi_zones"]), 2)

    def test_load_baseline_validation_and_duplicate_rejection(self) -> None:
        conn = FakeConnection()
        duplicate_rows = [
            {
                "pickup_zone_id": 161,
                "iso_weekday": 1,
                "slot_15m": 32,
                "baseline_trip_count": "2.0",
                "reference_windows": 2,
                "history_start": "2025-01-06T08:00:00-05:00",
                "history_end": "2025-01-13T08:15:00-05:00",
            },
            {
                "pickup_zone_id": 161,
                "iso_weekday": 1,
                "slot_15m": 32,
                "baseline_trip_count": "3.0",
                "reference_windows": 2,
                "history_start": "2025-01-06T08:00:00-05:00",
                "history_end": "2025-01-13T08:15:00-05:00",
            },
        ]
        with self.assertRaisesRegex(ValueError, "Trùng khóa"):
            load_baseline(conn, duplicate_rows)

    def test_end_to_end_warehouse_queries_and_edge_cases(self) -> None:
        conn = FakeConnection()
        init_schema(conn)

        load_zone_lookup(
            conn,
            [
                {"pickup_zone_id": 161, "zone_name": "Midtown Center", "borough": "Manhattan"},
                {"pickup_zone_id": 230, "zone_name": "Times Sq/Theatre District", "borough": "Manhattan"},
                {"pickup_zone_id": 132, "zone_name": "JFK Airport", "borough": "Queens"},
            ],
        )
        load_baseline(
            conn,
            [
                {
                    "pickup_zone_id": 161,
                    "iso_weekday": 1,
                    "slot_15m": 32,
                    "baseline_trip_count": "2",
                    "reference_windows": 2,
                    "history_start": "2025-01-06T08:00:00-05:00",
                    "history_end": "2025-01-13T08:15:00-05:00",
                },
                {
                    "pickup_zone_id": 230,
                    "iso_weekday": 1,
                    "slot_15m": 32,
                    "baseline_trip_count": "10",
                    "reference_windows": 2,
                    "history_start": "2025-01-06T08:00:00-05:00",
                    "history_end": "2025-01-13T08:15:00-05:00",
                },
                {
                    "pickup_zone_id": 132,
                    "iso_weekday": 1,
                    "slot_15m": 32,
                    "baseline_trip_count": "0",
                    "reference_windows": 2,
                    "history_start": "2025-01-06T08:00:00-05:00",
                    "history_end": "2025-01-13T08:15:00-05:00",
                },
            ],
        )

        replay_id = "taxi-fixture-001"
        w_start = "2025-01-20T08:00:00-05:00"
        w_end = "2025-01-20T08:15:00-05:00"
        upsert_replay_counts(
            conn,
            [
                {"replay_id": replay_id, "pickup_zone_id": 161, "window_start": w_start, "window_end": w_end, "trip_count": 3},
                {"replay_id": replay_id, "pickup_zone_id": 230, "window_start": w_start, "window_end": w_end, "trip_count": 4},
                {"replay_id": replay_id, "pickup_zone_id": 132, "window_start": w_start, "window_end": w_end, "trip_count": 5},
                {"replay_id": replay_id, "pickup_zone_id": 999, "window_start": w_start, "window_end": w_end, "trip_count": 2},
            ],
        )

        rows = compare_pickup_demand(conn, replay_id, w_start)
        self.assertEqual(len(rows), 4)

        first = rows[0]
        self.assertEqual(first["pickup_zone_id"], 161)
        self.assertEqual(first["zone_name"], "Midtown Center")
        self.assertEqual(first["observed_trip_count"], 3)
        self.assertEqual(first["baseline_trip_count"], 2.0)
        self.assertEqual(first["reference_windows"], 2)
        self.assertEqual(first["change_pct"], 50.0)
        self.assertIsNone(first["change_unavailable_reason"])

        second = rows[1]
        self.assertEqual(second["pickup_zone_id"], 230)
        self.assertEqual(second["change_pct"], -60.0)

        by_zone = {r["pickup_zone_id"]: r for r in rows}
        self.assertIsNone(by_zone[132]["change_pct"])
        self.assertEqual(by_zone[132]["change_unavailable_reason"], "zero_baseline")

        self.assertEqual(by_zone[999]["zone_name"], "999")
        self.assertIsNone(by_zone[999]["change_pct"])
        self.assertEqual(by_zone[999]["change_unavailable_reason"], "missing_baseline")

        dispatch_rows = query_dispatch_matrix(conn, replay_id, w_start)
        dispatch_by_zone = {r["pickup_zone_id"]: r for r in dispatch_rows}
        self.assertEqual(dispatch_by_zone[161]["dispatch_action"], "SURGE_NEED_TAXIS")
        self.assertEqual(dispatch_by_zone[230]["dispatch_action"], "SURPLUS_CAN_REALLOCATE")

        borough_rows = query_borough_density_summary(conn, replay_id, w_start)
        self.assertEqual(borough_rows[0]["borough"], "Manhattan")
        self.assertEqual(borough_rows[0]["borough_total_trips"], 7)
        self.assertIn("Midtown Center", format_table(rows))


if __name__ == "__main__":
    unittest.main()
