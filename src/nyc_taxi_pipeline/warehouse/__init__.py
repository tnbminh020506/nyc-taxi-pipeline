"""Tầng Data Warehouse (Bước 3.1–3.3 · Bảo): Khởi tạo bảng, nạp dữ liệu và truy vấn phân tích trên PostgreSQL."""
from nyc_taxi_pipeline.warehouse.db import DEFAULT_DSN_ENV, connect_postgres, get_dsn
from nyc_taxi_pipeline.warehouse.loader import (
    PostgresReplayUpsertSink,
    init_schema,
    load_baseline,
    load_zone_lookup,
    upsert_replay_counts,
)
from nyc_taxi_pipeline.warehouse.queries import (
    compare_pickup_demand,
    format_table,
    query_airport_and_transit_hubs,
    query_borough_density_summary,
    query_dispatch_matrix,
    query_time_series_trend,
    query_trip_volume_summary,
    query_weekly_peak_hours,
    query_zone_and_borough_density,
    run_all_dashboard_queries,
)

__all__ = [
    "DEFAULT_DSN_ENV",
    "PostgresReplayUpsertSink",
    "compare_pickup_demand",
    "connect_postgres",
    "format_table",
    "get_dsn",
    "init_schema",
    "load_baseline",
    "load_zone_lookup",
    "query_airport_and_transit_hubs",
    "query_borough_density_summary",
    "query_dispatch_matrix",
    "query_time_series_trend",
    "query_trip_volume_summary",
    "query_weekly_peak_hours",
    "query_zone_and_borough_density",
    "run_all_dashboard_queries",
    "upsert_replay_counts",
]
