"""Quản lý kết nối PostgreSQL dùng chung cho Bước 3.1–3.3."""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional, Union

DEFAULT_DSN_ENV = "POSTGRES_DSN"
DEFAULT_DSN = "postgresql://postgres:postgres@localhost:5432/nyc_taxi"

REPO_ROOT = Path(__file__).resolve().parents[3]
DATABASE_DIR = REPO_ROOT / "database"


def get_dsn(dsn: Optional[str] = None, env_var: str = DEFAULT_DSN_ENV) -> str:
    """Lấy chuỗi kết nối PostgreSQL từ tham số trực tiếp hoặc biến môi trường."""
    if dsn and dsn.strip():
        return dsn.strip()
    env_val = os.environ.get(env_var, "").strip()
    if env_val:
        return env_val
    return DEFAULT_DSN


def connect_postgres(dsn: Optional[str] = None) -> Any:
    """Mở kết nối PostgreSQL qua psycopg2 (hoặc psycopg v3 nếu có)."""
    resolved_dsn = get_dsn(dsn)
    try:
        import psycopg2  # type: ignore

        return psycopg2.connect(resolved_dsn)
    except ImportError:
        try:
            import psycopg  # type: ignore

            return psycopg.connect(resolved_dsn)
        except ImportError as exc:
            raise RuntimeError(
                "Cần cài đặt thư viện 'psycopg2-binary' (hoặc 'psycopg') để kết nối PostgreSQL: "
                "pip install psycopg2-binary"
            ) from exc


@contextmanager
def managed_connection(conn_or_dsn: Optional[Union[str, Any]] = None) -> Iterator[Any]:
    """Context manager tự động đóng kết nối nếu đầu vào là chuỗi DSN, hoặc giữ nguyên nếu là connection có sẵn."""
    if conn_or_dsn is None or isinstance(conn_or_dsn, str):
        conn = connect_postgres(conn_or_dsn)
        try:
            yield conn
        finally:
            conn.close()
    else:
        yield conn_or_dsn

