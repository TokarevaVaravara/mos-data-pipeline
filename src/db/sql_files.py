"""Выполнение SQL-файлов из sql/: DDL и витрины.

Запуск: PYTHONPATH=src python -m db.sql_files ddl|marts
Файлы папки выполняются по алфавиту в одной транзакции: либо всё, либо ничего.
"""
from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from .settings import SQL_DIR, PgSettings

log = logging.getLogger(__name__)

GROUPS = {"ddl": SQL_DIR / "ddl", "marts": SQL_DIR / "marts"}


def read_sql(relative: str) -> str:
    return (SQL_DIR / relative).read_text(encoding="utf-8")


def apply_dir(conn, directory: Path) -> list[str]:
    files = sorted(directory.glob("*.sql"))
    with conn.transaction():
        for path in files:
            started = time.perf_counter()
            conn.execute(path.read_text(encoding="utf-8"))
            log.info("%s: %.2f с", path.name, time.perf_counter() - started)
    return [p.name for p in files]


def apply(settings: PgSettings, group: str) -> list[str]:
    with settings.connect() as conn:
        names = apply_dir(conn, GROUPS[group])
    if group == "marts":
        with settings.connect(autocommit=True) as conn:
            conn.execute("ANALYZE mart.daily_permits, mart.active_permits_daily, "
                         "mart.cancel_rate_by_zone_month, mart.validity_stats")
    return names


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Выполнить SQL из sql/ddl или sql/marts")
    parser.add_argument("group", choices=sorted(GROUPS))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    apply(PgSettings.from_env(), args.group)


if __name__ == "__main__":
    main()
