"""PostgreSQL: загрузка clean/rejected и витрины на маленькой фикстуре.

Нужен запущенный Postgres (docker compose up -d) и POSTGRES_* в окружении или .env.
Тесты работают в отдельной базе <POSTGRES_DB>_test и пересоздают схемы перед каждым тестом.
"""
from datetime import date
from decimal import Decimal

import pytest

from cleaning import clean_permits_pandas
from test_clean_permits import write_page

LOAD_DATE = "2026-10-21"

#  метка  gid  ID  Series  Number  ValidFrom     ValidTo       Zone   Status         CancelDate
ROWS = [
    ("sk_short", 1, 1, "БА", "1", "01.01.2024", "10.01.2024", "СК", "Выдан", ""),  # 9 дней
    ("sk_cancelled", 2, 2, "БА", "2", "01.01.2024", "31.12.2024", "СК", "Аннулирован", "05.01.2024"),  # действует 1-5.01
    ("sk_short_2", 3, 3, "БА", "3", "03.01.2024", "12.01.2024", "СК", "Выдан", ""),
    ("ttk_cancel_before", 4, 4, "БА", "4", "02.01.2024", "30.12.2024", "ТТК", "Аннулирован", "01.01.2024"),  # ни дня
    ("ttk_short", 5, 5, "БА", "5", "02.01.2024", "11.01.2024", "ТТК", "Выдан", ""),
    ("ttk_short_dup", 6, 5, "БА", "5", "02.01.2024", "11.01.2024", "ТТК", "Выдан", ""),  # rejected: duplicate_id
    ("sk_feb", 7, 7, "БА", "7", "10.02.2024", "09.02.2025", "СК", "Выдан", ""),  # 365 дней
    ("ttk_range", 8, 8, "БА", "8", "05.01.2024", "01.01.2024", "ТТК", "Выдан", ""),  # rejected: invalid_validity_range
]


def pg_settings():
    from db.settings import PgSettings

    try:
        admin = PgSettings.from_env()
        with admin.connect(autocommit=True, connect_timeout=3) as conn:
            test_db = f"{admin.dbname}_test"
            if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (test_db,)).fetchone():
                conn.execute(f'CREATE DATABASE "{test_db}"')
        return admin.with_db(test_db)
    except Exception as exc:  # noqa: BLE001 - нет настроек или Postgres не запущен
        pytest.skip(f"PostgreSQL недоступен: {type(exc).__name__}", allow_module_level=True)


SETTINGS = pg_settings()


def make_clean(root, rows, rejected=2):
    """raw -> очистка pandas -> data/clean и data/rejected, как в настоящем пайплайне."""
    write_page(root / "raw" / "permits" / f"load_date={LOAD_DATE}" / "page_000000.jsonl", rows)
    report = clean_permits_pandas.run(root, root, LOAD_DATE)
    assert (report["rows_clean"], report["rows_rejected"]) == (len(rows) - rejected, rejected)
    return root


@pytest.fixture
def db():
    with SETTINGS.connect(autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS mart CASCADE")
        conn.execute("DROP SCHEMA IF EXISTS clean CASCADE")
        yield conn


@pytest.fixture
def loaded(spark, db, tmp_path):
    from db.sql_files import apply
    from spark_jobs.load_postgres import load

    data_root = make_clean(tmp_path / "data", ROWS)
    first = load(spark, SETTINGS, data_root, LOAD_DATE)
    apply(SETTINGS, "marts")
    return first, data_root


def fingerprint(db, table):
    return db.execute(f"SELECT md5(string_agg(t::text, '|' ORDER BY global_id)) FROM clean.{table} t").fetchone()[0]


def rows(db, sql, *params):
    return db.execute(sql, params).fetchall()


# --- загрузка ----------------------------------------------------------------

def test_load_counts_match_parquet(loaded, db):
    first, _ = loaded
    assert first["parquet_rows"] == first["table_rows"] == {"clean": 6, "rejected": 2}
    assert first["permits"] == {"inserted": 6, "updated": 0, "removed_from_other": 0}
    assert first["rejected"] == {"inserted": 2, "updated": 0, "removed_from_other": 0}
    assert rows(db, "SELECT global_id, reason FROM clean.permits_rejected ORDER BY 1") == [
        (6, "duplicate_id"), (8, "invalid_validity_range"),
    ]
    assert rows(db, "SELECT count(*) FROM clean.permits_stage") == [(0,)]  # промежуточная очищена


def test_types_and_flags(loaded, db):
    row = rows(db, "SELECT id, valid_from, valid_to, cancel_date, cancelled_before_start, status_cancel_mismatch, "
                   "id_conflict, source_load_date FROM clean.permits WHERE global_id = 4")[0]
    assert row == (4, date(2024, 1, 2), date(2024, 12, 30), date(2024, 1, 1), True, False, False, date(2026, 10, 21))


def test_second_load_changes_nothing(loaded, spark, db):
    from spark_jobs.load_postgres import load

    _, data_root = loaded
    before = fingerprint(db, "permits"), fingerprint(db, "permits_rejected")
    second = load(spark, SETTINGS, data_root, LOAD_DATE)
    assert second["permits"] == {"inserted": 0, "updated": 0, "removed_from_other": 0}
    assert second["rejected"] == {"inserted": 0, "updated": 0, "removed_from_other": 0}
    assert second["table_rows"] == {"clean": 6, "rejected": 2}
    assert (fingerprint(db, "permits"), fingerprint(db, "permits_rejected")) == before


def test_changed_row_is_updated_and_moved_row_leaves_other_table(loaded, spark, db, tmp_path):
    from spark_jobs.load_postgres import load

    changed = [r if r[0] != "sk_short_2" else (*r[:7], "ТТК", *r[8:]) for r in ROWS]  # зона СК -> ТТК
    # gid 1 становится отклонённым, а дубль gid 6 (rejected) - оставшимся: строки меняют таблицу
    changed = [r if r[0] != "sk_short" else (*r[:5], "01.01.2024", "31.12.2023", *r[7:]) for r in changed]
    changed = [r for r in changed if r[0] != "ttk_short"]
    changed.append(("other_rejected", 9, 9, "БА", "9", "01.01.2024", "31.12.2023", "СК", "Выдан", ""))
    data_root = make_clean(tmp_path / "data2", changed, rejected=3)  # gid 1, 8, 9

    result = load(spark, SETTINGS, data_root, LOAD_DATE)
    assert result["permits"] == {"inserted": 1, "updated": 1, "removed_from_other": 1}  # gid 6 вставлен, gid 3 изменён
    assert result["rejected"]["removed_from_other"] == 1  # gid 1 убран из clean
    assert rows(db, "SELECT zone FROM clean.permits WHERE global_id = 3") == [("ТТК",)]
    assert rows(db, "SELECT count(*) FROM clean.permits WHERE global_id = 1") == [(0,)]
    assert rows(db, "SELECT count(*) FROM clean.permits_rejected WHERE global_id = 6") == [(0,)]


# --- витрины -----------------------------------------------------------------

def test_daily_permits_with_windows(loaded, db):
    got = {(d, z): r for d, z, *r in rows(db, "SELECT day, zone, permits, cancelled, permits_ma7, permits_cumulative "
                                             "FROM mart.daily_permits")}
    assert len(got) == 41 * 2  # 01.01-10.02.2024 по двум зонам, дни без пропусков тоже есть
    assert got[(date(2024, 1, 1), "СК")] == [2, 1, Decimal("2.00"), 2]
    assert got[(date(2024, 1, 3), "СК")] == [1, 0, Decimal("1.00"), 3]  # (2+0+1)/3
    assert got[(date(2024, 1, 7), "СК")][2:] == [Decimal("0.43"), 3]  # 3/7
    assert got[(date(2024, 1, 8), "СК")][2] == Decimal("0.14")  # 1/7: 01.01 вышло из окна
    assert got[(date(2024, 2, 10), "СК")] == [1, 0, Decimal("0.14"), 4]
    assert got[(date(2024, 1, 2), "ТТК")] == [2, 1, Decimal("1.00"), 2]


def test_active_permits_daily(loaded, db):
    got = dict(((d, z), n) for d, z, n in rows(db, "SELECT day, zone, active_permits FROM mart.active_permits_daily"))
    expected = {
        (date(2024, 1, 1), "СК"): 2, (date(2024, 1, 3), "СК"): 3,
        (date(2024, 1, 5), "СК"): 3,  # в день аннулирования ещё действует
        (date(2024, 1, 6), "СК"): 2, (date(2024, 1, 11), "СК"): 1, (date(2024, 1, 13), "СК"): 0,
        (date(2024, 2, 10), "СК"): 1, (date(2025, 2, 9), "СК"): 1,
        (date(2024, 1, 1), "ТТК"): 0, (date(2024, 1, 2), "ТТК"): 1,  # отменённый до начала не действует
        (date(2024, 1, 11), "ТТК"): 1, (date(2024, 1, 12), "ТТК"): 0,
    }
    assert {k: got[k] for k in expected} == expected
    assert min(d for d, _ in got) == date(2024, 1, 1) and max(d for d, _ in got) == date(2025, 2, 9)


def test_active_permits_daily_equals_brute_force(loaded, db):
    brute = rows(db, """
        SELECT d::date, z.zone, count(p.global_id)
        FROM generate_series(DATE '2024-01-01', DATE '2025-02-09', interval '1 day') d
        CROSS JOIN (SELECT DISTINCT zone FROM clean.permits) z
        LEFT JOIN clean.permits p ON p.zone = z.zone AND p.valid_from <= d AND p.valid_to >= d
                                 AND (p.cancel_date IS NULL OR p.cancel_date >= d)
        GROUP BY 1, 2 ORDER BY 1, 2""")
    assert rows(db, "SELECT day, zone, active_permits FROM mart.active_permits_daily ORDER BY 1, 2") == brute


def test_cancel_rate_by_zone_month(loaded, db):
    got = rows(db, "SELECT month, zone, permits, cancelled, cancel_rate::float8 FROM mart.cancel_rate_by_zone_month "
                   "ORDER BY 1, 2")
    assert got == [
        (date(2024, 1, 1), "СК", 3, 1, 0.3333),
        (date(2024, 1, 1), "ТТК", 2, 1, 0.5),
        (date(2024, 2, 1), "СК", 1, 0, 0.0),
    ]


def test_validity_stats(loaded, db):
    got = {r[0]: r[1:] for r in rows(db, "SELECT zone, permits, min_days, median_days, p25_days, p75_days, p90_days, "
                                         "p99_days, max_days, longer_400_days, top_durations FROM mart.validity_stats")}
    # длительности: СК 9, 365, 9, 365; ТТК 363, 9
    assert got["все"] == (6, 0 + 9, 186.0, 9, 365, 365, 365, 365, 0,
                          [{"days": 9, "permits": 3}, {"days": 365, "permits": 2}, {"days": 363, "permits": 1}])
    assert got["СК"][:3] == (4, 9, 187.0)
    assert got["СК"][-1] == [{"days": 9, "permits": 2}, {"days": 365, "permits": 2}]
    assert got["ТТК"][:3] == (2, 9, 186.0)


def test_marts_rebuild_is_repeatable(loaded, db):
    from db.sql_files import apply

    before = rows(db, "SELECT * FROM mart.daily_permits ORDER BY zone, day")
    apply(SETTINGS, "marts")
    assert rows(db, "SELECT * FROM mart.daily_permits ORDER BY zone, day") == before


def test_ddl_is_idempotent(db):
    from db.sql_files import apply

    apply(SETTINGS, "ddl")
    apply(SETTINGS, "ddl")
    assert rows(db, "SELECT count(*) FROM pg_indexes WHERE indexname = 'permits_valid_from_zone_idx'") == [(1,)]


def test_password_not_in_repr():
    assert SETTINGS.password not in repr(SETTINGS)
