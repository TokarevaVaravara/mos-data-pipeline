"""Сверка PostgreSQL с parquet и с расчётом витрин на pandas.

Запуск: PYTHONPATH=src python scripts/check_postgres.py --load-date 2026-10-21
Выход с кодом 1, если хоть одна проверка не сошлась. Печатает отпечатки таблиц (md5 всех строк),
по которым видно, изменила ли что-то повторная загрузка.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import numpy as np
import pandas as pd

from cleaning.permits_rules import global_id_checksum, output_dirs
from db.settings import PgSettings

SAMPLE_DAYS = 120  # дат для проверки active_permits_daily прямым перебором


def pg_round(numerator: int, denominator: int, places: int) -> Decimal:
    """Как round(numeric, places) в Postgres: точно и половина - от нуля."""
    return (Decimal(int(numerator)) / Decimal(int(denominator))).quantize(Decimal(1).scaleb(-places), ROUND_HALF_UP)


def read_clean(data_root: Path, load_date: str) -> pd.DataFrame:
    df = pd.read_parquet(output_dirs(data_root, load_date)["clean"],
                         columns=["global_id", "valid_from", "valid_to", "zone", "cancel_date"])
    for col in ("valid_from", "valid_to", "cancel_date"):
        df[col] = pd.to_datetime(df[col])
    df["zone"] = df["zone"].astype(str)
    return df


def query(conn, sql: str) -> pd.DataFrame:
    cur = conn.execute(sql)
    return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def expected_daily(df: pd.DataFrame) -> pd.DataFrame:
    days = pd.date_range(df["valid_from"].min(), df["valid_from"].max(), freq="D")
    grid = pd.MultiIndex.from_product([sorted(df["zone"].unique()), days], names=["zone", "day"])
    g = df.groupby(["zone", "valid_from"]).agg(permits=("global_id", "size"), cancelled=("cancel_date", "count"))
    g.index.names = ["zone", "day"]
    out = g.reindex(grid, fill_value=0).reset_index()
    by_zone = out.groupby("zone")["permits"]
    window_sum = by_zone.transform(lambda s: s.rolling(7, min_periods=1).sum())
    window_len = out.groupby("zone").cumcount().clip(upper=6) + 1  # в первые 6 дней окно короче
    out["permits_ma7"] = [pg_round(s, n, 2) for s, n in zip(window_sum, window_len)]
    out["permits_cumulative"] = by_zone.cumsum()
    return out


def active_on(df: pd.DataFrame, day: pd.Timestamp) -> pd.Series:
    """Прямо по определению: valid_from <= d <= valid_to и CancelDate пуста или не раньше d."""
    mask = (df["valid_from"] <= day) & (df["valid_to"] >= day) & (df["cancel_date"].isna() | (df["cancel_date"] >= day))
    return df[mask].groupby("zone").size()


def expected_active_full(df: pd.DataFrame) -> pd.DataFrame:
    """Весь ряд событиями: +1 в первый день, -1 после последнего (тот же смысл, другой код)."""
    last = df[["valid_to"]].assign(c=df["cancel_date"].fillna(df["valid_to"])).min(axis=1)
    ok = last >= df["valid_from"]
    starts = pd.DataFrame({"zone": df.loc[ok, "zone"], "day": df.loc[ok, "valid_from"], "d": 1})
    ends = pd.DataFrame({"zone": df.loc[ok, "zone"], "day": last[ok] + pd.Timedelta(days=1), "d": -1})
    deltas = pd.concat([starts, ends]).groupby(["zone", "day"])["d"].sum()
    days = pd.date_range(df.loc[ok, "valid_from"].min(), last[ok].max(), freq="D")
    grid = pd.MultiIndex.from_product([sorted(df["zone"].unique()), days], names=["zone", "day"])
    out = deltas.reindex(grid, fill_value=0).groupby(level="zone").cumsum().rename("active_permits").reset_index()
    return out


def validity_expected(df: pd.DataFrame) -> dict:
    days = (df["valid_to"] - df["valid_from"]).dt.days
    groups = {zone: days[df["zone"] == zone] for zone in sorted(df["zone"].unique())} | {"все": days}
    result = {}
    for zone, s in groups.items():
        counts = s.value_counts()
        top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
        result[zone] = {
            "permits": len(s), "min_days": int(s.min()), "median_days": float(s.median()),
            **{f"p{p}_days": int(np.percentile(s, p, method="inverted_cdf")) for p in (25, 75, 90, 99)},
            "max_days": int(s.max()), "longer_400_days": int((s > 400).sum()),
            "top_durations": [{"days": int(d), "permits": int(n)} for d, n in top],
        }
    return result


def fingerprints(conn) -> dict[str, str]:
    return {
        t: conn.execute(f"SELECT md5(string_agg(t::text, '|' ORDER BY global_id)) FROM clean.{t} t").fetchone()[0]
        for t in ("permits", "permits_rejected")
    }


def run_checks(conn, data_root: Path, load_date: str) -> list[tuple[str, str, str, bool]]:
    checks = []

    def add(name, expected, actual, ok=None):
        checks.append((name, str(expected), str(actual), expected == actual if ok is None else ok))

    df = read_clean(data_root, load_date)
    rejected = pd.read_parquet(output_dirs(data_root, load_date)["rejected"], columns=["global_id"])
    ids = conn.execute("SELECT global_id FROM clean.permits").fetchall()
    add("clean.permits: строк = parquet clean", len(df), len(ids))
    add("clean.permits: sha256 global_id = parquet", global_id_checksum(df["global_id"])[:16],
        global_id_checksum(r[0] for r in ids)[:16])
    add("clean.permits_rejected: строк = parquet rejected", len(rejected),
        conn.execute("SELECT count(*) FROM clean.permits_rejected").fetchone()[0])

    # daily_permits - все строки
    exp = expected_daily(df)
    got = query(conn, "SELECT zone, day, permits, cancelled, permits_ma7, permits_cumulative FROM mart.daily_permits")
    got["day"] = pd.to_datetime(got["day"])
    m = exp.merge(got, on=["zone", "day"], how="outer", suffixes=("_pd", "_pg"), indicator=True)
    add("daily_permits: строк", len(exp), len(got))
    add("daily_permits: строк без пары", 0, int((m["_merge"] != "both").sum()))
    for col in ("permits", "cancelled", "permits_ma7", "permits_cumulative"):
        add(f"daily_permits: {col} расходится в строках", 0, int((m[f"{col}_pd"] != m[f"{col}_pg"]).sum()))
    for zone, n in df.groupby("zone").size().items():
        add(f"daily_permits: сумма permits, {zone}", n, int(got.loc[got["zone"] == zone, "permits"].sum()))
    add("daily_permits: сумма cancelled", int(df["cancel_date"].notna().sum()), int(got["cancelled"].sum()))

    # active_permits_daily - весь ряд и выборка дат прямым перебором
    act = query(conn, "SELECT zone, day, active_permits FROM mart.active_permits_daily")
    act["day"] = pd.to_datetime(act["day"])
    full = expected_active_full(df).merge(act, on=["zone", "day"], how="outer", suffixes=("_pd", "_pg"))
    add("active_permits_daily: строк", len(act), len(full))
    add("active_permits_daily: расходится с расчётом событиями, строк", 0,
        int((full["active_permits_pd"] != full["active_permits_pg"]).sum()))
    rng = np.random.default_rng(0)
    all_days = act["day"].drop_duplicates().sort_values().to_numpy()
    sample = sorted(set(rng.choice(all_days, SAMPLE_DAYS - 2, replace=False)) | {all_days[0], all_days[-1]})
    act_idx = act.set_index(["zone", "day"])["active_permits"]
    wrong = 0
    for day in sample:
        brute = active_on(df, pd.Timestamp(day))
        for zone in df["zone"].unique():
            wrong += int(brute.get(zone, 0) != act_idx.get((zone, pd.Timestamp(day)), 0))
    add(f"active_permits_daily: расходится с перебором на {len(sample)} датах x 3 зоны", 0, wrong)

    # cancel_rate_by_zone_month
    cr = query(conn, "SELECT zone, month, permits, cancelled, cancel_rate FROM mart.cancel_rate_by_zone_month")
    cr["month"] = pd.to_datetime(cr["month"])
    e = df.assign(month=df["valid_from"].dt.to_period("M").dt.to_timestamp()).groupby(["zone", "month"]).agg(
        permits=("global_id", "size"), cancelled=("cancel_date", "count")).reset_index()
    mm = e.merge(cr, on=["zone", "month"], how="outer", suffixes=("_pd", "_pg"))
    add("cancel_rate_by_zone_month: строк", len(e), len(cr))
    add("cancel_rate_by_zone_month: permits/cancelled расходятся, строк", 0,
        int(((mm["permits_pd"] != mm["permits_pg"]) | (mm["cancelled_pd"] != mm["cancelled_pg"])).sum()))
    expected_rate = [pg_round(c, p, 4) for c, p in zip(mm["cancelled_pd"], mm["permits_pd"])]
    add("cancel_rate_by_zone_month: cancel_rate расходится, строк", 0,
        sum(int(e != g) for e, g in zip(expected_rate, mm["cancel_rate"])))

    # validity_stats
    vs = query(conn, "SELECT * FROM mart.validity_stats")
    expected_vs = validity_expected(df)
    for row in vs.to_dict("records"):
        exp_row = expected_vs[row["zone"]]
        got_row = {k: (float(row[k]) if k == "median_days" else row[k]) for k in exp_row}
        add(f"validity_stats: {row['zone']} (все поля, top-5)", "совпадает",
            "совпадает" if got_row == exp_row else f"{got_row} != {exp_row}", got_row == exp_row)
    return checks


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Сверка PostgreSQL с parquet и pandas")
    parser.add_argument("--load-date", type=date.fromisoformat, required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    args = parser.parse_args(argv)

    with PgSettings.from_env().connect() as conn:
        checks = run_checks(conn, args.data_root, args.load_date.isoformat())
        prints = fingerprints(conn)
    print("| проверка | ожидается (parquet / pandas) | Postgres | ок |\n|---|---|---|---|")
    for name, expected, actual, ok in checks:
        print(f"| {name} | {expected} | {actual} | {'да' if ok else 'НЕТ'} |")
    print("\n| таблица | md5 всех строк |\n|---|---|")
    for table, value in prints.items():
        print(f"| clean.{table} | {value} |")
    if not all(ok for *_, ok in checks):
        sys.exit(1)


if __name__ == "__main__":
    main()
