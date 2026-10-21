"""Очистка пропусков на pandas - те же правила, что в spark_jobs.clean_permits (для сверки).

Запуск из корня проекта:
PYTHONPATH=src python -m cleaning.clean_permits_pandas --load-date 2026-10-21 --out-root data/pandas
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import time
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from .permits_rules import (
    ANNULLED,
    DATE_FORMAT_RE,
    DUPLICATE_KEY,
    FLAGS,
    ISSUED,
    STATUSES,
    ZONES,
    build_report,
    global_id_checksum,
    output_dirs,
    raw_partition,
    write_report,
)

log = logging.getLogger(__name__)

CELL_FIELDS = ("global_id", "ID", "Series", "Number", "ValidFrom", "ValidTo", "Zone", "Status", "CancelDate")


def read_raw(source: Path) -> pd.DataFrame:
    """Только page_*.jsonl партиции, только поля Cells. Битая строка - ошибка, как FAILFAST в Spark."""
    records = []
    for page in sorted(source.glob("page_*.jsonl")):
        with page.open(encoding="utf-8") as f:
            for line in f:
                cells = json.loads(line)["Cells"]
                records.append(tuple(cells.get(k) for k in CELL_FIELDS))
    df = pd.DataFrame.from_records(records, columns=CELL_FIELDS)
    for col in ("global_id", "ID"):
        if not df[col].map(lambda v: v is None or (isinstance(v, int) and not isinstance(v, bool))).all():
            raise ValueError(f"{col}: не целое число в raw")
    df["global_id"] = df["global_id"].astype("Int64")
    df["ID"] = df["ID"].astype("Int32")
    for col in CELL_FIELDS[2:]:
        df[col] = df[col].astype("string")
    return df


def _strict_date(s: pd.Series) -> pd.Series:
    ok = s.str.match(DATE_FORMAT_RE).fillna(False).astype(bool)
    return pd.to_datetime(s.where(ok), format="%d.%m.%Y", errors="coerce")


def classify(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cancel_empty"] = (df["CancelDate"].isna() | (df["CancelDate"] == "")).astype(bool)
    df["valid_from"] = _strict_date(df["ValidFrom"])
    df["valid_to"] = _strict_date(df["ValidTo"])
    df["cancel_date"] = _strict_date(df["CancelDate"].where(~df["cancel_empty"]))

    bad_date = df["valid_from"].isna() | df["valid_to"].isna() | (~df["cancel_empty"] & df["cancel_date"].isna())
    conditions = [
        bad_date,
        (df["valid_to"] < df["valid_from"]).fillna(False),
        ~df["Zone"].isin(ZONES).fillna(False).astype(bool),
        ~df["Status"].isin(STATUSES).fillna(False).astype(bool),
    ]
    reasons = ["unparseable_date", "invalid_validity_range", "unknown_zone", "unknown_status"]
    df["reason"] = pd.Series(np.select(conditions, reasons, default=""), index=df.index).replace("", None)

    # дубли - только среди прошедших остальные проверки; остаётся минимальный global_id
    candidates = df["reason"].isna()
    ordered = df[candidates].sort_values("global_id", kind="stable")
    dup_index = ordered.index[ordered.duplicated(subset=list(DUPLICATE_KEY), keep="first")]
    df.loc[dup_index, "reason"] = "duplicate_id"

    kept = df["reason"].isna()
    per_id = df[kept].groupby("ID")["ID"].transform("size")
    df["id_conflict"] = pd.Series(pd.NA, index=df.index, dtype="boolean")
    df.loc[kept, "id_conflict"] = per_id > 1

    df["cancelled_before_start"] = (df["cancel_date"] < df["valid_from"]).fillna(False).astype(bool)
    df["status_cancel_mismatch"] = ((df["Status"] == ISSUED) & ~df["cancel_empty"]) | (
        (df["Status"] == ANNULLED) & df["cancel_empty"]
    )
    df["status_cancel_mismatch"] = df["status_cancel_mismatch"].fillna(False).astype(bool)
    return df


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "global_id": df["global_id"],
        "id": df["ID"],
        "series": df["Series"],
        "number": df["Number"],
        "valid_from": df["valid_from"].astype("date32[pyarrow]"),
        "valid_to": df["valid_to"].astype("date32[pyarrow]"),
        "zone": df["Zone"],
        "status": df["Status"],
        "cancel_date": df["cancel_date"].astype("date32[pyarrow]"),
        "cancelled_before_start": df["cancelled_before_start"],
        "status_cancel_mismatch": df["status_cancel_mismatch"],
        "id_conflict": df["id_conflict"].astype(bool),
        "valid_from_year": df["valid_from"].dt.year.astype("int32"),
    })
    return out


def rejected_columns(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "global_id": df["global_id"],
        "id": df["ID"],
        "series": df["Series"],
        "number": df["Number"],
        "valid_from_raw": df["ValidFrom"],
        "valid_to_raw": df["ValidTo"],
        "zone": df["Zone"],
        "status": df["Status"],
        "cancel_date_raw": df["CancelDate"],
        "reason": df["reason"].astype("string"),
    })


def _write(df: pd.DataFrame, path: Path, partition_cols: list[str] | None = None) -> None:
    shutil.rmtree(path, ignore_errors=True)  # перезапись, как mode("overwrite") в Spark
    path.mkdir(parents=True)
    if partition_cols:
        df.to_parquet(path, engine="pyarrow", partition_cols=partition_cols, index=False)
    else:
        df.to_parquet(path / "part-0.parquet", engine="pyarrow", index=False)


def run(data_root: Path, out_root: Path, load_date: str) -> dict:
    source = raw_partition(data_root, load_date)
    if not source.is_dir():
        raise FileNotFoundError(f"нет партиции {source}")
    dirs = output_dirs(out_root, load_date)

    classified = classify(read_raw(source))
    rows_raw = len(classified)
    is_clean = classified["reason"].isna()
    _write(clean_columns(classified[is_clean]), dirs["clean"], ["valid_from_year"])
    _write(rejected_columns(classified[~is_clean]), dirs["rejected"])

    # отчёт - по тому, что реально записано
    clean_out = pd.read_parquet(dirs["clean"], columns=["global_id", *FLAGS])
    rejected_out = pd.read_parquet(dirs["rejected"], columns=["reason"])
    report = build_report(
        "pandas", load_date, source,
        rows_raw=rows_raw, rows_clean=len(clean_out), rows_rejected=len(rejected_out),
        rejected_by_reason=rejected_out["reason"].value_counts().to_dict(),
        flags={f: int(clean_out[f].sum()) for f in FLAGS},
        clean_checksum=global_id_checksum(clean_out["global_id"]),
    )
    write_report(dirs["quality"], report)
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="pandas: raw -> clean/rejected для пропусков")
    parser.add_argument("--load-date", type=date.fromisoformat, required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"), help="где лежит raw/")
    parser.add_argument("--out-root", type=Path, default=Path("data/pandas"), help="куда писать clean/rejected/quality")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    started = time.perf_counter()
    report = run(args.data_root, args.out_root, args.load_date.isoformat())
    log.info(
        "готово за %.2f с: raw %d, clean %d, rejected %d",
        time.perf_counter() - started, report["rows_raw"], report["rows_clean"], report["rows_rejected"],
    )


if __name__ == "__main__":
    main()
