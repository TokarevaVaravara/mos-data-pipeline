"""Очистка raw -> clean/rejected: одинаковые проверки для Spark и pandas на маленькой партиции."""
import json

import pandas as pd
import pytest

from cleaning import clean_permits_pandas
from cleaning.permits_rules import InvariantError, check_invariant, global_id_checksum, output_dirs, write_report

LOAD_DATE = "2026-10-21"

#  метка  gid  ID  Series  Number  ValidFrom     ValidTo       Zone    Status           CancelDate
ROWS = [
    ("valid", 1, 1, "БА", "0001", "01.01.2020", "31.12.2020", "СК", "Выдан", ""),
    ("dup_later", 11, 2, "БА", "0002", "01.02.2020", "31.01.2021", "ТТК", "Выдан", ""),
    ("dup_kept", 10, 2, "БА", "0002", "01.02.2020", "31.01.2021", "ТТК", "Выдан", ""),
    ("conflict_a", 20, 3, "БА", "0003", "01.01.2021", "31.12.2021", "СК", "Выдан", ""),
    ("conflict_b", 21, 3, "БА", "0003", "01.01.2021", "30.12.2021", "СК", "Выдан", ""),
    ("range", 30, 4, "БА", "0004", "01.03.2021", "01.02.2021", "СК", "Выдан", ""),
    ("iso_date", 40, 5, "БА", "0005", "2021-03-01", "01.03.2022", "СК", "Выдан", ""),
    ("feb31", 41, 6, "БА", "0006", "01.01.2020", "31.12.2020", "СК", "Аннулирован", "31.02.2020"),
    ("short_date_bad_zone", 42, 7, "БА", "0007", "1.02.2020", "31.12.2020", "ЦАО", "Выдан", ""),
    ("cancel_before", 50, 8, "ББ", "0008", "01.06.2020", "31.05.2021", "МКАД", "Аннулирован", "01.01.2020"),
    ("issued_cancelled", 60, 9, "ББ", "0009", "01.01.2020", "31.12.2020", "СК", "Выдан", "05.05.2020"),
    ("annulled_open", 61, 10, "ББ", "0010", "01.01.2020", "31.12.2020", "СК", "Аннулирован", ""),
    ("bad_zone", 70, 11, "ББ", "0011", "01.01.2020", "31.12.2020", "ЦАО", "Выдан", ""),
    ("bad_status", 71, 12, "ББ", "0012", "01.01.2020", "31.12.2020", "СК", "Приостановлен", ""),
    ("bad_zone_and_status", 80, 13, "ББ", "0013", "01.01.2020", "31.12.2020", "ЦАО", "X", ""),
    ("rejected_twin_1", 100, 15, "ВВ", "0015", "01.01.2020", "31.12.2020", "ЦАО", "Выдан", ""),
    ("rejected_twin_2", 101, 15, "ВВ", "0015", "01.01.2020", "31.12.2020", "ЦАО", "Выдан", ""),
    ("no_cancel_key", 110, 16, "ВВ", "0016", "01.01.2023", "31.12.2023", "ТТК", "Выдан", None),
]
GID = {label: gid for label, gid, *_ in ROWS}
EXPECTED_REJECTED = {
    "dup_later": "duplicate_id",
    "range": "invalid_validity_range",
    "iso_date": "unparseable_date",
    "feb31": "unparseable_date",
    "short_date_bad_zone": "unparseable_date",  # дата важнее зоны
    "bad_zone": "unknown_zone",
    "bad_zone_and_status": "unknown_zone",  # зона важнее статуса
    "rejected_twin_1": "unknown_zone",  # одинаковые отклонённые строки не схлопываются в duplicate_id
    "rejected_twin_2": "unknown_zone",
    "bad_status": "unknown_status",
}
FIELDS = ("global_id", "ID", "Series", "Number", "ValidFrom", "ValidTo", "Zone", "Status", "CancelDate")


def write_page(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for n, (_, *values) in enumerate(rows, start=1):
            cells = {k: v for k, v in zip(FIELDS, values) if v is not None}  # None - поля нет вовсе
            # внешний Number - позиция в выдаче, внешний global_id намеренно другой: берутся поля Cells
            f.write(json.dumps({"global_id": -cells["global_id"], "Number": n, "Cells": cells}, ensure_ascii=False) + "\n")


@pytest.fixture
def data_root(tmp_path):
    root = tmp_path / "data"
    part = root / "raw" / "permits" / f"load_date={LOAD_DATE}"
    write_page(part / "page_000000.jsonl", ROWS[:9])
    write_page(part / "page_000001.jsonl", ROWS[9:])
    (part / "_manifest.json").write_text('{"rows": 18}', encoding="utf-8")
    # соседняя партиция не должна попасть в чтение
    write_page(root / "raw" / "permits" / "load_date=2026-10-20" / "page_000000.jsonl", [("other", 999, 999, "ZZ", "1", "01.01.2020", "31.12.2020", "СК", "Выдан", "")])
    return root


@pytest.fixture(params=["spark", "pandas"])
def result(request, data_root, tmp_path):
    out_root = tmp_path / "out"
    if request.param == "spark":
        from spark_jobs.clean_permits import run

        report = run(request.getfixturevalue("spark"), data_root, out_root, LOAD_DATE)
    else:
        report = clean_permits_pandas.run(data_root, out_root, LOAD_DATE)
    dirs = output_dirs(out_root, LOAD_DATE)
    clean = pd.read_parquet(dirs["clean"]).set_index("global_id")
    rejected = pd.read_parquet(dirs["rejected"]).set_index("global_id")
    return report, clean, rejected, dirs


def test_valid_row_is_clean_with_types_and_no_flags(result):
    _, clean, _, _ = result
    row = clean.loc[GID["valid"]]
    assert row["id"] == 1 and row["zone"] == "СК" and row["status"] == "Выдан"
    assert str(row["valid_from"])[:10] == "2020-01-01" and str(row["valid_to"])[:10] == "2020-12-31"
    assert pd.isna(row["cancel_date"])
    assert not row["cancelled_before_start"] and not row["status_cancel_mismatch"] and not row["id_conflict"]
    assert int(row["valid_from_year"]) == 2020


def test_identical_duplicate_keeps_min_global_id(result):
    _, clean, rejected, _ = result
    assert GID["dup_kept"] in clean.index and not clean.loc[GID["dup_kept"], "id_conflict"]
    assert rejected.loc[GID["dup_later"], "reason"] == "duplicate_id"


def test_same_id_different_fields_is_id_conflict_not_dropped(result):
    _, clean, rejected, _ = result
    for label in ("conflict_a", "conflict_b"):
        assert clean.loc[GID[label], "id_conflict"]
        assert GID[label] not in rejected.index


def test_rejected_reasons_and_priority(result):
    _, clean, rejected, _ = result
    assert {gid: reason for gid, reason in rejected["reason"].items()} == {GID[k]: v for k, v in EXPECTED_REJECTED.items()}
    assert not set(clean.index) & set(rejected.index)
    # в rejected лежат исходные строки дат
    assert rejected.loc[GID["iso_date"], "valid_from_raw"] == "2021-03-01"
    assert rejected.loc[GID["feb31"], "cancel_date_raw"] == "31.02.2020"


def test_invalid_validity_range(result):
    assert result[2].loc[GID["range"], "reason"] == "invalid_validity_range"


def test_unparseable_date(result):
    rejected = result[2]
    for label in ("iso_date", "feb31", "short_date_bad_zone"):
        assert rejected.loc[GID[label], "reason"] == "unparseable_date"


def test_cancelled_before_start_flag(result):
    _, clean, _, _ = result
    row = clean.loc[GID["cancel_before"]]
    assert row["cancelled_before_start"] and not row["status_cancel_mismatch"]
    assert clean["cancelled_before_start"].sum() == 1


def test_status_cancel_mismatch_flag(result):
    _, clean, _, _ = result
    assert clean.loc[GID["issued_cancelled"], "status_cancel_mismatch"]
    assert clean.loc[GID["annulled_open"], "status_cancel_mismatch"]
    assert not clean.loc[GID["no_cancel_key"], "status_cancel_mismatch"]  # нет ключа CancelDate = пустая
    assert clean["status_cancel_mismatch"].sum() == 2


def test_report_and_invariant(result):
    report, clean, rejected, dirs = result
    assert report["rows_raw"] == 18  # только своя партиция, _manifest.json не читается
    assert report["rows_clean"] == len(clean) == 8
    assert report["rows_rejected"] == len(rejected) == 10
    assert report["invariant_ok"] and report["rows_raw"] == report["rows_clean"] + report["rows_rejected"]
    assert report["rejected_by_reason"] == {
        "unparseable_date": 3, "invalid_validity_range": 1, "unknown_zone": 4, "unknown_status": 1, "duplicate_id": 1,
    }
    assert report["flags"] == {"cancelled_before_start": 1, "status_cancel_mismatch": 2, "id_conflict": 2}
    assert report["clean_global_id_sha256"] == global_id_checksum(clean.index)
    assert json.loads((dirs["quality"] / "quality_report.json").read_text(encoding="utf-8")) == report
    assert "| raw = clean + rejected | да |" in (dirs["quality"] / "quality_report.md").read_text(encoding="utf-8")


def test_clean_is_partitioned_by_valid_from_year(result):
    dirs = result[3]
    years = sorted(p.name for p in dirs["clean"].glob("valid_from_year=*"))
    assert years == ["valid_from_year=2020", "valid_from_year=2021", "valid_from_year=2023"]


def test_invariant_violation_fails(tmp_path):
    with pytest.raises(InvariantError):
        check_invariant(10, 7, 2)
    report = {"rows_raw": 10, "rows_clean": 7, "rows_rejected": 2, "invariant_ok": False, "load_date": LOAD_DATE,
              "engine": "test", "source": "x", "rejected_by_reason": {}, "flags": {}, "clean_global_id_sha256": ""}
    with pytest.raises(InvariantError):
        write_report(tmp_path / "q", report)
    assert (tmp_path / "q" / "quality_report.json").exists()  # отчёт пишется и при провале - для разбора


def test_spark_and_pandas_agree(spark, data_root, tmp_path):
    from spark_jobs.clean_permits import run

    by_spark = run(spark, data_root, tmp_path / "s", LOAD_DATE)
    by_pandas = clean_permits_pandas.run(data_root, tmp_path / "p", LOAD_DATE)
    drop = ("engine",)
    assert {k: v for k, v in by_spark.items() if k not in drop} == {k: v for k, v in by_pandas.items() if k not in drop}
