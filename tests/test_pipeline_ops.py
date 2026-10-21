"""Шаги пайплайна (pipeline.ops) и идемпотентность всего пути DAG на маленькой фикстуре.

Нужен запущенный Postgres: используется тестовая база из test_postgres (<POSTGRES_DB>_test).
"""
import json

import pytest

from cleaning import clean_permits_pandas
from pipeline import ops
from test_clean_permits import write_page
from test_postgres import ROWS, SETTINGS, fingerprint

LOAD_DATE = "2026-10-21"


def make_raw(root, raw_rows, version="1.0", count=None, **manifest_overrides):
    part = root / "raw" / "permits" / f"load_date={LOAD_DATE}"
    write_page(part / "page_000000.jsonl", raw_rows)
    count = len(raw_rows) if count is None else count
    manifest = {"mode": "api", "dataset_id": 3247, "version_before": version, "version_after": version,
                "count_reported": count, "expected_rows": count, "tolerance": 2, "shortfall": count - len(raw_rows),
                "load_date": LOAD_DATE, "pages": 1, "rows": len(raw_rows), **manifest_overrides}
    (part / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


@pytest.fixture
def db():
    with SETTINGS.connect(autocommit=True) as conn:
        for schema in ("mart", "clean", "ops"):
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        yield conn


# --- check-version -----------------------------------------------------------

def test_check_version_proceeds_on_empty_log_then_stops_on_same_version(db, tmp_path):
    root = make_raw(tmp_path / "data", ROWS, version="1.0")
    clean_permits_pandas.run(root, root, LOAD_DATE)

    first = ops.check_version(SETTINGS, root, LOAD_DATE, skip_load=True)
    assert first["proceed"] and first["last_success"] is None and first["source"] == "manifest"

    ops.record_success(SETTINGS, root, LOAD_DATE, "run-1")
    second = ops.check_version(SETTINGS, root, LOAD_DATE, skip_load=True)
    assert not second["proceed"]  # та же версия -> ShortCircuit
    assert second["last_success"] == {"load_date": LOAD_DATE, "dataset_version": "1.0"}

    make_raw(tmp_path / "data", ROWS, version="1.1")  # вышла новая версия
    assert ops.check_version(SETTINGS, root, LOAD_DATE, skip_load=True)["proceed"]


# --- validate-raw ------------------------------------------------------------

def test_validate_raw_ok(tmp_path):
    root = make_raw(tmp_path / "data", ROWS, count=len(ROWS) + 1)
    result = ops.validate_raw(root, LOAD_DATE, tolerance=2)
    assert result == {"rows": len(ROWS), "pages": 1, "version": "1.0", "shortfall": 1, "tolerance": 2}


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"version_after": "1.1"}, "версия менялась"),
        ({"rows": 999}, "строк в манифесте 999"),
        ({"expected_rows": 100}, "вне допуска"),
    ],
    ids=["version_changed", "rows_mismatch", "shortfall_over_tolerance"],
)
def test_validate_raw_fails(tmp_path, overrides, message):
    root = make_raw(tmp_path / "data", ROWS, **overrides)
    with pytest.raises(ops.CheckFailed, match=message):
        ops.validate_raw(root, LOAD_DATE, tolerance=2)


# --- quality-gate ------------------------------------------------------------

def test_quality_gate_passes_on_good_data(tmp_path):
    valid = [("v", 1000 + i, 1000 + i, "ВВ", str(i), "01.01.2024", "31.12.2024", "СК", "Выдан", "") for i in range(199)]
    bad = [("bad", 5000, 5000, "ВВ", "x", "01.02.2024", "01.01.2024", "СК", "Выдан", "")]  # ValidTo < ValidFrom
    root = make_raw(tmp_path / "data", valid + bad)  # 1 из 200 = 0,5 % - в пределах 1 %
    clean_permits_pandas.run(root, root, LOAD_DATE)
    assert ops.quality_gate(root, LOAD_DATE, 0.01) == {
        "rows_raw": 200, "rows_clean": 199, "rows_rejected": 1, "rejected_share": 0.005,
    }


def test_quality_gate_fails_when_rejected_share_too_high(tmp_path):
    root = make_raw(tmp_path / "data", ROWS)  # 2 rejected из 8 = 25 %
    clean_permits_pandas.run(root, root, LOAD_DATE)
    with pytest.raises(ops.CheckFailed, match=r"доля rejected 25\.0000% больше 1\.00%"):
        ops.quality_gate(root, LOAD_DATE, 0.01)


def test_quality_gate_fails_when_invariant_broken(tmp_path):
    root = make_raw(tmp_path / "data", ROWS)
    clean_permits_pandas.run(root, root, LOAD_DATE)
    report_path = root / "quality" / "permits" / f"load_date={LOAD_DATE}" / "quality_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["rows_clean"] -= 1  # потерянная строка
    report_path.write_text(json.dumps(report), encoding="utf-8")
    with pytest.raises(ops.CheckFailed, match="raw 8 != clean 5 \\+ rejected 2"):
        ops.quality_gate(root, LOAD_DATE, max_rejected_share=1.0)


# --- весь путь DAG дважды ------------------------------------------------------

def test_pipeline_twice_for_same_load_date_changes_nothing(spark, db, tmp_path):
    from db.sql_files import apply
    from spark_jobs.clean_permits import run as clean_spark
    from spark_jobs.load_postgres import load

    root = make_raw(tmp_path / "data", ROWS, version="1.0", count=len(ROWS) + 1)

    def pipeline(run_id):
        ops.validate_raw(root, LOAD_DATE, tolerance=2)
        clean_spark(spark, root, root, LOAD_DATE)
        ops.quality_gate(root, LOAD_DATE, max_rejected_share=0.5)  # в фикстуре нарочно много отклонённых
        result = load(spark, SETTINGS, root, LOAD_DATE)
        apply(SETTINGS, "marts")
        ops.record_success(SETTINGS, root, LOAD_DATE, run_id)
        return result

    def snapshot():
        return {
            "permits": fingerprint(db, "permits"),
            "rejected": fingerprint(db, "permits_rejected"),
            "rows": db.execute("SELECT (SELECT count(*) FROM clean.permits), (SELECT count(*) FROM clean.permits_rejected),"
                               " (SELECT count(*) FROM ops.load_log)").fetchone(),
            "marts": db.execute("SELECT md5(string_agg(t::text, '|' ORDER BY zone, day)) FROM mart.daily_permits t")
                       .fetchone()[0],
        }

    first = pipeline("run-1")
    after_first = snapshot()
    second = pipeline("run-2")
    after_second = snapshot()

    assert first["permits"]["inserted"] == 6 and first["rejected"]["inserted"] == 2
    assert second["permits"] == {"inserted": 0, "updated": 0, "removed_from_other": 0}
    assert second["rejected"] == {"inserted": 0, "updated": 0, "removed_from_other": 0}
    assert after_first == after_second
    assert after_second["rows"] == (6, 2, 1)  # одна строка журнала на load_date
    assert db.execute("SELECT dag_run_id, dataset_version FROM ops.load_log").fetchone() == ("run-2", "1.0")
