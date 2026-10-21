"""Проверки и журнал пайплайна: check-version, validate-raw, quality-gate, record-success.

Запуск: PYTHONPATH=src python -m pipeline.ops <команда> --load-date YYYY-MM-DD [...]
Результат - одна строка JSON в stdout; ошибка проверки - исключение и код выхода 1.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

from cleaning.permits_rules import DATASET, output_dirs, raw_partition
from db.settings import PgSettings
from db.sql_files import read_sql

MANIFEST = "_manifest.json"


class CheckFailed(RuntimeError):
    pass


def read_manifest(data_root: Path, load_date: str) -> dict:
    path = raw_partition(data_root, load_date) / MANIFEST
    if not path.is_file():
        raise CheckFailed(f"нет {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def read_quality(data_root: Path, load_date: str) -> dict:
    path = output_dirs(data_root, load_date)["quality"] / "quality_report.json"
    if not path.is_file():
        raise CheckFailed(f"нет {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_ops_table(conn) -> None:
    conn.execute(read_sql("ddl/004_ops.sql"))


def last_success(conn) -> dict | None:
    row = conn.execute(
        "SELECT load_date, dataset_version FROM ops.load_log ORDER BY last_success_at DESC, load_date DESC LIMIT 1"
    ).fetchone()
    return {"load_date": row[0].isoformat(), "dataset_version": row[1]} if row else None


def check_version(settings: PgSettings, data_root: Path, load_date: str, skip_load: bool) -> dict:
    """proceed=False, если версия набора совпадает с последней успешной в ops.load_log."""
    if skip_load:
        manifest = read_manifest(data_root, load_date)
        current = {"version": manifest["version_after"], "count": manifest["count_reported"], "source": "manifest"}
    else:
        from loader import LoaderConfig, MosApiClient  # ключ нужен только здесь

        meta = MosApiClient(LoaderConfig.from_env()).get_meta()
        current = {"version": meta.version, "count": meta.count, "source": "api"}
    with settings.connect() as conn:
        ensure_ops_table(conn)
        last = last_success(conn)
    return {**current, "last_success": last, "proceed": last is None or last["dataset_version"] != current["version"]}


def validate_raw(data_root: Path, load_date: str, tolerance: int) -> dict:
    manifest = read_manifest(data_root, load_date)
    part = raw_partition(data_root, load_date)
    pages = sorted(part.glob("page_*.jsonl"))
    rows = 0
    for page in pages:
        with page.open("rb") as f:
            rows += sum(1 for _ in f)
    problems = []
    if manifest.get("load_date") != load_date:
        problems.append(f"load_date в манифесте {manifest.get('load_date')} != {load_date}")
    if manifest["rows"] != rows:
        problems.append(f"строк в манифесте {manifest['rows']}, в файлах {rows}")
    if manifest["pages"] != len(pages):
        problems.append(f"страниц в манифесте {manifest['pages']}, файлов {len(pages)}")
    if manifest["version_before"] != manifest["version_after"]:
        problems.append(f"версия менялась: {manifest['version_before']} -> {manifest['version_after']}")
    shortfall = manifest["expected_rows"] - manifest["rows"]
    if not 0 <= shortfall <= tolerance:
        problems.append(f"нехватка {shortfall} вне допуска 0..{tolerance}")
    if problems:
        raise CheckFailed("; ".join(problems))
    return {"rows": rows, "pages": len(pages), "version": manifest["version_after"], "shortfall": shortfall,
            "tolerance": tolerance}


def quality_gate(data_root: Path, load_date: str, max_rejected_share: float) -> dict:
    report = read_quality(data_root, load_date)
    share = report["rows_rejected"] / report["rows_raw"] if report["rows_raw"] else 1.0
    problems = []
    if report["load_date"] != load_date:
        problems.append(f"отчёт за {report['load_date']}, а не за {load_date}")
    if report["rows_raw"] != report["rows_clean"] + report["rows_rejected"]:
        problems.append(f"raw {report['rows_raw']} != clean {report['rows_clean']}"
                        f" + rejected {report['rows_rejected']}")
    if share > max_rejected_share:
        problems.append(f"доля rejected {share:.4%} больше {max_rejected_share:.2%}")
    if problems:
        raise CheckFailed("; ".join(problems))
    return {"rows_raw": report["rows_raw"], "rows_clean": report["rows_clean"],
            "rows_rejected": report["rows_rejected"], "rejected_share": round(share, 6)}


def record_success(settings: PgSettings, data_root: Path, load_date: str, dag_run_id: str | None) -> dict:
    manifest, report = read_manifest(data_root, load_date), read_quality(data_root, load_date)
    row = {
        "load_date": load_date, "dataset_version": manifest["version_after"],
        "count_reported": manifest["count_reported"],
        "rows_raw": report["rows_raw"], "rows_clean": report["rows_clean"], "rows_rejected": report["rows_rejected"],
        "dag_run_id": dag_run_id,
    }
    with settings.connect() as conn, conn.transaction():
        ensure_ops_table(conn)
        conn.execute(
            """
            INSERT INTO ops.load_log (load_date, dataset_version, count_reported, rows_raw, rows_clean, rows_rejected,
                                      dag_run_id)
            VALUES (%(load_date)s, %(dataset_version)s, %(count_reported)s, %(rows_raw)s, %(rows_clean)s,
                    %(rows_rejected)s, %(dag_run_id)s)
            ON CONFLICT (load_date) DO UPDATE SET
                dataset_version = EXCLUDED.dataset_version, count_reported = EXCLUDED.count_reported,
                rows_raw = EXCLUDED.rows_raw, rows_clean = EXCLUDED.rows_clean, rows_rejected = EXCLUDED.rows_rejected,
                dag_run_id = EXCLUDED.dag_run_id, last_success_at = now()
            """,
            row,
        )
    return row


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=f"Проверки и журнал пайплайна ({DATASET})")
    parser.add_argument("command", choices=["check-version", "validate-raw", "quality-gate", "record-success"])
    parser.add_argument("--load-date", type=date.fromisoformat, required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"))
    parser.add_argument("--skip-load", action="store_true", help="check-version: версия из манифеста, без API")
    parser.add_argument("--tolerance", type=int, default=0, help="validate-raw: допустимая нехватка строк")
    parser.add_argument("--max-rejected-share", type=float, default=0.01, help="quality-gate: доля rejected")
    parser.add_argument("--dag-run-id", default=None)
    args = parser.parse_args(argv)
    load_date = args.load_date.isoformat()

    try:
        if args.command == "check-version":
            result = check_version(PgSettings.from_env(), args.data_root, load_date, args.skip_load)
        elif args.command == "validate-raw":
            result = validate_raw(args.data_root, load_date, args.tolerance)
        elif args.command == "quality-gate":
            result = quality_gate(args.data_root, load_date, args.max_rejected_share)
        else:
            result = record_success(PgSettings.from_env(), args.data_root, load_date, args.dag_run_id)
    except CheckFailed as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        sys.exit(1)
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
