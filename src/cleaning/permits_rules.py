"""Правила очистки пропусков (набор 3247) и отчёт о качестве - общие для Spark и pandas.

Правила:
- поля берутся из Cells, внешний Number (позиция в выдаче API) не используется;
- даты строго ДД.ММ.ГГГГ: ValidFrom и ValidTo обязательны, пустая CancelDate - null,
  всё остальное, что не разобралось (в т.ч. 31.02.2020, 1.02.2020), - unparseable_date;
- одна причина отказа на строку, по порядку REJECT_REASONS;
- duplicate_id: среди строк, прошедших остальные проверки, с одним ID и совпадающими
  остальными полями Cells (кроме global_id) остаётся строка с минимальным global_id;
- если ID совпал, а поля различаются, строки остаются в clean с флагом id_conflict.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

DATASET = "permits"
DATE_FORMAT_RE = r"^\d{2}\.\d{2}\.\d{4}$"  # Spark и strptime иначе принимают 1.02.2020
ZONES = ("СК", "ТТК", "МКАД")
STATUSES = ("Выдан", "Аннулирован")
ISSUED, ANNULLED = STATUSES

REJECT_REASONS = (
    "unparseable_date",
    "invalid_validity_range",  # ValidTo < ValidFrom
    "unknown_zone",
    "unknown_status",
    "duplicate_id",
)
FLAGS = ("cancelled_before_start", "status_cancel_mismatch", "id_conflict")

# поля Cells, по которым строки считаются одинаковыми при поиске дублей (всё, кроме global_id)
DUPLICATE_KEY = ("ID", "Series", "Number", "ValidFrom", "ValidTo", "Zone", "Status", "CancelDate")


class InvariantError(RuntimeError):
    """raw != clean + rejected"""


def partition_name(load_date: str) -> str:
    return f"load_date={load_date}"


def raw_partition(data_root: Path, load_date: str) -> Path:
    return Path(data_root) / "raw" / DATASET / partition_name(load_date)


def output_dirs(out_root: Path, load_date: str) -> dict[str, Path]:
    part = partition_name(load_date)
    return {layer: Path(out_root) / layer / DATASET / part for layer in ("clean", "rejected", "quality")}


def global_id_checksum(ids) -> str:
    """sha256 отсортированных global_id, по одному в строке: сверка Spark и pandas."""
    digest = hashlib.sha256()
    for gid in sorted(int(x) for x in ids):
        digest.update(f"{gid}\n".encode())
    return digest.hexdigest()


def build_report(
    engine: str, load_date: str, source: Path, rows_raw: int, rows_clean: int, rows_rejected: int,
    rejected_by_reason: dict, flags: dict, clean_checksum: str,
) -> dict:
    return {
        "engine": engine,
        "load_date": load_date,
        "source": str(source),
        "rows_raw": rows_raw,
        "rows_clean": rows_clean,
        "rows_rejected": rows_rejected,
        "invariant_ok": rows_raw == rows_clean + rows_rejected,
        "rejected_by_reason": {r: int(rejected_by_reason.get(r, 0)) for r in REJECT_REASONS},
        "flags": {f: int(flags.get(f, 0)) for f in FLAGS},
        "clean_global_id_sha256": clean_checksum,
    }


def report_markdown(report: dict) -> str:
    lines = [
        f"# Качество очистки: {DATASET}, load_date={report['load_date']} ({report['engine']})",
        "",
        f"Источник: `{report['source']}`",
        "",
        "| слой | строк |",
        "|---|---|",
        f"| raw | {report['rows_raw']} |",
        f"| clean | {report['rows_clean']} |",
        f"| rejected | {report['rows_rejected']} |",
        f"| raw = clean + rejected | {'да' if report['invariant_ok'] else 'НЕТ'} |",
        "",
        "| причина отказа | строк |",
        "|---|---|",
        *(f"| {r} | {n} |" for r, n in report["rejected_by_reason"].items()),
        "",
        "| флаг в clean | строк |",
        "|---|---|",
        *(f"| {f} | {n} |" for f, n in report["flags"].items()),
        "",
        f"sha256 отсортированных global_id в clean: `{report['clean_global_id_sha256']}`",
        "",
    ]
    return "\n".join(lines)


def write_report(quality_dir: Path, report: dict) -> None:
    """Пишет quality_report.json/.md и падает, если raw != clean + rejected."""
    quality_dir.mkdir(parents=True, exist_ok=True)
    (quality_dir / "quality_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    (quality_dir / "quality_report.md").write_text(report_markdown(report), encoding="utf-8")
    check_invariant(report["rows_raw"], report["rows_clean"], report["rows_rejected"])


def check_invariant(rows_raw: int, rows_clean: int, rows_rejected: int) -> None:
    if rows_raw != rows_clean + rows_rejected:
        raise InvariantError(f"raw {rows_raw} != clean {rows_clean} + rejected {rows_rejected}")
