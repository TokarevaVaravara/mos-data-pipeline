"""Запись партиции data/raw/<dataset>/load_date=YYYY-MM-DD/.

Страницы пишутся во временную папку, и только после успешной загрузки
она заменяет старую партицию. Упавшая загрузка не портит прошлые данные,
повторный запуск за ту же дату полностью перезаписывает партицию.
"""
from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

MANIFEST = "_manifest.json"  # Spark пропускает файлы, начинающиеся с "_"


def partition_dir(raw_root: Path, dataset_name: str, load_date: date) -> Path:
    return Path(raw_root) / dataset_name / f"load_date={load_date.isoformat()}"


def page_name(page_no: int) -> str:
    return f"page_{page_no:06d}.jsonl"


class PartitionWriter:
    def __init__(self, raw_root: Path, dataset_name: str, load_date: date):
        self.target = partition_dir(raw_root, dataset_name, load_date)
        # точка в начале: Spark и Hive не читают скрытые папки
        self.staging = self.target.with_name(f".{self.target.name}.staging")
        self.pages = 0
        self.rows = 0

    def __enter__(self) -> "PartitionWriter":
        self.reset()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            shutil.rmtree(self.staging, ignore_errors=True)

    def reset(self) -> None:
        shutil.rmtree(self.staging, ignore_errors=True)
        self.staging.mkdir(parents=True)
        self.pages = 0
        self.rows = 0

    def write_page(self, rows: list[dict]) -> None:
        path = self.staging / page_name(self.pages)
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")
        self.pages += 1
        self.rows += len(rows)

    def commit(self, manifest: dict) -> Path:
        manifest = {**manifest, "pages": self.pages, "rows": self.rows}
        (self.staging / MANIFEST).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        old = self.target.with_name(f".{self.target.name}.old")
        shutil.rmtree(old, ignore_errors=True)
        if self.target.exists():
            self.target.rename(old)
        self.staging.rename(self.target)
        shutil.rmtree(old, ignore_errors=True)
        return self.target
