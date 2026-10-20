"""Запись партиции data/raw/<dataset>/load_date=YYYY-MM-DD/.

Страницы пишутся во временную папку, и только после успешной загрузки
она заменяет старую партицию. Упавшая загрузка не портит прошлые данные,
повторный запуск за ту же дату полностью перезаписывает партицию.

Если включено keep_staging_on_error, временная папка после падения остаётся
вместе с файлом прогресса, и загрузку можно продолжить (--resume).
"""
from __future__ import annotations

import json
import os
import shutil
from datetime import date
from pathlib import Path

MANIFEST = "_manifest.json"  # Spark пропускает файлы, начинающиеся с "_"
PROGRESS = "_progress.json"


def partition_dir(raw_root: Path, dataset_name: str, load_date: date) -> Path:
    return Path(raw_root) / dataset_name / f"load_date={load_date.isoformat()}"


def page_name(page_no: int) -> str:
    return f"page_{page_no:06d}.jsonl"


def _write_json_atomic(path: Path, data: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


class PartitionWriter:
    def __init__(self, raw_root: Path, dataset_name: str, load_date: date, keep_staging_on_error: bool = False):
        self.target = partition_dir(raw_root, dataset_name, load_date)
        # точка в начале: Spark и Hive не читают скрытые папки
        self.staging = self.target.with_name(f".{self.target.name}.staging")
        self.keep_staging_on_error = keep_staging_on_error
        self.state: dict | None = None  # что пишется в файл прогресса помимо числа страниц
        self.pages = 0
        self.rows = 0

    def __enter__(self) -> "PartitionWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None and not self.keep_staging_on_error:
            shutil.rmtree(self.staging, ignore_errors=True)

    def read_progress(self) -> dict | None:
        try:
            return json.loads((self.staging / PROGRESS).read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            return None

    def _write_progress(self) -> None:
        if self.state is not None:
            _write_json_atomic(self.staging / PROGRESS, {**self.state, "pages": self.pages})

    def reset(self, state: dict | None = None) -> None:
        """Начать с нуля: пустая временная папка."""
        shutil.rmtree(self.staging, ignore_errors=True)
        self.staging.mkdir(parents=True)
        self.state = state
        self.pages = 0
        self.rows = 0
        self._write_progress()

    def resume(self, state: dict, from_page: int) -> None:
        """Продолжить: страницы с номером >= from_page удаляются и будут загружены заново."""
        for path in self.staging.glob("page_*.jsonl"):
            if int(path.stem.split("_")[1]) >= from_page:
                path.unlink()
        kept = [self.staging / page_name(i) for i in range(from_page)]
        if not all(p.exists() for p in kept):
            raise FileNotFoundError("во временной папке не хватает страниц для продолжения")
        self.state = state
        self.pages = from_page
        self.rows = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in kept)
        self._write_progress()

    def write_page(self, rows: list[dict]) -> None:
        path = self.staging / page_name(self.pages)
        with path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False))
                f.write("\n")
        self.pages += 1
        self.rows += len(rows)
        self._write_progress()  # после страницы: прогресс не обгоняет данные

    def commit(self, manifest: dict) -> Path:
        (self.staging / PROGRESS).unlink(missing_ok=True)
        manifest = {**manifest, "pages": self.pages, "rows": self.rows}
        (self.staging / MANIFEST).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        old = self.target.with_name(f".{self.target.name}.old")
        shutil.rmtree(old, ignore_errors=True)
        if self.target.exists():
            self.target.rename(old)
        self.staging.rename(self.target)
        shutil.rmtree(old, ignore_errors=True)
        return self.target
