"""Режим local_files: CSV/JSON-выгрузки с data.mos.ru -> та же raw-партиция, что у API.

На выходе строки того же вида, что отдаёт /rows:
{"global_id": int, "Number": <номер строки на странице>, "Cells": {...}}.
Целые поля приводятся к int, пустые значения - к "", как в API.
"""
from __future__ import annotations

import csv
import io
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .config import LoaderConfig
from .raw_writer import PartitionWriter

INT_FIELDS = {"ID", "global_id"}
ENCODINGS = ("utf-8-sig", "cp1251")  # выгрузки портала встречаются в обеих кодировках


class LocalSourceError(RuntimeError):
    pass


def _read_text(path: Path) -> str:
    raw = path.read_bytes()
    for encoding in ENCODINGS:
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise LocalSourceError(f"{path.name}: не удалось определить кодировку")


def _normalize(record: dict) -> dict:
    cells = {}
    for key, value in record.items():
        if key is None or not key.strip():
            continue  # хвостовой разделитель в CSV даёт пустую колонку
        value = "" if value is None else value
        if key in INT_FIELDS and value != "":
            value = int(value)
        cells[key.strip()] = value
    if "global_id" not in cells or cells["global_id"] == "":
        raise LocalSourceError(f"в записи нет global_id: {record}")
    return cells


def _records_from_json(text: str, name: str) -> Iterator[dict]:
    data = json.loads(text)
    if isinstance(data, dict):  # как в ответах API, список может быть обёрнут
        data = next((v for v in data.values() if isinstance(v, list)), None)
    if not isinstance(data, list):
        raise LocalSourceError(f"{name}: ожидался список записей")
    for item in data:
        yield item["Cells"] if "Cells" in item else item  # API-формат или плоская выгрузка


def _records_from_csv(text: str) -> Iterator[dict]:
    delimiter = ";" if text.split("\n", 1)[0].count(";") >= text.split("\n", 1)[0].count(",") else ","
    for row in csv.DictReader(io.StringIO(text), delimiter=delimiter):
        # в выгрузках портала вторая строка - русские подписи колонок
        if not (row.get("global_id") or "").strip().isdigit():
            continue
        yield row


def iter_source_files(source_dir: Path) -> list[Path]:
    files = sorted(p for p in Path(source_dir).iterdir() if p.is_file() and p.suffix.lower() in {".csv", ".json"})
    if not files:
        raise LocalSourceError(f"в {source_dir} нет файлов .csv/.json")
    return files


def iter_cells(files: Iterable[Path]) -> Iterator[dict]:
    for path in files:
        text = _read_text(path)
        records = _records_from_json(text, path.name) if path.suffix.lower() == ".json" else _records_from_csv(text)
        for record in records:
            yield _normalize(record)


def load_from_local_files(config: LoaderConfig, load_date: date) -> Path:
    files = iter_source_files(config.local_source_dir)
    with PartitionWriter(config.raw_root, config.dataset_name, load_date) as writer:
        writer.reset()
        page: list[dict] = []
        for cells in iter_cells(files):
            page.append({"global_id": cells["global_id"], "Number": len(page) + 1, "Cells": cells})
            if len(page) == config.page_size:
                writer.write_page(page)
                page = []
        if page:
            writer.write_page(page)
        if writer.rows == 0:
            raise LocalSourceError("в исходных файлах нет ни одной записи")
        return writer.commit(
            {
                "mode": "local_files",
                "dataset_id": config.dataset_id,
                "source_files": [p.name for p in files],
                "load_date": load_date.isoformat(),
                "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
