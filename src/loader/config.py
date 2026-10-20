"""Настройки загрузчика. Ключ API берётся только из переменной окружения."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

API_MAX_TOP = 1000  # apidata.mos.ru отвечает 400 на $top > 1000


@dataclass(frozen=True)
class LoaderConfig:
    dataset_id: int = 3247
    dataset_name: str = "permits"  # имя папки в data/raw/
    base_url: str = "https://apidata.mos.ru/v1"
    api_key: str = field(default="", repr=False)  # не печатаем ключ в repr/логах

    page_size: int = API_MAX_TOP
    request_pause: float = 0.5  # пауза между успешными запросами, сек
    timeout: float = 60.0

    max_retries: int = 5  # повторы на 429 / 5xx / сетевые ошибки / оборванные ответы
    backoff_base: float = 5.0  # пауза перед повтором: base * 2**attempt (5, 10, 20, 40, 60)
    backoff_max: float = 60.0
    max_version_restarts: int = 3  # сколько раз перезапускать загрузку при смене версии
    max_pages: int | None = None  # пробный прогон: остановиться после N страниц
    count_tolerance: int = 0  # на сколько строк меньше /count допустимо получить (0 - строго)

    raw_root: Path = Path("data/raw")  # сюда пишутся партиции
    local_source_dir: Path = Path("data/raw")  # отсюда режим local_files читает CSV/JSON

    def __post_init__(self) -> None:
        if not 1 <= self.page_size <= API_MAX_TOP:
            raise ValueError(f"page_size должен быть в диапазоне 1..{API_MAX_TOP}")
        if self.count_tolerance < 0:
            raise ValueError("count_tolerance не может быть отрицательным")

    @classmethod
    def from_env(cls, **overrides) -> "LoaderConfig":
        env = os.environ
        values = {
            "api_key": env.get("MOS_API_KEY", ""),
            "dataset_id": int(env.get("MOS_DATASET_ID", cls.dataset_id)),
        }
        if "MOS_REQUEST_PAUSE" in env:
            values["request_pause"] = float(env["MOS_REQUEST_PAUSE"])
        if "MOS_COUNT_TOLERANCE" in env:
            values["count_tolerance"] = int(env["MOS_COUNT_TOLERANCE"])
        if "MOS_RAW_ROOT" in env:
            values["raw_root"] = Path(env["MOS_RAW_ROOT"])
            values["local_source_dir"] = Path(env["MOS_RAW_ROOT"])
        values.update(overrides)
        return cls(**values)
