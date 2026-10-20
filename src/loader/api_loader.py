"""Режим api: постраничная выгрузка набора в raw-партицию."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from .api_client import MosApiClient
from .config import LoaderConfig
from .raw_writer import PartitionWriter

log = logging.getLogger(__name__)

# поля файла прогресса, которые должны совпасть, чтобы продолжить загрузку
RESUME_KEYS = ("dataset_id", "version", "max_pages", "page_size")


class LoadValidationError(RuntimeError):
    pass


def _fetch_all(client: MosApiClient, config: LoaderConfig, writer: PartitionWriter, expected: int) -> None:
    skip = writer.pages * config.page_size  # при продолжении - с первой незаписанной страницы
    while True:
        rows = client.get_rows(skip, config.page_size)
        if not rows:
            break
        writer.write_page(rows)
        skip += len(rows)
        log.info("страница %d: %d строк, всего %d из %d", writer.pages, len(rows), skip, expected)
        if len(rows) < config.page_size:
            break  # неполная страница - последняя, лишний запрос не нужен
        if config.max_pages is not None and writer.pages >= config.max_pages:
            break
        if config.request_pause:
            client.sleep(config.request_pause)


def _resume_mismatch(progress: dict | None, state: dict) -> str | None:
    """Почему продолжить нельзя; None - можно."""
    if progress is None:
        return "файла прогресса нет"
    diff = [f"{k}: {progress.get(k)!r} -> {state[k]!r}" for k in RESUME_KEYS if progress.get(k) != state[k]]
    if diff:
        return "не совпадает " + ", ".join(diff)
    if not isinstance(progress.get("pages"), int) or progress["pages"] < 1:
        return "в файле прогресса нет записанных страниц"
    return None


def load_from_api(
    config: LoaderConfig, load_date: date, client: MosApiClient | None = None, resume: bool = False
) -> Path:
    client = client or MosApiClient(config)
    with PartitionWriter(config.raw_root, config.dataset_name, load_date, keep_staging_on_error=True) as writer:
        progress = writer.read_progress() if resume else None
        resumed_from: int | None = None
        for attempt in range(config.max_version_restarts + 1):
            before = client.get_meta()
            log.info("набор %s: версия %s, строк %d", config.dataset_id, before.version, before.count)
            state = {
                "dataset_id": config.dataset_id,
                "version": before.version,
                "count": before.count,
                "max_pages": config.max_pages,
                "page_size": config.page_size,
            }
            reason = _resume_mismatch(progress, state) if resume and attempt == 0 else "без --resume"
            if reason is None:
                # последнюю записанную страницу загружаем заново: вдруг она записана не до конца
                resumed_from = progress["pages"] - 1
                writer.resume(state, resumed_from)
                log.info("продолжение загрузки со страницы %d (уже записано строк: %d)", resumed_from + 1, writer.rows)
            else:
                if resume and attempt == 0:
                    log.warning("продолжить нельзя (%s): загрузка с нуля", reason)
                resumed_from = None
                writer.reset(state)
            _fetch_all(client, config, writer, before.count)
            after = client.get_meta()
            if after.version == before.version and after.count == before.count:
                break
            log.warning(
                "набор изменился во время загрузки: версия %s -> %s, строк %d -> %d; перезапуск %d/%d",
                before.version, after.version, before.count, after.count, attempt + 1, config.max_version_restarts,
            )
        else:
            raise LoadValidationError(
                f"набор менялся во время каждой из {config.max_version_restarts + 1} попыток загрузки"
            )
        expected = before.count
        if config.max_pages is not None:
            expected = min(before.count, config.max_pages * config.page_size)
        shortfall = expected - writer.rows
        if shortfall < 0:
            raise LoadValidationError(
                f"получено {writer.rows} строк - больше, чем ожидалось ({expected}, /count = {before.count})"
            )
        if shortfall > config.count_tolerance:
            raise LoadValidationError(
                f"получено {writer.rows} строк, ожидалось {expected} (/count = {before.count}): "
                f"нехватка {shortfall} больше допуска {config.count_tolerance}"
            )
        if shortfall:
            log.warning("нехватка %d строк в пределах допуска %d", shortfall, config.count_tolerance)
        return writer.commit(
            {
                "mode": "api",
                "dataset_id": config.dataset_id,
                "version_before": before.version,
                "version_after": after.version,
                "count_reported": before.count,
                "expected_rows": expected,
                "tolerance": config.count_tolerance,
                "shortfall": shortfall,
                "max_pages": config.max_pages,
                "resumed": resumed_from is not None,
                # номер страницы в файлах (page_NNNNNN), с которой продолжили; None - загрузка с нуля
                "resumed_from_page": resumed_from,
                "load_date": load_date.isoformat(),
                "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
