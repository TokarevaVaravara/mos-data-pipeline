"""Режим api: постраничная выгрузка набора в raw-партицию."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

from .api_client import MosApiClient
from .config import LoaderConfig
from .raw_writer import PartitionWriter

log = logging.getLogger(__name__)


class LoadValidationError(RuntimeError):
    pass


class _VersionChanged(Exception):
    pass


def _fetch_all(client: MosApiClient, config: LoaderConfig, writer: PartitionWriter, expected: int) -> None:
    skip = 0
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


def load_from_api(config: LoaderConfig, load_date: date, client: MosApiClient | None = None) -> Path:
    client = client or MosApiClient(config)
    with PartitionWriter(config.raw_root, config.dataset_name, load_date) as writer:
        for attempt in range(config.max_version_restarts + 1):
            before = client.get_meta()
            log.info("набор %s: версия %s, строк %d", config.dataset_id, before.version, before.count)
            writer.reset()
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
                "load_date": load_date.isoformat(),
                "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        )
