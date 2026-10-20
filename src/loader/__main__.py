"""Запуск: python -m loader --mode api|local_files [--load-date YYYY-MM-DD] [--resume]"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import date

from .api_loader import load_from_api
from .config import LoaderConfig
from .local_loader import load_from_local_files

log = logging.getLogger("loader")

_KEY_IN_TEXT = re.compile(r"(api_key=)[^&\s'\"]+")


def redact(text: str, key: str = "") -> str:
    """Убирает ключ из текста: и как параметр api_key=..., и само значение где угодно."""
    text = _KEY_IN_TEXT.sub(r"\1***", text)
    return text.replace(key, "***") if key else text


class RedactingFilter(logging.Filter):
    """Фильтр на обработчике логов: ни одна запись не уйдёт с ключом."""

    def __init__(self, key: str = ""):
        super().__init__()
        self.key = key

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        clean = redact(message, self.key)
        if clean != message:
            record.msg, record.args = clean, ()
        record.exc_info = record.exc_text = None  # traceback целиком не пишем
        return True


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Загрузка набора data.mos.ru в data/raw/")
    parser.add_argument("--mode", choices=["api", "local_files"], required=True)
    parser.add_argument("--load-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--max-pages", type=int, default=None, help="пробный прогон: только первые N страниц")
    parser.add_argument(
        "--count-tolerance", type=int, default=None,
        help="допустимая нехватка строк относительно /count (перекрывает MOS_COUNT_TOLERANCE)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="продолжить упавшую загрузку, если версия набора и max_pages не изменились",
    )
    args = parser.parse_args(argv)
    if args.resume and args.mode != "api":
        parser.error("--resume поддерживается только в режиме api")

    overrides = {"max_pages": args.max_pages}
    if args.count_tolerance is not None:
        overrides["count_tolerance"] = args.count_tolerance
    config = LoaderConfig.from_env(**overrides)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", force=True)
    for handler in logging.getLogger().handlers:
        handler.addFilter(RedactingFilter(config.api_key))

    try:
        if args.mode == "api":
            path = load_from_api(config, args.load_date, resume=args.resume)
        else:
            path = load_from_local_files(config, args.load_date)
    except Exception as exc:  # noqa: BLE001 - верхний уровень: только тип и сообщение, без traceback
        log.error("загрузка не удалась: %s: %s", type(exc).__name__, redact(str(exc), config.api_key))
        sys.exit(1)
    log.info("партиция записана: %s", path)


if __name__ == "__main__":
    main()
