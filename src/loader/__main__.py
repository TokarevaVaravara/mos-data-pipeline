"""Запуск: python -m loader --mode api|local_files [--load-date YYYY-MM-DD]"""
from __future__ import annotations

import argparse
import logging
from datetime import date

from .api_loader import load_from_api
from .config import LoaderConfig
from .local_loader import load_from_local_files


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Загрузка набора data.mos.ru в data/raw/")
    parser.add_argument("--mode", choices=["api", "local_files"], required=True)
    parser.add_argument("--load-date", type=date.fromisoformat, default=date.today())
    parser.add_argument("--max-pages", type=int, default=None, help="пробный прогон: только первые N страниц")
    parser.add_argument(
        "--count-tolerance", type=int, default=None,
        help="допустимая нехватка строк относительно /count (перекрывает MOS_COUNT_TOLERANCE)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    overrides = {"max_pages": args.max_pages}
    if args.count_tolerance is not None:
        overrides["count_tolerance"] = args.count_tolerance
    config = LoaderConfig.from_env(**overrides)
    if args.mode == "api":
        path = load_from_api(config, args.load_date)
    else:
        path = load_from_local_files(config, args.load_date)
    logging.getLogger("loader").info("партиция записана: %s", path)


if __name__ == "__main__":
    main()
