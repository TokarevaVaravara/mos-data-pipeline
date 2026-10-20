import json
import shutil
from dataclasses import replace
from datetime import date

import pytest

from conftest import FIXTURES
from loader import LocalSourceError, load_from_local_files

DAY = date(2026, 10, 20)
API_ROWS = json.loads((FIXTURES / "api_rows.json").read_text(encoding="utf-8"))


def rows_of(path):
    return [json.loads(line) for p in sorted(path.glob("page_*.jsonl")) for line in p.read_text(encoding="utf-8").splitlines()]


@pytest.mark.parametrize("name", ["export.csv", "export.json", "api_rows.json"])
def test_local_file_gives_same_rows_as_api(config, name):
    config.local_source_dir.mkdir(parents=True)
    shutil.copy(FIXTURES / name, config.local_source_dir)

    path = load_from_local_files(config, DAY)

    assert path == config.raw_root / "permits" / "load_date=2026-10-20"
    assert rows_of(path) == API_ROWS
    assert json.loads((path / "_manifest.json").read_text())["source_files"] == [name]


def test_local_files_are_paged_and_rerun_overwrites(config):
    config.local_source_dir.mkdir(parents=True)
    shutil.copy(FIXTURES / "export.csv", config.local_source_dir)
    shutil.copy(FIXTURES / "export.json", config.local_source_dir / "export2.json")

    path = load_from_local_files(replace(config, page_size=2), DAY)
    assert [p.name for p in sorted(path.glob("page_*.jsonl"))] == ["page_000000.jsonl", "page_000001.jsonl", "page_000002.jsonl"]
    assert len(rows_of(path)) == 6

    (config.local_source_dir / "export2.json").unlink()
    path = load_from_local_files(replace(config, page_size=2), DAY)
    assert len(rows_of(path)) == 3 and len(list(path.glob("page_*.jsonl"))) == 2


def test_partition_subfolders_in_source_dir_are_ignored(config):
    # по умолчанию источник и партиции лежат в одном data/raw/
    cfg = replace(config, local_source_dir=config.raw_root)
    config.raw_root.mkdir(parents=True)
    shutil.copy(FIXTURES / "export.json", config.raw_root)
    load_from_local_files(cfg, DAY)
    # второй запуск: рядом с export.json уже лежит permits/load_date=.../, его не читаем
    assert len(rows_of(load_from_local_files(cfg, DAY))) == 3


def test_empty_source_dir_is_an_error(config):
    config.local_source_dir.mkdir(parents=True)
    with pytest.raises(LocalSourceError):
        load_from_local_files(config, DAY)
