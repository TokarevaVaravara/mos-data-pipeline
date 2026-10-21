"""Продолжение упавшей загрузки (--resume)."""
import json
from dataclasses import replace
from datetime import date

import pytest

from conftest import FakeApi
from loader import ApiError, load_from_api
from loader.raw_writer import PROGRESS

DAY = date(2026, 10, 20)


def read_partition(path):
    pages = sorted(path.glob("page_*.jsonl"))
    return [p.name for p in pages], [json.loads(line) for p in pages for line in p.read_text(encoding="utf-8").splitlines()]


def staging_of(config):
    return config.raw_root / "permits" / ".load_date=2026-10-20.staging"


def crash_at(config, make_client, api, skip):
    """Загрузка, которая падает на странице с этим $skip."""
    api.fail_at_skip = skip
    with pytest.raises(ApiError):
        load_from_api(config, DAY, make_client(api))
    api.fail_at_skip = None
    api.calls.clear()


def skips(api):
    return [c["$skip"] for c in api.rows_calls()]


def test_failed_load_keeps_hidden_staging_with_progress(config, make_client):
    api = FakeApi(rows=3500)
    crash_at(config, make_client, api, skip=2000)

    staging = staging_of(config)
    assert staging.name.startswith(".")  # Spark скрытые папки не читает
    progress = json.loads((staging / PROGRESS).read_text())
    assert progress == {"dataset_id": 3247, "version": "1.0", "count": 3500, "max_pages": None, "page_size": 1000, "pages": 2}
    assert sorted(p.name for p in staging.glob("page_*.jsonl")) == ["page_000000.jsonl", "page_000001.jsonl"]
    assert not (config.raw_root / "permits" / "load_date=2026-10-20").exists()


def test_resume_same_version_gives_same_result_as_fresh_load(config, make_client, tmp_path):
    api = FakeApi(rows=3500)
    crash_at(config, make_client, api, skip=2000)

    path = load_from_api(config, DAY, make_client(api), resume=True)
    assert skips(api) == [1000, 2000, 3000]  # последняя записанная страница (skip=1000) загружена заново

    fresh_cfg = replace(config, raw_root=tmp_path / "fresh")
    fresh = load_from_api(fresh_cfg, DAY, make_client(FakeApi(rows=3500), fresh_cfg))
    assert read_partition(path) == read_partition(fresh)

    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["resumed"] is True and manifest["resumed_from_page"] == 1
    assert manifest["rows"] == 3500 and manifest["shortfall"] == 0
    assert not staging_of(config).exists() and not (path / PROGRESS).exists()


def test_version_changed_between_runs_loads_from_scratch(config, make_client):
    api = FakeApi(rows=3500)
    crash_at(config, make_client, api, skip=2000)

    api.versions = ["1.1"]
    path = load_from_api(config, DAY, make_client(api), resume=True)

    assert skips(api) == [0, 1000, 2000, 3000]
    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["resumed"] is False and manifest["resumed_from_page"] is None
    assert manifest["version_before"] == "1.1" and manifest["rows"] == 3500


def test_progress_from_trial_run_is_not_accepted(config, make_client):
    api = FakeApi(rows=5000)
    crash_at(replace(config, max_pages=3), make_client, api, skip=2000)  # пробный прогон упал

    path = load_from_api(config, DAY, make_client(api), resume=True)  # полная загрузка, max_pages=None
    assert skips(api)[0] == 0
    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["resumed"] is False and manifest["max_pages"] is None and manifest["rows"] == 5000


def test_without_resume_flag_staging_is_discarded(config, make_client):
    api = FakeApi(rows=3500)
    crash_at(config, make_client, api, skip=2000)

    path = load_from_api(config, DAY, make_client(api))
    assert skips(api)[0] == 0
    assert json.loads((path / "_manifest.json").read_text())["resumed"] is False


def test_resume_without_staging_loads_from_scratch(config, make_client):
    api = FakeApi(rows=1500)
    path = load_from_api(config, DAY, make_client(api), resume=True)
    assert skips(api) == [0, 1000]
    assert json.loads((path / "_manifest.json").read_text())["resumed"] is False


def test_resume_drops_pages_written_after_progress(config, make_client):
    # страница записана, а прогресс обновить не успели: файл есть, в прогрессе его нет
    api = FakeApi(rows=3500)
    crash_at(config, make_client, api, skip=3000)
    staging = staging_of(config)
    progress = json.loads((staging / PROGRESS).read_text())
    (staging / PROGRESS).write_text(json.dumps({**progress, "pages": 2}))

    path = load_from_api(config, DAY, make_client(api), resume=True)
    assert skips(api) == [1000, 2000, 3000]
    assert read_partition(path)[1] == api.data
