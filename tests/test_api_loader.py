import json
from dataclasses import replace
from datetime import date

import pytest
import requests

from conftest import TEST_KEY, FakeApi, make_response
from loader import ApiError, LoaderConfig, LoadValidationError, load_from_api
from loader.api_client import parse_retry_after

DAY = date(2026, 10, 20)


def read_partition(path):
    pages = sorted(path.glob("page_*.jsonl"))
    rows = [json.loads(line) for p in pages for line in p.read_text(encoding="utf-8").splitlines()]
    return pages, rows


# --- пагинация -------------------------------------------------------------

def test_pagination_writes_pages_unchanged(config, make_client):
    api = FakeApi(rows=2500)
    path = load_from_api(config, DAY, make_client(api))

    assert path == config.raw_root / "permits" / "load_date=2026-10-20"
    pages, rows = read_partition(path)
    assert [p.name for p in pages] == ["page_000000.jsonl", "page_000001.jsonl", "page_000002.jsonl"]
    assert rows == api.data  # строки записаны как пришли из API
    assert [(c["$skip"], c["$top"]) for c in api.rows_calls()] == [(0, 1000), (1000, 1000), (2000, 1000)]

    manifest = json.loads((path / "_manifest.json").read_text(encoding="utf-8"))
    assert manifest["rows"] == 2500 and manifest["pages"] == 3
    assert manifest["version_before"] == manifest["version_after"] == "1.0"
    assert manifest["count_reported"] == 2500 and manifest["shortfall"] == 0 and manifest["tolerance"] == 0


def test_exact_multiple_of_page_size_stops_on_empty_page(config, make_client):
    api = FakeApi(rows=2000)
    pages, rows = read_partition(load_from_api(config, DAY, make_client(api)))
    assert len(pages) == 2 and len(rows) == 2000
    assert [c["$skip"] for c in api.rows_calls()] == [0, 1000, 2000]


def test_pause_between_requests_comes_from_config(config, make_client, sleeps):
    cfg = replace(config, request_pause=0.5)
    load_from_api(cfg, DAY, make_client(FakeApi(rows=2500), cfg))
    assert sleeps == [0.5, 0.5]  # после 1-й и 2-й страниц; 3-я неполная - последняя


def test_max_pages_limits_trial_run(config, make_client):
    api = FakeApi(rows=5000)
    path = load_from_api(replace(config, max_pages=3), DAY, make_client(api))

    pages, rows = read_partition(path)
    assert len(pages) == 3 and len(rows) == 3000
    assert len(api.rows_calls()) == 3
    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["max_pages"] == 3 and manifest["count_reported"] == 5000
    assert manifest["expected_rows"] == 3000 and manifest["shortfall"] == 0


def test_page_size_above_api_limit_is_rejected():
    with pytest.raises(ValueError):
        LoaderConfig(api_key=TEST_KEY, page_size=1001)


# --- смена версии во время загрузки ------------------------------------------

def test_version_change_restarts_load(config, make_client):
    # до 1-й попытки 1.0, после неё 1.1 -> перезапуск; 2-я попытка 1.1 / 1.1
    api = FakeApi(rows=1500, versions=["1.0", "1.1", "1.1", "1.1"])
    path = load_from_api(config, DAY, make_client(api))

    pages, rows = read_partition(path)
    assert len(rows) == 1500 and len(pages) == 2  # без дублей от первой попытки
    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["version_before"] == manifest["version_after"] == "1.1"
    assert len(api.rows_calls()) == 4  # две полные попытки по 2 страницы


def test_version_keeps_changing_fails_and_keeps_old_partition(config, make_client):
    good = load_from_api(config, DAY, make_client(FakeApi(rows=10)))
    before = sorted(p.read_bytes() for p in good.iterdir())

    api = FakeApi(rows=1500, versions=[str(v) for v in range(100)])
    with pytest.raises(LoadValidationError):
        load_from_api(config, DAY, make_client(api))

    assert sorted(p.read_bytes() for p in good.iterdir()) == before
    assert not list(good.parent.glob(".*"))  # временные папки убраны


def test_row_count_mismatch_is_invalid(config, make_client):
    api = FakeApi(rows=1500)
    real_get = api.get

    def short_last_page(url, params, timeout):
        response = real_get(url, params, timeout)
        if url.endswith("/rows") and params["$skip"] == 1000:
            return make_response(200, json.loads(response.text)[:-1])
        return response

    api.get = short_last_page
    with pytest.raises(LoadValidationError, match="1499"):
        load_from_api(config, DAY, make_client(api))


# --- допуск при сверке с /count ---------------------------------------------

def test_shortfall_within_tolerance_is_valid(config, make_client):
    api = FakeApi(rows=1500, count_offset=2)  # /count = 1502, отдаётся 1500
    path = load_from_api(replace(config, count_tolerance=2), DAY, make_client(api))

    manifest = json.loads((path / "_manifest.json").read_text())
    assert manifest["count_reported"] == 1502 and manifest["rows"] == 1500
    assert manifest["shortfall"] == 2 and manifest["tolerance"] == 2
    assert len(read_partition(path)[1]) == 1500


def test_shortfall_above_tolerance_fails_and_keeps_old_partition(config, make_client):
    good = load_from_api(config, DAY, make_client(FakeApi(rows=10)))
    before = sorted(p.read_bytes() for p in good.iterdir())

    api = FakeApi(rows=1500, count_offset=3)
    with pytest.raises(LoadValidationError, match="нехватка 3 больше допуска 2"):
        load_from_api(replace(config, count_tolerance=2), DAY, make_client(api))

    assert sorted(p.read_bytes() for p in good.iterdir()) == before
    assert not list(good.parent.glob(".*"))


def test_excess_rows_fail_even_with_large_tolerance(config, make_client):
    api = FakeApi(rows=1500, count_offset=-1)  # строк больше, чем /count
    with pytest.raises(LoadValidationError, match="больше, чем ожидалось"):
        load_from_api(replace(config, count_tolerance=1000), DAY, make_client(api))
    assert not (config.raw_root / "permits" / "load_date=2026-10-20").exists()


@pytest.mark.parametrize("count_offset", [1, -1])
def test_zero_tolerance_is_strict(config, make_client, count_offset):
    assert config.count_tolerance == 0  # значение по умолчанию
    with pytest.raises(LoadValidationError):
        load_from_api(config, DAY, make_client(FakeApi(rows=1500, count_offset=count_offset)))


def test_count_tolerance_from_env(monkeypatch):
    monkeypatch.setenv("MOS_COUNT_TOLERANCE", "10")
    assert LoaderConfig.from_env().count_tolerance == 10
    assert LoaderConfig.from_env(count_tolerance=3).count_tolerance == 3  # флаг CLI перекрывает env
    monkeypatch.delenv("MOS_COUNT_TOLERANCE")
    assert LoaderConfig.from_env().count_tolerance == 0


def test_negative_tolerance_is_rejected():
    with pytest.raises(ValueError):
        LoaderConfig(api_key=TEST_KEY, count_tolerance=-1)


# --- повторный запуск -----------------------------------------------------

def test_rerun_same_date_overwrites_partition(config, make_client):
    load_from_api(config, DAY, make_client(FakeApi(rows=2500)))
    path = load_from_api(config, DAY, make_client(FakeApi(rows=1200)))

    pages, rows = read_partition(path)
    assert [p.name for p in pages] == ["page_000000.jsonl", "page_000001.jsonl"]  # page_000002 от прошлого запуска удалена
    assert len(rows) == 1200
    assert sorted(p.name for p in path.parent.iterdir()) == ["load_date=2026-10-20"]


def test_other_dates_are_not_touched(config, make_client):
    load_from_api(config, date(2026, 10, 19), make_client(FakeApi(rows=5)))
    load_from_api(config, DAY, make_client(FakeApi(rows=7)))
    assert len(read_partition(config.raw_root / "permits" / "load_date=2026-10-19")[1]) == 5


# --- ошибки HTTP -----------------------------------------------------------

def test_400_fails_immediately_without_key_in_message(config, make_client, sleeps):
    api = FakeApi(rows=10)
    api.failures = [make_response(400, {"code": 400, "message": "Bad request"})]
    with pytest.raises(ApiError) as err:
        load_from_api(config, DAY, make_client(api))

    assert err.value.status == 400
    assert TEST_KEY not in str(err.value)
    assert len(api.calls) == 1 and sleeps == []
    assert not (config.raw_root / "permits" / "load_date=2026-10-20").exists()


def test_429_respects_retry_after_seconds(config, make_client, sleeps):
    api = FakeApi(rows=10)
    api.failures = [make_response(429, "Too Many Requests", {"Retry-After": "7"})]
    load_from_api(config, DAY, make_client(api))
    assert sleeps == [7.0]


def test_429_without_retry_after_uses_growing_backoff(config, make_client, sleeps):
    api = FakeApi(rows=10)
    api.failures = [make_response(429, "")] * 3
    load_from_api(config, DAY, make_client(api))
    assert sleeps == [1.0, 2.0, 4.0]


def test_retry_after_is_capped_by_backoff_max(config, make_client, sleeps):
    api = FakeApi(rows=10)
    api.failures = [make_response(429, "", {"Retry-After": "3600"})]
    load_from_api(config, DAY, make_client(api))
    assert sleeps == [30.0]


def test_5xx_and_network_errors_are_retried(config, make_client, sleeps):
    api = FakeApi(rows=10)
    api.failures = [make_response(503, "unavailable"), requests.ConnectionError("reset")]
    load_from_api(config, DAY, make_client(api))
    assert sleeps == [1.0, 2.0]


def test_retries_exhausted_raise_api_error(config, make_client, sleeps):
    api = FakeApi(rows=10)
    api.failures = [make_response(502, "bad gateway")] * 10
    with pytest.raises(ApiError) as err:
        load_from_api(config, DAY, make_client(api))
    assert err.value.status == 502
    assert len(api.calls) == config.max_retries + 1
    assert sleeps == [1.0, 2.0, 4.0]
    assert TEST_KEY not in str(err.value)


def test_parse_retry_after_http_date():
    from datetime import datetime, timezone

    now = datetime(2026, 10, 20, 12, 0, 0, tzinfo=timezone.utc)
    assert parse_retry_after("Tue, 20 Oct 2026 12:00:10 GMT", now) == 10.0
    assert parse_retry_after("garbage", now) is None


def test_missing_key_is_reported(config):
    from loader import MosApiClient

    with pytest.raises(ApiError, match="MOS_API_KEY"):
        MosApiClient(replace(config, api_key=""))


def test_key_not_in_config_repr(config):
    assert TEST_KEY not in repr(config)
