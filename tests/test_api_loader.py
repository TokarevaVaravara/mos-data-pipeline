import http.client
import json
from dataclasses import replace
from datetime import date

import pytest
import requests
import urllib3

from conftest import TEST_KEY, FakeApi, SlowResponse, make_response
from loader import ApiError, LoaderConfig, LoadValidationError, load_from_api
from loader.api_client import parse_retry_after

DAY = date(2026, 10, 20)


def read_partition(path):
    pages = sorted(path.glob("page_*.jsonl"))
    rows = [json.loads(line) for p in pages for line in p.read_text(encoding="utf-8").splitlines()]
    return pages, rows


def assert_only_hidden_staging_left(partition):
    """После падения рядом с партицией остаётся только скрытая временная папка (для --resume)."""
    names = sorted(p.name for p in partition.parent.iterdir())
    assert names == [f".{partition.name}.staging", partition.name]


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
    assert_only_hidden_staging_left(good)


def test_row_count_mismatch_is_invalid(config, make_client):
    api = FakeApi(rows=1500)
    real_get = api.get

    def short_last_page(url, params, timeout, stream=False):
        response = real_get(url, params, timeout, stream)
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
    assert_only_hidden_staging_left(good)


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


@pytest.mark.parametrize(
    "broken",
    [
        requests.exceptions.ChunkedEncodingError("Response ended prematurely"),
        urllib3.exceptions.ProtocolError("Response ended prematurely"),
        http.client.IncompleteRead(b"[{", 500),
        make_response(200, '[{"global_id": 1, "Cells": {"ID"'),  # тело оборвано, JSON не разбирается
    ],
    ids=["chunked", "protocol", "incomplete_read", "truncated_json"],
)
def test_broken_response_is_retried(config, make_client, sleeps, broken):
    api = FakeApi(rows=1500)
    api.failures = [broken]
    path = load_from_api(config, DAY, make_client(api))
    assert sleeps == [1.0, 0.01]  # повтор после обрыва + обычная пауза между страницами
    assert read_partition(path)[1] == api.data


def test_broken_responses_in_a_row_fail_after_retry_limit(config, make_client, sleeps):
    api = FakeApi(rows=10)
    api.failures = [requests.exceptions.ChunkedEncodingError(f"url?api_key={TEST_KEY}")] * 10
    with pytest.raises(ApiError) as err:
        load_from_api(config, DAY, make_client(api))
    assert len(api.calls) == config.max_retries + 1
    assert sleeps == [1.0, 2.0, 4.0]
    assert "ChunkedEncodingError" in str(err.value) and TEST_KEY not in str(err.value)


# --- таймауты --------------------------------------------------------------

def test_session_get_receives_connect_read_pair(config, make_client):
    api = FakeApi(rows=1500)
    cfg = replace(config, connect_timeout=7.0, read_timeout=33.0)
    load_from_api(cfg, DAY, make_client(api, cfg))
    assert api.request_kwargs  # карточка, /count и /rows
    assert all(kw == {"timeout": (7.0, 33.0), "stream": True} for kw in api.request_kwargs)


def test_timeout_defaults_and_env(monkeypatch):
    cfg = LoaderConfig(api_key=TEST_KEY)
    assert (cfg.connect_timeout, cfg.read_timeout, cfg.page_deadline) == (10.0, 60.0, 180.0)
    monkeypatch.setenv("MOS_CONNECT_TIMEOUT", "5")
    monkeypatch.setenv("MOS_READ_TIMEOUT", "30")
    monkeypatch.setenv("MOS_PAGE_DEADLINE", "90")
    cfg = LoaderConfig.from_env()
    assert (cfg.connect_timeout, cfg.read_timeout, cfg.page_deadline) == (5.0, 30.0, 90.0)


@pytest.mark.parametrize("field", ["connect_timeout", "read_timeout", "page_deadline"])
def test_non_positive_timeout_is_rejected(field):
    with pytest.raises(ValueError):
        LoaderConfig(api_key=TEST_KEY, **{field: 0})


def slow_rows(api, clock, step, times=None):
    """Первые `times` запросов /rows (None - все) отвечают телом, растянутым на 4 * step секунд."""
    real_get = api.get

    def get(url, params, timeout, stream=False):
        slow = times is None or len(api.rows_calls()) < times
        if url.endswith("/rows") and slow:
            api.calls.append((url, dict(params)))
            return SlowResponse(api.data[params["$skip"] : params["$skip"] + params["$top"]], clock, step)
        return real_get(url, params, timeout, stream)

    api.get = get


def test_slow_page_within_deadline_is_accepted(config, make_client, sleeps, clock):
    api = FakeApi(rows=10)
    slow_rows(api, clock, step=40.0, times=1)  # 4 куска по 40 с = 160 с < 180
    path = load_from_api(config, DAY, make_client(api))
    assert sleeps == [] and read_partition(path)[1] == api.data


def test_slow_page_over_deadline_is_retried_then_succeeds(config, make_client, sleeps, clock, caplog):
    api = FakeApi(rows=10)
    slow_rows(api, clock, step=100.0, times=1)  # второй кусок уже за 180 с
    path = load_from_api(config, DAY, make_client(api))

    assert read_partition(path)[1] == api.data
    assert len(api.rows_calls()) == 2 and sleeps == [1.0]
    assert "PageDeadlineExceeded" in caplog.text and TEST_KEY not in caplog.text


def test_slow_pages_fail_after_retry_limit(config, make_client, sleeps, clock):
    api = FakeApi(rows=10)
    slow_rows(api, clock, step=100.0)
    with pytest.raises(ApiError) as err:
        load_from_api(config, DAY, make_client(api))

    assert len(api.rows_calls()) == config.max_retries + 1
    assert sleeps == [1.0, 2.0, 4.0]
    assert "PageDeadlineExceeded" in str(err.value) and TEST_KEY not in str(err.value)
    assert err.value.status is None  # статус 200 недочитанного ответа не выдаём за итог
    assert not (config.raw_root / "permits" / "load_date=2026-10-20").exists()


def test_new_defaults_for_pause_and_backoff():
    cfg = LoaderConfig(api_key=TEST_KEY)
    assert (cfg.request_pause, cfg.backoff_base, cfg.backoff_max) == (0.5, 5.0, 60.0)


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
