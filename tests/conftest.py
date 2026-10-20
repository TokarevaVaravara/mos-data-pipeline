"""Фейковый apidata.mos.ru: сессия, которая отвечает по сценарию без сети."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import requests

from loader import LoaderConfig, MosApiClient

FIXTURES = Path(__file__).parent / "fixtures"
TEST_KEY = "test-secret-key-123"


def make_response(status: int, body=None, headers: dict | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = (body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)).encode("utf-8")
    response.headers.update(headers or {})
    return response


def make_rows(n: int, start: int = 0) -> list[dict]:
    template = json.loads((FIXTURES / "api_rows.json").read_text(encoding="utf-8"))[0]["Cells"]
    rows = []
    for i in range(start, start + n):
        cells = {**template, "ID": 1000 + i, "global_id": 9000 + i}
        rows.append({"global_id": cells["global_id"], "Number": i - start + 1, "Cells": cells})
    return rows


class FakeApi:
    """Набор из `rows` строк. versions - очередь версий, которые отдаёт карточка
    (последняя повторяется). failures - очередь ответов, которые вернутся
    вместо настоящих перед ближайшими запросами."""

    def __init__(self, rows: int, versions=("1.0",), count_offset: int = 0):
        self.data = make_rows(rows)
        self.versions = list(versions)
        self.count_offset = count_offset  # /count = rows + count_offset
        self.fail_at_skip: int | None = None  # /rows с этим $skip всегда обрывается
        self.failures: list[requests.Response | Exception] = []
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: float) -> requests.Response:
        assert params["api_key"] == TEST_KEY
        self.calls.append((url, dict(params)))
        if self.failures:
            failure = self.failures.pop(0)
            if isinstance(failure, Exception):
                raise failure
            return failure
        if url.endswith("/count"):
            return make_response(200, str(len(self.data) + self.count_offset))
        if url.endswith("/rows"):
            skip, top = int(params["$skip"]), int(params["$top"])
            if skip == self.fail_at_skip:
                raise requests.exceptions.ChunkedEncodingError("Response ended prematurely")
            if top > 1000:
                return make_response(400, {"code": 400, "message": "Параметр $top не может принимать значение больше 1000."})
            return make_response(200, self.data[skip : skip + top])
        version = self.versions.pop(0) if len(self.versions) > 1 else self.versions[0]
        return make_response(200, {"Id": 3247, "VersionNumber": version, "ItemsCount": len(self.data)})

    def rows_calls(self) -> list[dict]:
        return [p for url, p in self.calls if url.endswith("/rows")]


@pytest.fixture
def config(tmp_path) -> LoaderConfig:
    return LoaderConfig(
        api_key=TEST_KEY,
        page_size=1000,
        request_pause=0.01,
        max_retries=3,
        backoff_base=1.0,
        backoff_max=30.0,
        raw_root=tmp_path / "raw",
        local_source_dir=tmp_path / "source",
    )


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def make_client(config, sleeps):
    def factory(api: FakeApi, cfg: LoaderConfig | None = None) -> MosApiClient:
        return MosApiClient(cfg or config, session=api, sleep=sleeps.append)

    return factory
