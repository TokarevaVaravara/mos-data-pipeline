"""Фейковый apidata.mos.ru: сессия, которая отвечает по сценарию без сети."""
from __future__ import annotations

import io
import json
import logging
import socket
from pathlib import Path

import pytest
import requests

from loader import LoaderConfig, MosApiClient

FIXTURES = Path(__file__).parent / "fixtures"
TEST_KEY = "test-secret-key-123"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}


@pytest.fixture(autouse=True)
def no_api_key_no_network(monkeypatch):
    """Настоящий ключ тестам не нужен, а Python-код тестов ходит только на localhost
    (Postgres, шлюз py4j). Выход наружу, в т.ч. на apidata.mos.ru, - ошибка теста."""
    monkeypatch.delenv("MOS_API_KEY", raising=False)
    real_connect = socket.socket.connect

    def guarded_connect(sock, address):
        host = address[0] if isinstance(address, tuple) else None
        if sock.family != socket.AF_UNIX and host not in LOCAL_HOSTS:
            raise RuntimeError(f"тест пытается выйти в сеть: {host}")
        return real_connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)


def make_response(status: int, body=None, headers: dict | None = None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = (body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)).encode("utf-8")
    response._content_consumed = True  # тело уже в памяти: iter_content отдаёт его кусками
    response.headers.update(headers or {})
    return response


class FakeClock:
    """Часы для клиента: время идёт, только когда его двигает тест."""

    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class SlowResponse(requests.Response):
    """200 с телом, каждый кусок которого приходит через `step` секунд по FakeClock."""

    def __init__(self, body, clock: FakeClock, step: float, parts: int = 4):
        super().__init__()
        self.status_code = 200
        self.raw = io.BytesIO()  # close() закрывает raw недочитанного ответа
        self.data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.clock, self.step, self.parts = clock, step, parts

    def iter_content(self, chunk_size=1, decode_unicode=False):
        size = -(-len(self.data) // self.parts)
        for i in range(0, len(self.data), size):
            self.clock.now += self.step
            yield self.data[i : i + size]


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
        self.request_kwargs: list[dict] = []  # timeout и stream каждого запроса

    def get(self, url: str, params: dict, timeout, stream: bool = False) -> requests.Response:
        assert params["api_key"] == TEST_KEY
        self.calls.append((url, dict(params)))
        self.request_kwargs.append({"timeout": timeout, "stream": stream})
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


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Одна SparkSession на весь прогон: конфиг (в т.ч. JDBC-драйвер) задаётся только при создании."""
    from db.settings import PgSettings
    from spark_jobs.clean_permits import build_spark
    from spark_jobs.load_postgres import jdbc_conf

    # py4j пишет info при сборке мусора на выходе, когда pytest уже закрыл потоки логов
    logging.getLogger("py4j").setLevel(logging.WARNING)
    session = build_spark(
        master="local[2]", shuffle_partitions=2, local_dir=tmp_path_factory.mktemp("spark-local"),
        app_name="tests", extra_conf=jdbc_conf(PgSettings()),
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sleeps() -> list[float]:
    return []


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def make_client(config, sleeps, clock):
    def factory(api: FakeApi, cfg: LoaderConfig | None = None) -> MosApiClient:
        return MosApiClient(cfg or config, session=api, sleep=sleeps.append, clock=clock)

    return factory
