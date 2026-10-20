"""Клиент apidata.mos.ru: ретраи с нарастающей паузой и учётом Retry-After.

Ключ передаётся через params и никогда не попадает в тексты исключений:
у requests.HTTPError в сообщении полный URL, поэтому его мы не пробрасываем.
"""
from __future__ import annotations

import http.client
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Callable, TypeVar

import requests
import urllib3

from .config import LoaderConfig

log = logging.getLogger(__name__)

T = TypeVar("T")

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
# сетевые сбои и ответы, оборванные посреди передачи
RETRYABLE_EXCEPTIONS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    urllib3.exceptions.ProtocolError,
    http.client.IncompleteRead,
)


class ApiError(RuntimeError):
    """Ошибка API без URL и ключа в тексте."""

    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class DatasetMeta:
    version: str
    count: int


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Retry-After бывает числом секунд или HTTP-датой."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    now = now or datetime.now(timezone.utc)
    return max(0.0, (when - now).total_seconds())


def _json(response: requests.Response):
    return response.json()


class MosApiClient:
    def __init__(
        self,
        config: LoaderConfig,
        session: requests.Session | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        if not config.api_key:
            raise ApiError("MOS_API_KEY не задан в окружении")
        self.config = config
        self.session = session or requests.Session()
        self.sleep = sleep

    def _dataset_url(self, suffix: str = "") -> str:
        return f"{self.config.base_url}/datasets/{self.config.dataset_id}{suffix}"

    def _backoff(self, attempt: int, response: requests.Response | None) -> float:
        retry_after = parse_retry_after(response.headers.get("Retry-After")) if response is not None else None
        if retry_after is not None:
            return min(retry_after, self.config.backoff_max)
        return min(self.config.backoff_base * 2**attempt, self.config.backoff_max)

    def _get(self, suffix: str, params: dict | None = None, parse: Callable[[requests.Response], T] = _json) -> T:
        """GET с повторами. parse выполняется внутри цикла: тело, оборванное
        так, что не разбирается, - такой же повод для повтора, как обрыв сети."""
        what = f"GET /datasets/{self.config.dataset_id}{suffix} {params or ''}"
        query = {**(params or {}), "api_key": self.config.api_key}
        for attempt in range(self.config.max_retries + 1):
            response = None
            try:
                response = self.session.get(self._dataset_url(suffix), params=query, timeout=self.config.timeout)
            except RETRYABLE_EXCEPTIONS as exc:
                problem = type(exc).__name__  # str(exc) может содержать URL с ключом
            else:
                if response.status_code == 200:
                    try:
                        return parse(response)
                    except ValueError as exc:  # JSONDecodeError, int("") и т.п.
                        problem = f"тело ответа не разобралось ({type(exc).__name__})"
                elif response.status_code not in RETRYABLE_STATUSES:
                    raise ApiError(f"{what}: HTTP {response.status_code}: {response.text[:300]}", response.status_code)
                else:
                    problem = f"HTTP {response.status_code}"
            if attempt == self.config.max_retries:
                status = response.status_code if response is not None else None
                raise ApiError(f"{what}: {problem}, попыток: {attempt + 1}", status)
            pause = self._backoff(attempt, response)
            log.warning("%s: %s, повтор через %.1f с", what, problem, pause)
            self.sleep(pause)
        raise AssertionError("unreachable")

    def get_meta(self) -> DatasetMeta:
        card = self._get("")
        count = self._get("/count", parse=lambda r: int(r.text))
        return DatasetMeta(version=str(card["VersionNumber"]), count=count)

    def get_rows(self, skip: int, top: int) -> list[dict]:
        rows = self._get("/rows", {"$skip": skip, "$top": top})
        if not isinstance(rows, list):
            raise ApiError(f"/rows вернул {type(rows).__name__}, ожидался список")
        return rows
