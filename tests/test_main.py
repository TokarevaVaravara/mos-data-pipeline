"""Точка входа: ошибки пишутся в лог одной строкой и без ключа."""
import logging

import pytest
import requests

import loader.__main__ as cli
from conftest import TEST_KEY


@pytest.fixture
def env(monkeypatch, tmp_path):
    monkeypatch.setenv("MOS_API_KEY", TEST_KEY)
    monkeypatch.setenv("MOS_RAW_ROOT", str(tmp_path / "raw"))


def run_main(monkeypatch, capsys, exc):
    def boom(*args, **kwargs):
        logging.getLogger("loader.api_client").warning("запрос https://apidata.mos.ru/v1/datasets/3247?api_key=%s", TEST_KEY)
        raise exc

    monkeypatch.setattr(cli, "load_from_api", boom)
    with pytest.raises(SystemExit) as code:
        cli.main(["--mode", "api"])
    assert code.value.code == 1
    return capsys.readouterr().err


def test_exception_with_key_in_url_is_logged_without_key(env, monkeypatch, capsys):
    url = f"https://apidata.mos.ru/v1/datasets/3247/rows?%24skip=0&api_key={TEST_KEY}"
    err = run_main(monkeypatch, capsys, requests.ConnectionError(f"Max retries exceeded with url: {url}"))

    assert TEST_KEY not in err
    assert "ERROR loader: загрузка не удалась: ConnectionError: Max retries exceeded" in err
    assert "api_key=***" in err
    assert "Traceback" not in err


def test_key_is_removed_even_without_api_key_prefix(env, monkeypatch, capsys):
    err = run_main(monkeypatch, capsys, RuntimeError(f"странная ошибка, ключ {TEST_KEY} прямо в тексте"))
    assert TEST_KEY not in err and "ключ *** прямо" in err


def test_other_log_records_are_redacted_too(env, monkeypatch, capsys):
    err = run_main(monkeypatch, capsys, RuntimeError("boom"))
    assert "WARNING loader.api_client: запрос https://apidata.mos.ru/v1/datasets/3247?api_key=***" in err
    assert TEST_KEY not in err


def test_resume_only_for_api_mode(env):
    with pytest.raises(SystemExit):
        cli.main(["--mode", "local_files", "--resume"])
