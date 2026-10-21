"""Тестам не нужен настоящий MOS_API_KEY и выход в сеть: см. no_api_key_no_network в conftest."""
import os

import pytest
import requests


def test_api_key_is_removed_from_environment():
    assert "MOS_API_KEY" not in os.environ


def test_external_network_is_blocked():
    with pytest.raises(Exception, match="тест пытается выйти в сеть"):
        requests.get("https://apidata.mos.ru/v1/datasets/3247", timeout=5)
