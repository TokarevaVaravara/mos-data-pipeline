"""Подключение к PostgreSQL. Пароль - только из окружения или .env, в коде его нет."""
from __future__ import annotations

import os
from dataclasses import dataclass, field, replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = PROJECT_ROOT / "sql"

# JDBC-драйвер для Spark: версия фиксирована, переопределяется POSTGRES_JDBC_VERSION
POSTGRES_JDBC_VERSION = "42.7.14"

_KEYS = ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_JDBC_VERSION")


def _read_env_file(path: Path) -> dict[str, str]:
    """Только переменные POSTGRES_* из .env; остальное (в т.ч. MOS_API_KEY) не читаем."""
    values = {}
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.strip().partition("=")
            if sep and key in _KEYS and value:
                values[key] = value
    return values


@dataclass(frozen=True)
class PgSettings:
    host: str = "localhost"
    port: int = 5432
    dbname: str = ""
    user: str = ""
    password: str = field(default="", repr=False)
    jdbc_version: str = POSTGRES_JDBC_VERSION

    @classmethod
    def from_env(cls, env_file: Path | None = PROJECT_ROOT / ".env") -> "PgSettings":
        """Окружение важнее .env: так можно переопределить, не меняя файл."""
        env = {**(_read_env_file(env_file) if env_file else {}), **{k: v for k, v in os.environ.items() if k in _KEYS and v}}
        settings = cls(
            host=env.get("POSTGRES_HOST", cls.host),
            port=int(env.get("POSTGRES_PORT", cls.port)),
            dbname=env.get("POSTGRES_DB", ""),
            user=env.get("POSTGRES_USER", ""),
            password=env.get("POSTGRES_PASSWORD", ""),
            jdbc_version=env.get("POSTGRES_JDBC_VERSION", POSTGRES_JDBC_VERSION),
        )
        missing = [name for name, value in (("POSTGRES_DB", settings.dbname), ("POSTGRES_USER", settings.user),
                                            ("POSTGRES_PASSWORD", settings.password)) if not value]
        if missing:
            raise RuntimeError(f"не заданы {', '.join(missing)} (окружение или .env)")
        return settings

    def with_db(self, dbname: str) -> "PgSettings":
        return replace(self, dbname=dbname)

    @property
    def jdbc_package(self) -> str:
        return f"org.postgresql:postgresql:{self.jdbc_version}"

    @property
    def jdbc_url(self) -> str:
        # reWriteBatchedInserts: драйвер склеивает пакет INSERT-ов в многострочные - быстрее
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}?reWriteBatchedInserts=true"

    def jdbc_properties(self) -> dict[str, str]:
        return {"user": self.user, "password": self.password, "driver": "org.postgresql.Driver"}

    def connect(self, **kwargs):
        import psycopg

        return psycopg.connect(
            host=self.host, port=self.port, dbname=self.dbname, user=self.user, password=self.password, **kwargs
        )
