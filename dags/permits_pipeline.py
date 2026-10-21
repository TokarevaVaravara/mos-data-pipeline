"""permits_pipeline: набор data.mos.ru 3247, raw -> clean/rejected -> PostgreSQL -> витрины.

Каждая задача запускает модуль проекта отдельным процессом из venv проекта (/opt/project-venv):
зависимости проекта не смешиваются с зависимостями Airflow, логика проверяется тестами без Airflow.
"""
from __future__ import annotations

import json
import os
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

TZ = pendulum.timezone("Europe/Moscow")
PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", "/opt/project"))
PROJECT_PYTHON = os.environ.get("PROJECT_PYTHON", "/opt/project-venv/bin/python")
API_KEY_FILE = Path(os.environ.get("MOS_API_KEY_FILE", "/run/secrets/mos_api_key"))
COUNT_TOLERANCE = 2
MAX_REJECTED_SHARE = 0.01


def resolve_load_date() -> str:
    """Параметр load_date или дата запуска (run_after) по Москве."""
    ctx = get_current_context()
    value = ctx["params"].get("load_date")
    if value:
        return date.fromisoformat(value).isoformat()
    return pendulum.instance(ctx["dag_run"].run_after).in_timezone(TZ).date().isoformat()


def run_project(*args: str, api_key: bool = False, extra_env: dict | None = None) -> dict | None:
    """python -m <модуль> в venv проекта; вывод - в лог задачи. Последняя строка-JSON возвращается."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("AIRFLOW")}  # Fernet, метабаза - не для проекта
    env["PYTHONPATH"] = str(PROJECT_DIR / "src")
    env.update(extra_env or {})
    if api_key:
        env["MOS_API_KEY"] = API_KEY_FILE.read_text(encoding="utf-8").strip()  # только в окружение процесса
    cmd = [PROJECT_PYTHON, "-m", *args]
    print("запуск:", " ".join(cmd), flush=True)
    proc = subprocess.Popen(cmd, cwd=PROJECT_DIR, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    last = None
    for line in proc.stdout:
        print(line.rstrip(), flush=True)
        if line.startswith("{"):
            last = line
    if proc.wait() != 0:
        raise RuntimeError(f"{args[0]} завершился с кодом {proc.returncode}")
    return json.loads(last) if last else None


@dag(
    dag_id="permits_pipeline",
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 0},
    params={
        "load_date": Param(None, type=["null", "string"], format="date",
                           description="YYYY-MM-DD; по умолчанию - дата запуска по Москве"),
        "skip_load": Param(False, type="boolean",
                           description="не грузить из API, взять уже лежащую партицию raw"),
    },
    tags=["mos", "permits"],
    doc_md=__doc__,
)
def permits_pipeline():
    @task.short_circuit(ignore_downstream_trigger_rules=True)
    def check_version() -> bool:
        ctx = get_current_context()
        skip = bool(ctx["params"]["skip_load"])
        args = ["pipeline.ops", "check-version", "--load-date", resolve_load_date()] + (["--skip-load"] if skip else [])
        result = run_project(*args, api_key=not skip)
        if not result["proceed"]:
            print(f"версия {result['version']} уже загружена успешно: дальше не идём", flush=True)
        return bool(result["proceed"])

    @task(retries=3, retry_delay=timedelta(minutes=5), execution_timeout=timedelta(hours=3))
    def load_raw() -> None:
        if get_current_context()["params"]["skip_load"]:
            raise AirflowSkipException("skip_load=true: используем уже лежащую партицию")
        run_project("loader", "--mode", "api", "--resume", "--load-date", resolve_load_date(),
                    api_key=True, extra_env={"MOS_COUNT_TOLERANCE": str(COUNT_TOLERANCE)})

    @task(trigger_rule="none_failed")  # skip_load: load_raw пропущена, проверяем лежащую партицию
    def validate_raw() -> dict:
        return run_project("pipeline.ops", "validate-raw", "--load-date", resolve_load_date(),
                           "--tolerance", str(COUNT_TOLERANCE))

    @task(execution_timeout=timedelta(minutes=30))
    def clean_spark() -> None:
        run_project("spark_jobs.clean_permits", "--load-date", resolve_load_date())

    @task
    def quality_gate() -> dict:
        return run_project("pipeline.ops", "quality-gate", "--load-date", resolve_load_date(),
                           "--max-rejected-share", str(MAX_REJECTED_SHARE))

    @task(execution_timeout=timedelta(minutes=30))
    def load_postgres() -> None:
        run_project("spark_jobs.load_postgres", "--load-date", resolve_load_date())

    @task
    def refresh_marts() -> None:
        run_project("db.sql_files", "marts")

    @task
    def record_success() -> dict:
        run_id = get_current_context()["run_id"]
        return run_project("pipeline.ops", "record-success", "--load-date", resolve_load_date(), "--dag-run-id", run_id)

    (check_version() >> load_raw() >> validate_raw() >> clean_spark() >> quality_gate()
     >> load_postgres() >> refresh_marts() >> record_success())


permits_pipeline()
