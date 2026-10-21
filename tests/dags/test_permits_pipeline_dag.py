"""DAG permits_pipeline: импорт, структура, ShortCircuit, провал quality_gate.

Нужен Airflow: запускается в контейнере (docker compose exec airflow-scheduler ...), локально пропускается.
"""
import json
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
if not DAGS_DIR.is_dir():  # в контейнере DAG лежат в /opt/airflow/dags
    DAGS_DIR = Path("/opt/airflow/dags")
CHAIN = ["check_version", "load_raw", "validate_raw", "clean_spark", "quality_gate",
         "load_postgres", "refresh_marts", "record_success"]


@pytest.fixture(scope="module")
def dag():
    bag = DagBag(dag_folder=str(DAGS_DIR))  # примеры отключены: AIRFLOW__CORE__LOAD_EXAMPLES=false
    assert bag.import_errors == {}
    return bag.get_dag("permits_pipeline")


def patch_dag_globals(monkeypatch, task, **values):
    """Функции задачи берут run_project и пр. из модуля DAG, загруженного DagBag: подменяем там."""
    for name, value in values.items():
        monkeypatch.setitem(task.python_callable.__globals__, name, value)


def test_dag_imports_and_settings(dag):
    assert dag is not None
    assert dag.catchup is False and dag.max_active_runs == 1 and dag.is_paused_upon_creation is True
    assert dag.timetable.expression == "0 */6 * * *" and str(dag.timetable.timezone) == "Europe/Moscow"
    assert set(dag.params.keys()) == {"load_date", "skip_load"}
    assert dag.params["skip_load"] is False and dag.params["load_date"] is None


def test_linear_chain(dag):
    assert sorted(dag.task_ids) == sorted(CHAIN)
    for upstream, downstream in zip(CHAIN, CHAIN[1:]):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}
    assert dag.get_task("check_version").upstream_task_ids == set()
    assert dag.get_task("record_success").downstream_task_ids == set()


def test_task_settings(dag):
    check = dag.get_task("check_version")
    assert type(check).__name__ == "_ShortCircuitDecoratedOperator" and check.ignore_downstream_trigger_rules
    load = dag.get_task("load_raw")
    assert load.retries == 3 and load.execution_timeout == timedelta(hours=3)
    assert dag.get_task("validate_raw").trigger_rule == "none_failed"  # skip_load не должен пропускать проверки


@pytest.mark.parametrize("proceed", [False, True], ids=["same_version", "new_version"])
def test_check_version_short_circuits_on_same_version(dag, monkeypatch, proceed):
    calls = []

    def fake_run_project(*args, api_key=False, extra_env=None):
        calls.append((args, api_key))
        return {"ok": True, "version": "1.6280", "last_success": {"dataset_version": "1.6280"}, "proceed": proceed}

    task = dag.get_task("check_version")
    patch_dag_globals(monkeypatch, task, run_project=fake_run_project,
                      get_current_context=lambda: {"params": {"skip_load": True, "load_date": "2026-10-21"}})
    assert task.python_callable() is proceed  # False -> ShortCircuit пропускает всё дальше
    # skip_load: версия из манифеста, ключ API процессу не передаётся
    assert calls == [(("pipeline.ops", "check-version", "--load-date", "2026-10-21", "--skip-load"), False)]


def test_quality_gate_fails_on_bad_report(dag, tmp_path, monkeypatch):
    """Настоящий подпроцесс pipeline.ops в venv проекта на отчёте с долей rejected 25 %."""
    project_src = Path("/opt/project/src")
    if not project_src.is_dir():
        pytest.skip("нет /opt/project/src: тест для контейнера Airflow")
    (tmp_path / "src").symlink_to(project_src)
    quality = tmp_path / "data" / "quality" / "permits" / "load_date=2026-10-21"
    quality.mkdir(parents=True)
    (quality / "quality_report.json").write_text(json.dumps(
        {"load_date": "2026-10-21", "rows_raw": 8, "rows_clean": 6, "rows_rejected": 2}), encoding="utf-8")

    task = dag.get_task("quality_gate")
    patch_dag_globals(monkeypatch, task, PROJECT_DIR=tmp_path,
                      get_current_context=lambda: {"params": {"load_date": "2026-10-21", "skip_load": True}})
    with pytest.raises(RuntimeError, match="pipeline.ops завершился с кодом 1"):
        task.python_callable()
