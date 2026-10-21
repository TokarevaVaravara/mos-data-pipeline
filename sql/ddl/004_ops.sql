-- Журнал успешных прогонов пайплайна: одна строка на load_date (повторный прогон её обновляет).
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.load_log (
    load_date        date PRIMARY KEY,
    dataset_version  text NOT NULL,
    count_reported   integer NOT NULL,  -- /count на момент загрузки (из _manifest.json)
    rows_raw         integer NOT NULL,
    rows_clean       integer NOT NULL,
    rows_rejected    integer NOT NULL,
    dag_run_id       text,
    first_success_at timestamptz NOT NULL DEFAULT now(),
    last_success_at  timestamptz NOT NULL DEFAULT now()
);
