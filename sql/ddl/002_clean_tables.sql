-- Пропуска после очистки (data/clean/permits). Ограничения повторяют гарантии очистки.
CREATE TABLE IF NOT EXISTS clean.permits (
    global_id              bigint PRIMARY KEY,
    id                     integer,
    series                 text,
    number                 text,
    valid_from             date    NOT NULL,  -- дата начала действия (не дата выдачи)
    valid_to               date    NOT NULL,
    zone                   text    NOT NULL CHECK (zone IN ('СК', 'ТТК', 'МКАД')),
    status                 text    NOT NULL CHECK (status IN ('Выдан', 'Аннулирован')),
    cancel_date            date,
    cancelled_before_start boolean NOT NULL,
    status_cancel_mismatch boolean NOT NULL,
    id_conflict            boolean NOT NULL,
    source_load_date       date    NOT NULL,  -- партиция, из которой строка записана или последний раз изменена
    updated_at             timestamptz NOT NULL DEFAULT now(),
    CHECK (valid_to >= valid_from)
);

-- Отклонённые строки (data/rejected/permits): исходные значения и причина.
CREATE TABLE IF NOT EXISTS clean.permits_rejected (
    global_id        bigint PRIMARY KEY,
    id               integer,
    series           text,
    number           text,
    valid_from_raw   text,
    valid_to_raw     text,
    zone             text,
    status           text,
    cancel_date_raw  text,
    reason           text NOT NULL CHECK (reason IN (
        'unparseable_date', 'invalid_validity_range', 'unknown_zone', 'unknown_status', 'duplicate_id'
    )),
    source_load_date date NOT NULL,
    updated_at       timestamptz NOT NULL DEFAULT now()
);

-- Промежуточные таблицы для Spark JDBC: перезаписываются каждым запуском, WAL не нужен.
CREATE UNLOGGED TABLE IF NOT EXISTS clean.permits_stage (
    global_id              bigint,
    id                     integer,
    series                 text,
    number                 text,
    valid_from             date,
    valid_to               date,
    zone                   text,
    status                 text,
    cancel_date            date,
    cancelled_before_start boolean,
    status_cancel_mismatch boolean,
    id_conflict            boolean
);

CREATE UNLOGGED TABLE IF NOT EXISTS clean.permits_rejected_stage (
    global_id       bigint,
    id              integer,
    series          text,
    number          text,
    valid_from_raw  text,
    valid_to_raw    text,
    zone            text,
    status          text,
    cancel_date_raw text,
    reason          text
);
