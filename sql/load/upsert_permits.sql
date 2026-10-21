-- clean.permits_stage -> clean.permits. Параметр: %(load_date)s.
-- Строка обновляется, только если изменились данные: повторный запуск ничего не меняет.
WITH moved AS (
    -- строка, раньше отклонённая, теперь в clean: в rejected её больше нет
    DELETE FROM clean.permits_rejected r
    USING clean.permits_stage s
    WHERE r.global_id = s.global_id
    RETURNING 1
),
up AS (
    INSERT INTO clean.permits AS p (
        global_id, id, series, number, valid_from, valid_to, zone, status, cancel_date,
        cancelled_before_start, status_cancel_mismatch, id_conflict, source_load_date
    )
    SELECT global_id, id, series, number, valid_from, valid_to, zone, status, cancel_date,
           cancelled_before_start, status_cancel_mismatch, id_conflict, %(load_date)s::date
    FROM clean.permits_stage
    ON CONFLICT (global_id) DO UPDATE SET
        id                     = EXCLUDED.id,
        series                 = EXCLUDED.series,
        number                 = EXCLUDED.number,
        valid_from             = EXCLUDED.valid_from,
        valid_to               = EXCLUDED.valid_to,
        zone                   = EXCLUDED.zone,
        status                 = EXCLUDED.status,
        cancel_date            = EXCLUDED.cancel_date,
        cancelled_before_start = EXCLUDED.cancelled_before_start,
        status_cancel_mismatch = EXCLUDED.status_cancel_mismatch,
        id_conflict            = EXCLUDED.id_conflict,
        source_load_date       = EXCLUDED.source_load_date,
        updated_at             = now()
    WHERE (p.id, p.series, p.number, p.valid_from, p.valid_to, p.zone, p.status, p.cancel_date,
           p.cancelled_before_start, p.status_cancel_mismatch, p.id_conflict)
          IS DISTINCT FROM
          (EXCLUDED.id, EXCLUDED.series, EXCLUDED.number, EXCLUDED.valid_from, EXCLUDED.valid_to,
           EXCLUDED.zone, EXCLUDED.status, EXCLUDED.cancel_date, EXCLUDED.cancelled_before_start,
           EXCLUDED.status_cancel_mismatch, EXCLUDED.id_conflict)
    RETURNING (xmax = 0) AS inserted  -- xmax = 0 у вставленной строки, иначе - обновлённая
)
SELECT count(*) FILTER (WHERE inserted)     AS inserted,
       count(*) FILTER (WHERE NOT inserted) AS updated,
       (SELECT count(*) FROM moved)         AS removed_from_other
FROM up;
