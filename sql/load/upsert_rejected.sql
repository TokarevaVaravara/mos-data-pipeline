-- clean.permits_rejected_stage -> clean.permits_rejected. Параметр: %(load_date)s.
WITH moved AS (
    -- строка, раньше бывшая в clean, теперь отклонена: из clean её убираем
    DELETE FROM clean.permits p
    USING clean.permits_rejected_stage s
    WHERE p.global_id = s.global_id
    RETURNING 1
),
up AS (
    INSERT INTO clean.permits_rejected AS r (
        global_id, id, series, number, valid_from_raw, valid_to_raw, zone, status, cancel_date_raw,
        reason, source_load_date
    )
    SELECT global_id, id, series, number, valid_from_raw, valid_to_raw, zone, status, cancel_date_raw,
           reason, %(load_date)s::date
    FROM clean.permits_rejected_stage
    ON CONFLICT (global_id) DO UPDATE SET
        id               = EXCLUDED.id,
        series           = EXCLUDED.series,
        number           = EXCLUDED.number,
        valid_from_raw   = EXCLUDED.valid_from_raw,
        valid_to_raw     = EXCLUDED.valid_to_raw,
        zone             = EXCLUDED.zone,
        status           = EXCLUDED.status,
        cancel_date_raw  = EXCLUDED.cancel_date_raw,
        reason           = EXCLUDED.reason,
        source_load_date = EXCLUDED.source_load_date,
        updated_at       = now()
    WHERE (r.id, r.series, r.number, r.valid_from_raw, r.valid_to_raw, r.zone, r.status,
           r.cancel_date_raw, r.reason)
          IS DISTINCT FROM
          (EXCLUDED.id, EXCLUDED.series, EXCLUDED.number, EXCLUDED.valid_from_raw, EXCLUDED.valid_to_raw,
           EXCLUDED.zone, EXCLUDED.status, EXCLUDED.cancel_date_raw, EXCLUDED.reason)
    RETURNING (xmax = 0) AS inserted
)
SELECT count(*) FILTER (WHERE inserted)     AS inserted,
       count(*) FILTER (WHERE NOT inserted) AS updated,
       (SELECT count(*) FROM moved)         AS removed_from_other
FROM up;
