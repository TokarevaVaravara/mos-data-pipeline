-- По дате начала действия (ValidFrom) и зоне: число пропусков и аннулированных (CancelDate заполнена),
-- скользящее среднее числа пропусков за 7 календарных дней и накопительная сумма.
-- Календарь плотный: дни без пропусков есть и дают 0, иначе окно в 7 строк было бы не 7 днями.
DROP MATERIALIZED VIEW IF EXISTS mart.daily_permits;
CREATE MATERIALIZED VIEW mart.daily_permits AS
WITH bounds AS (
    SELECT min(valid_from) AS first_day, max(valid_from) AS last_day FROM clean.permits
),
calendar AS (
    SELECT z.zone, d::date AS day
    FROM bounds
    CROSS JOIN generate_series(bounds.first_day, bounds.last_day, interval '1 day') AS d
    CROSS JOIN (SELECT DISTINCT zone FROM clean.permits) AS z
),
daily AS (
    SELECT valid_from AS day, zone, count(*) AS permits, count(cancel_date) AS cancelled
    FROM clean.permits
    GROUP BY valid_from, zone
)
SELECT c.day,
       c.zone,
       coalesce(d.permits, 0)::int   AS permits,
       coalesce(d.cancelled, 0)::int AS cancelled,
       round(avg(coalesce(d.permits, 0)) OVER last_7_days, 2) AS permits_ma7,  -- в первые 6 дней - по имеющимся
       (sum(coalesce(d.permits, 0)) OVER (PARTITION BY c.zone ORDER BY c.day))::bigint AS permits_cumulative
FROM calendar c
LEFT JOIN daily d USING (day, zone)
WINDOW last_7_days AS (PARTITION BY c.zone ORDER BY c.day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);

CREATE UNIQUE INDEX ON mart.daily_permits (zone, day);
