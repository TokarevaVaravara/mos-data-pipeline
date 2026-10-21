-- Число действующих пропусков на каждую дату по зоне.
-- Действует в день d: valid_from <= d <= valid_to и не аннулирован до d (cancel_date пуста или >= d),
-- то есть интервал действия [valid_from, least(valid_to, cancel_date)]; при cancel_date < valid_from он пуст.
-- Вместо перебора "день x пропуск" (~5 млрд сравнений): +1 в первый день, -1 после последнего,
-- накопительная сумма по календарю из generate_series.
DROP MATERIALIZED VIEW IF EXISTS mart.active_permits_daily;
CREATE MATERIALIZED VIEW mart.active_permits_daily AS
WITH intervals AS (
    SELECT zone, valid_from AS first_day, least(valid_to, coalesce(cancel_date, valid_to)) AS last_day
    FROM clean.permits
),
nonempty AS (
    SELECT * FROM intervals WHERE last_day >= first_day
),
deltas AS (
    SELECT zone, day, sum(delta) AS delta
    FROM (
        SELECT zone, first_day AS day, 1 AS delta FROM nonempty
        UNION ALL
        SELECT zone, last_day + 1, -1 FROM nonempty
    ) AS events
    GROUP BY zone, day
),
calendar AS (
    SELECT z.zone, d::date AS day
    FROM (SELECT min(first_day) AS first_day, max(last_day) AS last_day FROM nonempty) AS bounds
    CROSS JOIN generate_series(bounds.first_day, bounds.last_day, interval '1 day') AS d
    CROSS JOIN (SELECT DISTINCT zone FROM clean.permits) AS z
)
SELECT c.day,
       c.zone,
       (sum(coalesce(dl.delta, 0)) OVER (PARTITION BY c.zone ORDER BY c.day))::int AS active_permits
FROM calendar c
LEFT JOIN deltas dl USING (zone, day);

CREATE UNIQUE INDEX ON mart.active_permits_daily (zone, day);
