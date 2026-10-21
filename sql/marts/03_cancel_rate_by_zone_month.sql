-- Доля аннулированных (CancelDate заполнена) среди пропусков, начавших действовать в месяце, по зоне.
DROP MATERIALIZED VIEW IF EXISTS mart.cancel_rate_by_zone_month;
CREATE MATERIALIZED VIEW mart.cancel_rate_by_zone_month AS
SELECT date_trunc('month', valid_from)::date AS month,
       zone,
       count(*)::int           AS permits,
       count(cancel_date)::int AS cancelled,
       round(count(cancel_date)::numeric / count(*), 4) AS cancel_rate
FROM clean.permits
GROUP BY 1, 2;

CREATE UNIQUE INDEX ON mart.cancel_rate_by_zone_month (zone, month);
