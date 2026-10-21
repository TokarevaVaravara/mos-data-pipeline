-- Длительность действия valid_to - valid_from (дней) по зоне и по всем ('все').
-- Медиана - percentile_cont; перцентили - percentile_disc (реальные значения из данных).
DROP MATERIALIZED VIEW IF EXISTS mart.validity_stats;
CREATE MATERIALIZED VIEW mart.validity_stats AS
WITH durations AS (
    SELECT zone, valid_to - valid_from AS days FROM clean.permits
    UNION ALL
    SELECT 'все', valid_to - valid_from FROM clean.permits
),
stats AS (
    SELECT zone,
           count(*)::int AS permits,
           min(days)     AS min_days,
           percentile_cont(0.5)  WITHIN GROUP (ORDER BY days) AS median_days,
           percentile_disc(0.25) WITHIN GROUP (ORDER BY days) AS p25_days,
           percentile_disc(0.75) WITHIN GROUP (ORDER BY days) AS p75_days,
           percentile_disc(0.9)  WITHIN GROUP (ORDER BY days) AS p90_days,
           percentile_disc(0.99) WITHIN GROUP (ORDER BY days) AS p99_days,
           max(days)     AS max_days,
           count(*) FILTER (WHERE days > 400)::int AS longer_400_days
    FROM durations
    GROUP BY zone
),
frequent AS (
    SELECT zone, days, count(*) AS permits,
           row_number() OVER (PARTITION BY zone ORDER BY count(*) DESC, days) AS rank
    FROM durations
    GROUP BY zone, days
)
SELECT s.*,
       (SELECT jsonb_agg(jsonb_build_object('days', f.days, 'permits', f.permits) ORDER BY f.rank)
        FROM frequent f
        WHERE f.zone = s.zone AND f.rank <= 5) AS top_durations
FROM stats s;

CREATE UNIQUE INDEX ON mart.validity_stats (zone);
