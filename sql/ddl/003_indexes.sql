-- Запросы за период по дате начала действия (агрегаты daily_permits / cancel_rate за месяц, неделю):
-- index only scan вместо чтения всей таблицы. Полная пересборка витрин читает всю таблицу - ей индекс не нужен.
CREATE INDEX IF NOT EXISTS permits_valid_from_zone_idx
    ON clean.permits (valid_from, zone) INCLUDE (cancel_date);
