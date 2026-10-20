# mos-data-pipeline

Учебный пайплайн данных на открытых данных [data.mos.ru](https://data.mos.ru):
Airflow + PySpark + PostgreSQL, слои `raw -> clean -> marts`.

Набор данных: **3247** — пропуска для грузового автотранспорта в зоны ограничения
движения (МКАД, ТТК, СК), ~980 тыс. записей, обновляется ежедневно.

## Структура

```
dags/          DAG-и Airflow
src/loader/    загрузчик: API apidata.mos.ru и локальные файлы data/raw/
src/spark_jobs/ PySpark-задачи raw -> clean
sql/           DDL и витрины PostgreSQL
tests/         юнит-тесты
docs/          заметки и схемы
data/          локальные данные (не в git)
```

## Быстрый старт

```bash
cp .env.example .env
```

Запуск через `docker compose up` — будет добавлен на следующих шагах.

## Статус

- [x] Шаг 0: каркас репозитория
- [ ] Шаг 1: загрузчик (API / локальные файлы)
- [ ] Docker Compose: PostgreSQL, Airflow, Spark
- [ ] Слои clean и marts
