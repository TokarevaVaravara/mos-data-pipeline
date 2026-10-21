"""Очистка пропусков на PySpark: raw (JSONL) -> clean / rejected (parquet) + quality_report.

Запуск из корня проекта: PYTHONPATH=src python -m spark_jobs.clean_permits --load-date 2026-10-21
Правила - в cleaning.permits_rules.
"""
from __future__ import annotations

import argparse
import logging
import time
from datetime import date
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from cleaning.permits_rules import (
    ANNULLED,
    DATE_FORMAT_RE,
    DUPLICATE_KEY,
    FLAGS,
    ISSUED,
    STATUSES,
    ZONES,
    build_report,
    global_id_checksum,
    output_dirs,
    raw_partition,
    write_report,
)

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CELLS_SCHEMA = T.StructType([
    T.StructField("global_id", T.LongType()),
    T.StructField("ID", T.IntegerType()),
    T.StructField("Series", T.StringType()),
    T.StructField("Number", T.StringType()),
    T.StructField("ValidFrom", T.StringType()),  # даты разбираем сами: строго и с причиной отказа
    T.StructField("ValidTo", T.StringType()),
    T.StructField("Zone", T.StringType()),
    T.StructField("Status", T.StringType()),
    T.StructField("CancelDate", T.StringType()),
])
# внешние global_id и Number тоже есть в строке, но берём только Cells
RAW_SCHEMA = T.StructType([T.StructField("Cells", CELLS_SCHEMA)])


def build_spark(
    master: str = "local[*]",
    shuffle_partitions: int = 16,  # ~1 млн строк, 16 ядер; 200 по умолчанию - мелкие задачи
    local_dir: Path = PROJECT_ROOT / ".spark-tmp",  # временные файлы Spark - в проекте, не в /tmp
    app_name: str = "clean_permits",
) -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.local.dir", str(local_dir))
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # иначе 1.02.2020 роняет задачу, а не даёт null
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _strict_date(col: str):
    return F.when(F.col(col).rlike(DATE_FORMAT_RE), F.to_date(F.col(col), "dd.MM.yyyy"))


def classify(raw: DataFrame) -> DataFrame:
    """Все строки raw с колонкой reason (null - строка идёт в clean) и флагами."""
    df = (
        raw.select("Cells.*")
        .withColumn("cancel_empty", F.col("CancelDate").isNull() | (F.col("CancelDate") == ""))
        .withColumn("valid_from", _strict_date("ValidFrom"))
        .withColumn("valid_to", _strict_date("ValidTo"))
        .withColumn("cancel_date", F.when(~F.col("cancel_empty"), _strict_date("CancelDate")))
    )
    bad_date = (
        F.col("valid_from").isNull()
        | F.col("valid_to").isNull()
        | (~F.col("cancel_empty") & F.col("cancel_date").isNull())
    )
    df = df.withColumn(
        "reason",
        F.when(bad_date, "unparseable_date")
        .when(F.col("valid_to") < F.col("valid_from"), "invalid_validity_range")
        .when(~F.coalesce(F.col("Zone").isin(*ZONES), F.lit(False)), "unknown_zone")
        .when(~F.coalesce(F.col("Status").isin(*STATUSES), F.lit(False)), "unknown_status"),
    )

    # дубли ищем только среди строк, прошедших остальные проверки
    candidates = df.filter(F.col("reason").isNull())
    by_content = Window.partitionBy(*DUPLICATE_KEY).orderBy("global_id")
    candidates = candidates.withColumn(
        "reason", F.when(F.row_number().over(by_content) > 1, F.lit("duplicate_id"))
    )
    kept = candidates.filter(F.col("reason").isNull())
    # после удаления дублей несколько строк на ID - значит, поля различаются
    kept = kept.withColumn("id_conflict", F.count(F.lit(1)).over(Window.partitionBy("ID")) > 1)
    dropped = candidates.filter(F.col("reason").isNotNull()).withColumn("id_conflict", F.lit(None).cast("boolean"))
    rejected_early = df.filter(F.col("reason").isNotNull()).withColumn("id_conflict", F.lit(None).cast("boolean"))

    return (
        kept.unionByName(dropped).unionByName(rejected_early)
        .withColumn("cancelled_before_start", F.coalesce(F.col("cancel_date") < F.col("valid_from"), F.lit(False)))
        .withColumn(
            "status_cancel_mismatch",
            ((F.col("Status") == ISSUED) & ~F.col("cancel_empty")) | ((F.col("Status") == ANNULLED) & F.col("cancel_empty")),
        )
    )


def clean_columns(df: DataFrame) -> DataFrame:
    return df.select(
        "global_id",
        F.col("ID").alias("id"),
        F.col("Series").alias("series"),
        F.col("Number").alias("number"),
        "valid_from",
        "valid_to",
        F.col("Zone").alias("zone"),
        F.col("Status").alias("status"),
        "cancel_date",
        *FLAGS,
        F.year("valid_from").alias("valid_from_year"),
    )


def rejected_columns(df: DataFrame) -> DataFrame:
    """Исходные значения Cells как есть: у части строк они не разбираются."""
    return df.select(
        "global_id",
        F.col("ID").alias("id"),
        F.col("Series").alias("series"),
        F.col("Number").alias("number"),
        F.col("ValidFrom").alias("valid_from_raw"),
        F.col("ValidTo").alias("valid_to_raw"),
        F.col("Zone").alias("zone"),
        F.col("Status").alias("status"),
        F.col("CancelDate").alias("cancel_date_raw"),
        "reason",
    )


def run(spark: SparkSession, data_root: Path, out_root: Path, load_date: str) -> dict:
    source = raw_partition(data_root, load_date)
    if not source.is_dir():
        raise FileNotFoundError(f"нет партиции {source}")
    dirs = output_dirs(out_root, load_date)

    raw = spark.read.schema(RAW_SCHEMA).option("mode", "FAILFAST").json(str(source))
    classified = classify(raw).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows_raw = classified.count()
        clean = clean_columns(classified.filter(F.col("reason").isNull()))
        clean.repartition("valid_from_year").write.mode("overwrite").partitionBy("valid_from_year").parquet(str(dirs["clean"]))
        rejected = rejected_columns(classified.filter(F.col("reason").isNotNull()))
        rejected.coalesce(1).write.mode("overwrite").parquet(str(dirs["rejected"]))
    finally:
        classified.unpersist()

    # отчёт - по тому, что реально записано
    clean_out = spark.read.parquet(str(dirs["clean"]))
    rejected_out = spark.read.parquet(str(dirs["rejected"]))
    flags = clean_out.agg(*(F.sum(F.col(f).cast("int")).alias(f) for f in FLAGS)).first().asDict()
    by_reason = {r["reason"]: r["count"] for r in rejected_out.groupBy("reason").count().collect()}
    ids = [r[0] for r in clean_out.select("global_id").collect()]
    report = build_report(
        "spark", load_date, source,
        rows_raw=rows_raw, rows_clean=len(ids), rows_rejected=rejected_out.count(),
        rejected_by_reason=by_reason, flags={k: v or 0 for k, v in flags.items()},
        clean_checksum=global_id_checksum(ids),
    )
    write_report(dirs["quality"], report)
    return report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="PySpark: raw -> clean/rejected для пропусков")
    parser.add_argument("--load-date", type=date.fromisoformat, required=True)
    parser.add_argument("--data-root", type=Path, default=Path("data"), help="где лежит raw/")
    parser.add_argument("--out-root", type=Path, default=None, help="куда писать clean/rejected/quality (по умолчанию = data-root)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    started = time.perf_counter()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession запущена за %.2f с", time.perf_counter() - started)
    try:
        report = run(spark, args.data_root, args.out_root or args.data_root, args.load_date.isoformat())
    finally:
        spark.stop()
    log.info(
        "готово за %.2f с: raw %d, clean %d, rejected %d",
        time.perf_counter() - started, report["rows_raw"], report["rows_clean"], report["rows_rejected"],
    )


if __name__ == "__main__":
    main()
