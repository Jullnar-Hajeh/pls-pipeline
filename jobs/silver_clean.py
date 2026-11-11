import argparse
import json
import os
from pathlib import Path

from pyspark.sql import Window, functions as F

from spark_runtime import start_spark


KEYS = ("FSCSKEY", "fiscal_year")
TEXT_COLUMNS = ("FSCSKEY", "STABR", "LIBNAME", "STATSTRU")
METRIC_TYPES = {
    "POPU_LSA": "long",
    "VISITS": "long",
    "TOTCIR": "long",
    "ELMATCIR": "long",
    "REGBOR": "long",
    "TOTOPEXP": "long",
    "TOTSTAFF": "double",
    "HRS_OPEN": "long",
    "TOTPRO": "long",
    "TOTATTEN": "long",
}
MISSING_REASONS = {
    -1: "missing",
    -3: "code_-3_outside_status_23",
    -4: "not_applicable",
    -9: "suppressed",
}


def count_by_year(rows):
    return {row.fiscal_year: row["count"] for row in rows.groupBy("fiscal_year").count().collect()}


def normalize_metrics(rows):
    reason_lookup = F.create_map(
        *[
            item
            for code, reason in MISSING_REASONS.items()
            for item in (F.lit(float(code)), F.lit(reason))
        ]
    )
    reasons = F.create_map(
        *[
            item
            for metric in METRIC_TYPES
            for item in (F.lit(metric), F.element_at(reason_lookup, F.col(metric).cast("double")))
        ]
    )
    return rows.select(
        *TEXT_COLUMNS,
        "fiscal_year",
        *[
            F.when(~F.col(metric).isin(*MISSING_REASONS), F.col(metric)).alias(metric)
            for metric in METRIC_TYPES
        ],
        F.map_filter(reasons, lambda metric, reason: reason.isNotNull()).alias("missing_reason"),
    )


def clean_years(spark, years, storage_dir):
    bronze = (
        spark.table("bronze.pls_ae")
        .where(F.col("fiscal_year").isin(years))
        .select(
            *TEXT_COLUMNS,
            F.col("fiscal_year").cast("int").alias("fiscal_year"),
            *[F.trim(F.col(name)).cast(kind).alias(name) for name, kind in METRIC_TYPES.items()],
        )
        .persist()
    )
    try:
        closed = F.col("STATSTRU").eqNullSafe("23")
        has_minus_three = F.exists(F.array(*METRIC_TYPES), lambda value: value == -3)
        source_stats = {
            row.fiscal_year: row
            for row in bronze.groupBy("fiscal_year").agg(
                F.count("*").alias("bronze_rows"),
                F.count(F.when(closed, 1)).alias("closed_removed"),
                F.count(F.when(~closed & has_minus_three, 1)).alias("minus3_outside_status23"),
                F.countDistinct(F.when(~closed, F.struct("FSCSKEY"))).alias("expected_silver_rows"),
            ).collect()
        }
        if set(source_stats) != set(years):
            raise ValueError(f"Missing Bronze partitions: {sorted(set(years) - source_stats.keys())}")

        # Use a stable ordering if conflicting records share the same library-year key.
        ordering = [F.col(name).asc_nulls_last() for name in bronze.columns if name not in KEYS]
        library_year = Window.partitionBy(*KEYS).orderBy(*ordering)
        unique = (
            bronze.where(~closed)
            .withColumn("_rank", F.row_number().over(library_year))
            .where(F.col("_rank") == 1)
            .drop("_rank")
        )
        silver = normalize_metrics(unique)

        spark.sql("CREATE DATABASE IF NOT EXISTS silver")
        table_path = (storage_dir / "silver" / "library_year").as_uri()
        predicate = ", ".join(str(year) for year in years)
        (
            silver.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"fiscal_year IN ({predicate})")
            .partitionBy("fiscal_year")
            .save(table_path)
        )
        spark.sql(f"CREATE TABLE IF NOT EXISTS silver.library_year USING DELTA LOCATION '{table_path}'")
        saved = spark.table("silver.library_year").where(F.col("fiscal_year").isin(years))
        saved_counts = count_by_year(saved)
        for year in years:
            stats = source_stats[year]
            saved_count = saved_counts.get(year, 0)
            if saved_count != stats.expected_silver_rows:
                raise ValueError(
                    f"FY{year} Silver count: expected {stats.expected_silver_rows}, found {saved_count}"
                )
            duplicates = stats.bronze_rows - stats.closed_removed - stats.expected_silver_rows
            print(
                f"Silver FY{year}: bronze_rows={stats.bronze_rows}, "
                f"closed_removed={stats.closed_removed}, "
                f"minus3_outside_status23={stats.minus3_outside_status23}, "
                f"duplicates_removed={duplicates}, silver_rows={saved_count}",
                flush=True,
            )
    finally:
        bronze.unpersist()


def main():
    parser = argparse.ArgumentParser(description="Clean Bronze PLS data into Silver.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--fiscal-year", default="all")
    args = parser.parse_args()
    sources = json.loads(args.config.read_text(encoding="utf-8"))
    if args.fiscal_year != "all" and args.fiscal_year not in sources:
        parser.error(f"unknown fiscal year: {args.fiscal_year}")
    years = sorted(map(int, sources)) if args.fiscal_year == "all" else [int(args.fiscal_year)]

    storage_dir = Path(os.environ["PLS_STORAGE_DIR"]).resolve()
    spark = start_spark("pls-silver-clean", storage_dir)
    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.ansi.enabled", "true")
        clean_years(spark, years, storage_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
