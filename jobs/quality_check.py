import argparse
import json
import os
from pathlib import Path

from pyspark.sql import functions as F

from silver_clean import METRIC_TYPES
from spark_runtime import start_spark


def check_layers(spark, sources, years):
    bronze = spark.table("bronze.pls_ae").where(F.col("fiscal_year").isin(years))
    silver = spark.table("silver.library_year").where(F.col("fiscal_year").isin(years))
    closed = F.col("STATSTRU").eqNullSafe("23")
    invalid_key = (
        F.col("FSCSKEY").isNull()
        | (F.trim("FSCSKEY") == "")
        | F.col("STABR").isNull()
        | (F.trim("STABR") == "")
    )
    negative_metric = F.exists(F.array(*METRIC_TYPES), lambda value: value < 0)

    bronze_stats = {
        row.fiscal_year: row
        for row in bronze.groupBy("fiscal_year").agg(
            F.count("*").alias("rows"),
            F.count(F.when(closed, 1)).alias("closed"),
            F.countDistinct(F.when(~closed, F.struct("FSCSKEY"))).alias("expected_silver"),
        ).collect()
    }
    silver_stats = {
        row.fiscal_year: row
        for row in silver.groupBy("fiscal_year").agg(
            F.count("*").alias("rows"),
            F.count(F.when(closed, 1)).alias("closed"),
            F.count(F.when(invalid_key, 1)).alias("invalid_keys"),
            F.count(F.when(negative_metric, 1)).alias("negative_rows"),
            F.countDistinct(F.struct("FSCSKEY")).alias("unique_libraries"),
        ).collect()
    }

    errors = []
    for year in years:
        source = sources[str(year)]
        raw = bronze_stats.get(year)
        clean = silver_stats.get(year)
        if raw is None or clean is None:
            errors.append(f"FY{year}: missing Bronze or Silver partition")
            continue

        checks = {
            "Bronze rows": (raw.rows, source["expected_rows"]),
            "Bronze closed libraries": (raw.closed, source["expected_closed"]),
            "Silver rows": (clean.rows, raw.expected_silver),
            "Silver closed libraries": (clean.closed, 0),
            "Silver null or blank keys": (clean.invalid_keys, 0),
            "Silver rows with negative metrics": (clean.negative_rows, 0),
            "Silver duplicate library-year keys": (clean.rows - clean.unique_libraries, 0),
        }
        for name, (actual, expected) in checks.items():
            if actual != expected:
                errors.append(f"FY{year} {name}: expected {expected}, found {actual}")

        probe = source.get("encoding_probe")
        if not probe:
            errors.append(f"FY{year}: missing encoding probe in source configuration")
            continue

        library_key = probe["library_key"]
        column = probe["column"]
        observed = (
            bronze.where(
                (F.col("fiscal_year") == year)
                & (F.col("FSCSKEY") == library_key)
            )
            .select(column)
            .limit(2)
            .collect()
        )
        values = [row[0] for row in observed]
        if values != [probe["expected_value"]]:
            errors.append(
                f"FY{year}: encoding probe failed for {library_key}.{column}; "
                f"expected {probe['expected_value']!r}, found {values!r}"
            )
        else:
            print(f"Quality FY{year}: encoding probe passed for {library_key}.{column}", flush=True)

    if errors:
        raise ValueError("Quality checks failed:\n" + "\n".join(errors))

    for year in years:
        print(
            f"Quality FY{year}: PASS, bronze_rows={bronze_stats[year].rows}, "
            f"silver_rows={silver_stats[year].rows}, closed_removed={bronze_stats[year].closed}",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description="Validate the Bronze and Silver PLS tables.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--fiscal-year", default="all")
    args = parser.parse_args()
    sources = json.loads(args.config.read_text(encoding="utf-8"))
    if args.fiscal_year != "all" and args.fiscal_year not in sources:
        parser.error(f"unknown fiscal year: {args.fiscal_year}")
    years = sorted(map(int, sources)) if args.fiscal_year == "all" else [int(args.fiscal_year)]

    storage_dir = Path(os.environ["PLS_STORAGE_DIR"]).resolve()
    spark = start_spark("pls-quality-check", storage_dir)
    try:
        spark.sparkContext.setLogLevel("WARN")
        check_layers(spark, sources, years)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
