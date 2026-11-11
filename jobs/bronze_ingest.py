import argparse
import json
import os
from pathlib import Path

from pyspark.sql import functions as F

from spark_runtime import start_spark


def ingest_year(spark, year, source, data_dir, table_path):
    csv_path = data_dir / source["file"]
    rows = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", source["encoding"])
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(csv_path.as_uri())
        .persist()
    )
    try:
        source_count = rows.count()
        if source_count != source["expected_rows"]:
            raise ValueError(
                f"FY{year} source count: expected {source['expected_rows']}, found {source_count}"
            )

        (
            rows.withColumn("fiscal_year", F.lit(year))
            .withColumn("source_file", F.lit(csv_path.name))
            .withColumn("ingestion_ts", F.current_timestamp())
            .write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("replaceWhere", f"fiscal_year = {year}")
            .partitionBy("fiscal_year")
            .save(table_path)
        )
    finally:
        rows.unpersist()

    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.pls_ae USING DELTA LOCATION '{table_path}'")
    spark.catalog.refreshTable("bronze.pls_ae")
    bronze_count = spark.table("bronze.pls_ae").where(F.col("fiscal_year") == year).count()
    if bronze_count != source_count:
        raise ValueError(f"FY{year} Bronze count: expected {source_count}, found {bronze_count}")
    print(f"Bronze FY{year}: source_rows={source_count}, delta_rows={bronze_count}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Load Public Libraries Survey AE CSVs into Bronze.")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--fiscal-year", default="all")
    args = parser.parse_args()
    sources = json.loads(args.config.read_text(encoding="utf-8"))
    if args.fiscal_year != "all" and args.fiscal_year not in sources:
        parser.error(f"unknown fiscal year: {args.fiscal_year}")
    years = sorted(sources) if args.fiscal_year == "all" else [args.fiscal_year]

    data_dir = Path(os.environ["PLS_DATA_DIR"]).resolve()
    storage_dir = Path(os.environ["PLS_STORAGE_DIR"]).resolve()
    table_path = (storage_dir / "bronze" / "pls_ae").as_uri()
    spark = start_spark("pls-bronze-ingest", storage_dir)
    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        for year in years:
            ingest_year(spark, int(year), sources[year], data_dir, table_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
