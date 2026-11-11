import os
from pathlib import Path

from pyspark.sql import Window, functions as F

from spark_runtime import start_spark


def divide_if_positive(numerator, denominator):
    return F.when(denominator > 0, numerator / denominator)


def build_state_kpis(silver):
    state_year = silver.groupBy("STABR", "fiscal_year").agg(
        F.count("*").alias("libraries"),
        F.sum("POPU_LSA").alias("population_served"),
        F.sum("VISITS").alias("total_visits"),
        F.sum("TOTCIR").alias("total_circulation"),
        F.sum("ELMATCIR").alias("_electronic_circulation"),
        F.sum("TOTOPEXP").alias("_operating_expenditure"),
    )
    population = F.col("population_served")
    circulation = F.col("total_circulation")
    return state_year.select(
        "STABR",
        "fiscal_year",
        "libraries",
        "population_served",
        "total_visits",
        "total_circulation",
        divide_if_positive(F.col("total_visits"), population).alias("visits_per_capita"),
        divide_if_positive(circulation, population).alias("circulation_per_capita"),
        divide_if_positive(F.col("_electronic_circulation"), circulation).alias("e_circ_share"),
        divide_if_positive(F.col("_operating_expenditure"), population).alias("opex_per_capita"),
    )


def add_previous_year(kpis):
    by_state = Window.partitionBy("STABR").orderBy("fiscal_year")
    return kpis.select(
        "*",
        F.lag("fiscal_year").over(by_state).alias("_previous_year"),
        F.lag("total_visits").over(by_state).alias("_previous_visits"),
    )


def validate_gold(gold):
    with_previous = add_previous_year(gold)
    previous_year = F.col("_previous_year")
    previous_visits = F.col("_previous_visits")
    no_baseline = (
        previous_year.isNull()
        | (previous_year != F.col("fiscal_year") - 1)
        | previous_visits.isNull()
        | (previous_visits <= 0)
        | F.col("total_visits").isNull()
    )
    null_yoy = F.col("visits_yoy_pct").isNull()
    invalid_key = (
        F.col("STABR").isNull() | (F.trim("STABR") == "") | F.col("fiscal_year").isNull()
    )
    stats = with_previous.groupBy("fiscal_year").agg(
        F.count("*").alias("rows"),
        F.countDistinct("STABR").alias("states"),
        F.count(F.when(invalid_key, 1)).alias("invalid_keys"),
        F.count(F.when(null_yoy != no_baseline, 1)).alias("unexpected_yoy"),
        F.count(F.when(null_yoy, 1)).alias("null_yoy"),
        F.count(F.when(null_yoy & previous_year.isNotNull(), 1)).alias("missing_yoy_after_first"),
    ).orderBy("fiscal_year").collect()
    if not stats:
        raise ValueError("Gold has no state-year rows")
    for year in stats:
        if year.rows != year.states or year.invalid_keys or year.unexpected_yoy:
            raise ValueError(f"Gold FY{year.fiscal_year} has invalid keys or YoY values: {year}")
    return stats


def write_gold(spark, storage_dir):
    state_kpis = build_state_kpis(spark.table("silver.library_year"))
    with_previous = add_previous_year(state_kpis)
    visits = F.col("total_visits")
    previous_visits = F.col("_previous_visits")
    gold = (
        with_previous.withColumn(
            "visits_yoy_pct",
            F.when(
                F.col("_previous_year") == F.col("fiscal_year") - 1,
                100 * divide_if_positive(visits - previous_visits, previous_visits),
            ),
        )
        .drop("_previous_year", "_previous_visits")
        .persist()
    )
    try:
        stats = validate_gold(gold)
        spark.sql("CREATE DATABASE IF NOT EXISTS gold")
        table_path = (storage_dir / "gold" / "state_year_kpis").as_uri()
        gold.write.format("delta").mode("overwrite").partitionBy("fiscal_year").save(table_path)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS gold.state_year_kpis USING DELTA LOCATION '{table_path}'"
        )
        saved = {
            row.fiscal_year: row["count"]
            for row in spark.table("gold.state_year_kpis").groupBy("fiscal_year").count().collect()
        }
        expected = {year.fiscal_year: year.rows for year in stats}
        if saved != expected:
            raise ValueError(f"Gold partition counts: expected {expected}, found {saved}")
        for year in stats:
            print(
                f"Gold FY{year.fiscal_year}: state_year_rows={year.rows}, "
                f"null_yoy={year.null_yoy}, missing_yoy_after_first={year.missing_yoy_after_first}",
                flush=True,
            )
        print(f"Gold total: state_year_rows={sum(saved.values())}", flush=True)
    finally:
        gold.unpersist()


def main():
    storage_dir = Path(os.environ["PLS_STORAGE_DIR"]).resolve()
    spark = start_spark("pls-gold-state-kpis", storage_dir)
    try:
        spark.sparkContext.setLogLevel("WARN")
        # A revised year changes its own YoY and can change the following year's YoY.
        write_gold(spark, storage_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
