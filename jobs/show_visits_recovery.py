import os
from pathlib import Path

from pyspark.sql import functions as F

from spark_runtime import start_spark


NON_STATE_CODES = ("AS", "DC", "GU", "MP", "PR", "VI")


def main():
    storage_dir = Path(os.environ["PLS_STORAGE_DIR"]).resolve()
    spark = start_spark("pls-visits-recovery", storage_dir)
    try:
        spark.sparkContext.setLogLevel("WARN")
        visits = (
            spark.table("gold.state_year_kpis")
            .where(F.col("fiscal_year").isin(2020, 2022) & ~F.col("STABR").isin(*NON_STATE_CODES))
            .groupBy("STABR")
            .agg(
                F.max(F.when(F.col("fiscal_year") == 2020, F.col("total_visits"))).alias("visits_2020"),
                F.max(F.when(F.col("fiscal_year") == 2022, F.col("total_visits"))).alias("visits_2022"),
            )
            .where((F.col("visits_2020") > 0) & F.col("visits_2022").isNotNull())
            .withColumn(
                "change_pct",
                100 * (F.col("visits_2022") - F.col("visits_2020")) / F.col("visits_2020"),
            )
        )

        for title, order in (
            ("Top 5 states", F.col("change_pct").desc()),
            ("Bottom 5 states", F.col("change_pct").asc()),
        ):
            print(title, flush=True)
            (
                visits.orderBy(order, "STABR")
                .limit(5)
                .withColumn("change_pct", F.round("change_pct", 2))
                .show(truncate=False)
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
