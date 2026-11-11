import os
from pathlib import Path

from pyspark.sql import functions as F

from spark_runtime import start_spark


def main():
    storage_dir = Path(os.environ["PLS_STORAGE_DIR"]).resolve()
    spark = start_spark("pls-bronze-time-travel", storage_dir)
    try:
        spark.sparkContext.setLogLevel("WARN")
        history = spark.sql("DESCRIBE HISTORY bronze.pls_ae")
        latest_version = history.agg(F.max("version")).first()[0]
        if latest_version is None:
            raise ValueError("Bronze history is empty")

        print("Bronze history (latest 10 commits)", flush=True)
        history.select("version", "timestamp", "operation").orderBy(F.desc("version")).show(10)

        table_path = (storage_dir / "bronze" / "pls_ae").as_uri()
        for label, version in (("first snapshot", 0), ("latest snapshot", latest_version)):
            print(f"Bronze version {version} ({label})", flush=True)
            snapshot = spark.read.format("delta").option("versionAsOf", version).load(table_path)
            (
                snapshot.groupBy("fiscal_year")
                .agg(F.count("*").alias("row_count"))
                .orderBy("fiscal_year")
                .show()
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
