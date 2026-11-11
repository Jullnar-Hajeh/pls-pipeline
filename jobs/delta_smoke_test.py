from pathlib import Path
import os

from pyspark.sql import SparkSession


def main():
    output = str(Path(os.environ["PLS_STORAGE_DIR"]) / "smoke-test")
    spark = (
        SparkSession.builder.appName("pls-delta-smoke-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    try:
        spark.sparkContext.setLogLevel("WARN")
        spark.range(3).write.format("delta").mode("overwrite").save(output)
        actual = sorted(row.id for row in spark.read.format("delta").load(output).collect())
        if actual != [0, 1, 2]:
            raise ValueError(f"Unexpected Delta smoke test result: {actual}")
        print(f"Delta round trip succeeded: {actual}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
