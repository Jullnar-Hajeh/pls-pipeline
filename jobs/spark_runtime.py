from pyspark.sql import SparkSession


def start_spark(app_name, storage_dir):
    metastore = storage_dir / "metastore_db"
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", (storage_dir / "warehouse").as_uri())
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore};create=true",
        )
        .enableHiveSupport()
        .getOrCreate()
    )
