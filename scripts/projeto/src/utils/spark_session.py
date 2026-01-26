from pyspark.sql import SparkSession
from src.config.settings import settings


def criar_spark_session(app_name: str = "projeto-etl-aws") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)

        # =========================
        # DELTA LAKE
        # =========================
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.1"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # =========================
        # S3A + LocalStack
        # =========================
        .config("spark.hadoop.fs.s3a.endpoint", settings.aws_endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # =========================
        # Performance (case)
        # =========================
        .config("spark.sql.shuffle.partitions", "8")

        .getOrCreate()
    )

    return spark
