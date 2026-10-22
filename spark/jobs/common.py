"""Shared Spark session setup for the lakehouse jobs."""

import os

from pyspark.sql import SparkSession

LAKE_BUCKET = os.environ["LAKE_BUCKET"]
LAKE = f"s3a://{LAKE_BUCKET}"


def lake_path(layer: str, table: str) -> str:
    return f"{LAKE}/{layer}/{table}"


def checkpoint_path(query_name: str) -> str:
    return f"{LAKE}/_checkpoints/{query_name}"


def spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A with credentials from the environment (pipeline IAM user, lake prefixes only)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )
