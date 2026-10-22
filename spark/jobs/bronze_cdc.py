"""Kafka -> bronze: append every Debezium change event, untouched, to one Delta table on S3.

Bronze is the replayable audit log of the source database. The payload is stored as the raw
JSON string so a schema change upstream can never break ingestion; parsing happens in silver.
A few fields are lifted out of the payload only to make filtering and partitioning cheap.

Exactly-once into Delta: Spark's checkpoint records the Kafka offsets committed with each
Delta transaction, so a restart neither loses nor duplicates events.
"""

import logging
import os

from pyspark.sql import functions as F

from common import checkpoint_path, lake_path, spark_session

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PATTERN = os.environ.get("TOPIC_PATTERN", r"shopflow\.shop\..*")
TRIGGER = os.environ.get("TRIGGER_INTERVAL", "1 minute")
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "200000")

QUERY_NAME = "bronze_cdc_events"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = spark_session(QUERY_NAME)
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    value = F.col("value").cast("string")
    events = (
        raw.where(F.col("value").isNotNull())
        .select(
            F.element_at(F.split("topic", r"\."), -1).alias("source_table"),
            F.get_json_object(value, "$.op").alias("op"),
            F.get_json_object(value, "$.source.lsn").cast("long").alias("source_lsn"),
            F.get_json_object(value, "$.source.ts_ms").cast("long").alias("source_ts_ms"),
            F.col("key").cast("string").alias("event_key"),
            value.alias("payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
            F.to_date("timestamp").alias("event_date"),
        )
    )

    query = (
        events.writeStream.format("delta")
        .queryName(QUERY_NAME)
        .outputMode("append")
        .partitionBy("event_date", "source_table")
        .option("checkpointLocation", checkpoint_path(QUERY_NAME))
        .trigger(processingTime=TRIGGER)
        .start(lake_path("bronze", "cdc_events"))
    )
    logging.info("streaming %s -> %s every %s", TOPIC_PATTERN, lake_path("bronze", "cdc_events"), TRIGGER)
    query.awaitTermination()


if __name__ == "__main__":
    main()
