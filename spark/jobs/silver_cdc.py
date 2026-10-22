"""bronze -> silver: turn the raw change log into clean, queryable tables.

Each micro-batch of bronze events is parsed per source table and applied with Delta MERGE:

* Current-state tables (customers, orders, order_items, payments, shipments): one row per key.
  A change is applied only if its source LSN is newer than the row's, so replays and
  out-of-order events are harmless. Deletes are soft (`_is_deleted`), which keeps a stale
  event from resurrecting a deleted row.
* products_history: SCD Type 2, one row per version with valid_from / valid_to / is_current,
  so revenue can be computed with the price that was valid at the time.
* order_status_history: one row per status transition, built from before/after images.
* PII: customer email is replaced by a salted SHA-256 hash plus its domain; names are dropped.

Rows that cannot be parsed or have no key go to silver/_rejects instead of failing the stream.
"""

import logging
import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import checkpoint_path, lake_path, spark_session, table_exists

TRIGGER = os.environ.get("TRIGGER_INTERVAL", "2 minutes")
PII_HASH_SALT = os.environ["PII_HASH_SALT"]
QUERY_NAME = "silver_cdc"

SCHEMAS = {
    "customers": "customer_id BIGINT, email STRING, full_name STRING, country STRING, "
                 "created_at STRING, updated_at STRING",
    "products": "product_id BIGINT, sku STRING, name STRING, category STRING, unit_price STRING, "
                "is_active BOOLEAN, created_at STRING, updated_at STRING",
    "orders": "order_id BIGINT, customer_id BIGINT, status STRING, currency STRING, total_amount STRING, "
              "created_at STRING, updated_at STRING",
    "order_items": "order_id BIGINT, line_no INT, product_id BIGINT, quantity INT, unit_price STRING",
    "payments": "payment_id BIGINT, order_id BIGINT, provider STRING, attempt INT, status STRING, "
                "amount STRING, failure_code STRING, created_at STRING, updated_at STRING",
    "shipments": "shipment_id BIGINT, order_id BIGINT, carrier STRING, status STRING, promised_at STRING, "
                 "shipped_at STRING, delivered_at STRING, created_at STRING, updated_at STRING",
}
KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_items": ["order_id", "line_no"],
    "payments": ["payment_id"],
    "shipments": ["shipment_id"],
}
MONEY = "DECIMAL(12,2)"
TIMESTAMPS = {"created_at", "updated_at", "promised_at", "shipped_at", "delivered_at"}
MONEY_COLUMNS = {"unit_price", "total_amount", "amount"}
META = ["_op", "_lsn", "_source_ts", "_is_deleted", "_processed_at"]


# --- Parsing ---------------------------------------------------------------------------

def parse(batch: DataFrame, table: str) -> tuple[DataFrame, DataFrame]:
    """Return (parsed rows with typed columns + before image, rejected rows)."""
    envelope = (f"STRUCT<before: STRUCT<{SCHEMAS[table]}>, after: STRUCT<{SCHEMAS[table]}>, "
                f"op: STRING, source: STRUCT<lsn: BIGINT, ts_ms: BIGINT>>")
    events = (
        batch.where(F.col("source_table") == table)
        .select("payload", "kafka_offset", F.from_json("payload", envelope).alias("e"))
    )
    # Deletes carry only `before`; everything else carries `after`.
    row = F.coalesce(F.col("e.after"), F.col("e.before"))
    key_present = F.lit(True)
    for k in KEYS[table]:
        key_present = key_present & row[k].isNotNull()

    good = events.where(F.col("e").isNotNull() & key_present)
    rejected = (events.where(F.col("e").isNull() | ~key_present)
                .select(F.lit(table).alias("source_table"), "payload", "kafka_offset",
                        F.current_timestamp().alias("_rejected_at")))

    columns = []
    for field in good.select(row.alias("r")).schema["r"].dataType.fields:
        c = F.col(f"r.{field.name}")
        if field.name in TIMESTAMPS:
            c = c.cast("timestamp")
        elif field.name in MONEY_COLUMNS:
            c = c.cast(MONEY)
        columns.append(c.alias(field.name))

    parsed = good.select(
        row.alias("r"),
        F.col("e.before").alias("_before"),
        F.col("e.op").alias("_op"),
        F.col("e.source.lsn").alias("_lsn"),
        (F.col("e.source.ts_ms") / 1000).cast("timestamp").alias("_source_ts"),
        "kafka_offset",
    ).select(
        *columns,
        "_before", "_op", "_lsn", "_source_ts",
        (F.col("_op") == "d").alias("_is_deleted"),
        F.current_timestamp().alias("_processed_at"),
        "kafka_offset",
    )
    return parsed, rejected


def latest_per_key(df: DataFrame, keys: list[str]) -> DataFrame:
    w = Window.partitionBy(*keys).orderBy(F.col("_lsn").desc(), F.col("kafka_offset").desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def mask_customers(df: DataFrame) -> DataFrame:
    email = F.lower(F.trim("email"))
    return (df.withColumn("email_hash", F.sha2(F.concat(F.lit(PII_HASH_SALT), email), 256))
              .withColumn("email_domain", F.substring_index(email, "@", -1))
              .drop("email", "full_name"))


# --- Writers ---------------------------------------------------------------------------


def upsert_current(spark: SparkSession, df: DataFrame, table: str) -> None:
    """Apply the latest change per key; only newer LSNs win."""
    keys = KEYS[table]
    rows = latest_per_key(df, keys).drop("_before", "kafka_offset")
    path = lake_path("silver", table)
    if not table_exists(spark, path):
        rows.write.format("delta").save(path)
        return
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(rows.alias("s"), on)
     .whenMatchedUpdateAll(condition="s._lsn > t._lsn")
     .whenNotMatchedInsertAll()
     .execute())


def append_status_history(spark: SparkSession, orders: DataFrame) -> None:
    """One row per order status transition (creation counts as a transition from NULL)."""
    transitions = orders.where(
        F.col("_op").isin("c", "r")
        | ((F.col("_op") == "u") & ~F.col("_before.status").eqNullSafe(F.col("status")))
    ).select(
        "order_id",
        F.when(F.col("_op") == "u", F.col("_before.status")).alias("from_status"),
        F.col("status").alias("to_status"),
        F.col("updated_at").alias("changed_at"),
        "_lsn",
        "_processed_at",
    )
    path = lake_path("silver", "order_status_history")
    if not table_exists(spark, path):
        transitions.write.format("delta").save(path)
        return
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(transitions.alias("s"), "t.order_id = s.order_id AND t._lsn = s._lsn")
     .whenNotMatchedInsertAll()
     .execute())


def apply_products_scd2(spark: SparkSession, products: DataFrame) -> None:
    """SCD Type 2: close the current version and insert the new ones, in a single MERGE."""
    path = lake_path("silver", "products_history")
    versions = products.drop("_before", "kafka_offset").withColumn("valid_from", F.col("updated_at"))

    if table_exists(spark, path):
        current = (spark.read.format("delta").load(path).where("is_current")
                   .select("product_id", F.col("_lsn").alias("_current_lsn")))
        # Skip anything already applied: makes replays of the same micro-batch a no-op.
        versions = (versions.join(current, "product_id", "left")
                    .where(F.col("_current_lsn").isNull() | (F.col("_lsn") > F.col("_current_lsn")))
                    .drop("_current_lsn"))

    w = Window.partitionBy("product_id").orderBy("_lsn")
    versions = (versions.withColumn("valid_to", F.lead("valid_from").over(w))
                .withColumn("is_current", F.col("valid_to").isNull()))

    if not table_exists(spark, path):
        versions.write.format("delta").save(path)
        return

    first_new = versions.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    staged = (versions.withColumn("_merge_key", F.lit(None).cast("bigint"))
              .unionByName(first_new.withColumn("_merge_key", F.col("product_id"))))
    insert_values = {c: f"s.{c}" for c in versions.columns}
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(staged.alias("s"), "t.product_id = s._merge_key AND t.is_current")
     .whenMatchedUpdate(set={"is_current": "false", "valid_to": "s.valid_from"})
     .whenNotMatchedInsert(condition="s._merge_key IS NULL", values=insert_values)
     .execute())


def write_rejects(rejected: DataFrame, table: str, batch_id: int) -> None:
    """Append-only, made idempotent per micro-batch with Delta's txnAppId/txnVersion."""
    if not rejected.isEmpty():
        (rejected.write.format("delta").mode("append")
         .option("txnAppId", f"{QUERY_NAME}_rejects_{table}").option("txnVersion", batch_id)
         .save(lake_path("silver", "_rejects")))
        print(f"rejected {rejected.count()} events from {table}", flush=True)


# --- Stream ----------------------------------------------------------------------------

def process_batch(batch: DataFrame, batch_id: int) -> None:
    spark = batch.sparkSession
    batch = batch.persist()
    counts = {r["source_table"]: r["count"] for r in batch.groupBy("source_table").count().collect()}
    for table in SCHEMAS:
        if not counts.get(table):
            continue
        parsed, rejected = parse(batch, table)
        parsed = parsed.persist()
        write_rejects(rejected, table, batch_id)
        if table == "products":
            apply_products_scd2(spark, parsed)
        else:
            if table == "orders":
                append_status_history(spark, parsed)
            upsert_current(spark, mask_customers(parsed) if table == "customers" else parsed, table)
        parsed.unpersist()
    batch.unpersist()
    # print, not logging: this runs on the Py4J callback thread, where log records were not showing up.
    print(f"batch {batch_id} applied: {counts}", flush=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = spark_session(QUERY_NAME)
    spark.sparkContext.setLogLevel("WARN")

    bronze = (spark.readStream.format("delta")
              .option("maxFilesPerTrigger", 1000)
              .load(lake_path("bronze", "cdc_events")))
    query = (bronze.writeStream.queryName(QUERY_NAME)
             .foreachBatch(process_batch)
             .option("checkpointLocation", checkpoint_path(QUERY_NAME))
             .trigger(processingTime=TRIGGER)
             .start())
    logging.info("streaming bronze -> silver every %s", TRIGGER)
    query.awaitTermination()


if __name__ == "__main__":
    main()
