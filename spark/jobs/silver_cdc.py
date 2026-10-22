"""bronze -> silver: turn the raw change log into clean, queryable tables.

Each micro-batch of bronze events is parsed per source table and applied with Delta MERGE:

* Current-state tables (customers, orders, order_items, payments, shipments): one row per key.
  A change is applied only if its source LSN is newer than the row's, so replays and
  out-of-order events are harmless. Deletes are soft (`_is_deleted`), which keeps a stale
  event from resurrecting a deleted row.
* products_history: SCD Type 2, one row per version with valid_from / valid_to / is_current,
  so revenue can be computed with the price that was valid at the time. Versions are keyed by
  (product_id, _lsn) and valid_to / is_current are recomputed over every stored version of the
  touched products, so the result does not depend on the order in which events arrive.
* order_status_history: one row per status transition, built from before/after images.
  Orders captured by the initial snapshot (op 'r') get a single transition (NULL -> their status
  at snapshot time): their earlier history happened before CDC started and cannot be recovered.
* PII: customer email is replaced by a salted SHA-256 hash plus its domain; names are dropped.

Event time is the source commit time (`_source_ts`, Debezium's source.ts_ms), not the row's
updated_at: it follows the same order as the LSN and also exists for deletes.

Kafka Connect is at-least-once, so the same change can reach bronze twice: every writer dedups
on (key, _lsn) and every write is idempotent, which also makes retries and replays safe.

Rows that cannot be parsed, have no key or hold a value that does not cast (a timestamp or amount
that is not null at the source but null after try_cast) go to silver/_rejects with a reason,
instead of failing the stream.
"""

import logging
import os
import time

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
REJECT_KEY = ["kafka_topic", "kafka_partition", "kafka_offset"]

MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 5
# Delta optimistic-concurrency failures (e.g. a MERGE racing the daily OPTIMIZE). Matched by name
# because PySpark surfaces them as Py4JJavaError / AnalysisException wrapping the Java exception.
CONFLICT_MARKERS = ("ConcurrentModification", "ConcurrentAppend", "ConcurrentDeleteRead",
                    "ConcurrentDeleteDelete", "ConcurrentTransaction", "MetadataChanged",
                    "ProtocolChanged", "DELTA_CONCURRENT")


# --- Parsing ---------------------------------------------------------------------------

def parse(batch: DataFrame, table: str) -> tuple[DataFrame, DataFrame]:
    """Return (parsed rows with typed columns + before image, rejected rows with a reason)."""
    envelope = (f"STRUCT<before: STRUCT<{SCHEMAS[table]}>, after: STRUCT<{SCHEMAS[table]}>, "
                f"op: STRING, source: STRUCT<lsn: BIGINT, ts_ms: BIGINT>>")
    events = (
        batch.where(F.col("source_table") == table)
        .select("payload", *REJECT_KEY, F.from_json("payload", envelope).alias("e"))
    )
    # Deletes carry only `before`; everything else carries `after`.
    row = F.coalesce(F.col("e.after"), F.col("e.before"))
    key_present = F.lit(True)
    for k in KEYS[table]:
        key_present = key_present & row[k].isNotNull()

    # try_cast: a malformed value becomes a reject instead of an ANSI cast error that kills the stream.
    typed, bad_values = {}, []
    for field in events.select(row.alias("r")).schema["r"].dataType.fields:
        source = row[field.name]
        if field.name in TIMESTAMPS:
            c = source.try_cast("timestamp")
        elif field.name in MONEY_COLUMNS:
            c = source.try_cast(MONEY)
        else:
            c = source
        if c is not source:
            bad_values.append(F.when(source.isNotNull() & c.isNull(), F.lit(field.name)))
        typed[field.name] = c
    bad = F.concat_ws(", ", *bad_values) if bad_values else F.lit("")

    reason = (F.when(row.isNull(), F.lit("unparseable payload"))
              .when(~key_present, F.lit("missing key"))
              .when(bad != "", F.concat(F.lit("invalid value in "), bad)))
    events = events.withColumn("_reason", reason)

    good = events.where(F.col("_reason").isNull())
    rejected = (events.where(F.col("_reason").isNotNull())
                .select(F.lit(table).alias("source_table"), *REJECT_KEY, "payload",
                        F.col("_reason").alias("reason"), F.current_timestamp().alias("_rejected_at")))

    parsed = good.select(
        *[c.alias(name) for name, c in typed.items()],
        F.col("e.before").alias("_before"),
        F.col("e.op").alias("_op"),
        F.col("e.source.lsn").alias("_lsn"),
        (F.col("e.source.ts_ms") / 1000).cast("timestamp").alias("_source_ts"),
        (F.col("e.op") == "d").alias("_is_deleted"),
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
    """One row per order status transition (creation counts as a transition from NULL).

    Snapshot rows (op 'r') only know the status at snapshot time, so pre-CDC orders get a
    single transition. Insert-only on (order_id, _lsn): replays and duplicates add nothing.
    """
    transitions = orders.dropDuplicates(KEYS["orders"] + ["_lsn"]).where(
        F.col("_op").isin("c", "r")
        | ((F.col("_op") == "u") & ~F.col("_before.status").eqNullSafe(F.col("status")))
    ).select(
        "order_id",
        F.when(F.col("_op") == "u", F.col("_before.status")).alias("from_status"),
        F.col("status").alias("to_status"),
        F.col("_source_ts").alias("changed_at"),
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
    """SCD Type 2 keyed by (product_id, _lsn), independent of arrival order.

    New versions are combined with every stored version of the products they touch, valid_to
    (next version's valid_from by LSN) and is_current are recomputed over the full sequence, and
    a single MERGE inserts the new versions and fixes the stored ones whose window changed. One
    commit, so readers never see a product with zero or two current versions. A replay finds
    every version already stored with the same window and changes nothing.
    """
    path = lake_path("silver", "products_history")
    versions = (products.drop("_before", "kafka_offset")
                .dropDuplicates(KEYS["products"] + ["_lsn"])
                .withColumn("valid_from", F.col("_source_ts")))
    w = Window.partitionBy("product_id").orderBy("_lsn")

    if not table_exists(spark, path):
        (versions.withColumn("valid_to", F.lead("valid_from").over(w))
         .withColumn("is_current", F.col("valid_to").isNull())
         .write.format("delta").save(path))
        return

    touched = versions.select("product_id").distinct()
    stored = (spark.read.format("delta").load(path).join(F.broadcast(touched), "product_id")
              .withColumnRenamed("valid_to", "_old_valid_to")
              .withColumnRenamed("is_current", "_old_is_current"))
    new = versions.join(stored.select("product_id", "_lsn"), ["product_id", "_lsn"], "left_anti")
    changes = (stored.withColumn("_stored", F.lit(True))
               .unionByName(new.withColumn("_stored", F.lit(False)), allowMissingColumns=True)
               .withColumn("valid_to", F.lead("valid_from").over(w))
               .withColumn("is_current", F.col("valid_to").isNull())
               .where(~F.col("_stored")
                      | ~F.col("valid_to").eqNullSafe(F.col("_old_valid_to"))
                      | ~F.col("is_current").eqNullSafe(F.col("_old_is_current"))))

    insert_values = {c: f"s.{c}" for c in versions.columns + ["valid_to", "is_current"]}
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(changes.alias("s"), "t.product_id = s.product_id AND t._lsn = s._lsn")
     .whenMatchedUpdate(set={"valid_to": "s.valid_to", "is_current": "s.is_current"})
     .whenNotMatchedInsert(values=insert_values)
     .execute())


def write_rejects(spark: SparkSession, rejected: DataFrame, table: str) -> None:
    """Insert-only on the Kafka coordinates, so it stays idempotent even if the checkpoint is reset."""
    rejected = rejected.dropDuplicates(REJECT_KEY)
    n = rejected.count()
    if not n:
        return
    path = lake_path("silver", "_rejects")
    if not table_exists(spark, path):
        rejected.write.format("delta").save(path)
    else:
        on = " AND ".join(f"t.{k} = s.{k}" for k in REJECT_KEY)
        # Schema evolution adds kafka_topic / kafka_partition / reason to a table created before them.
        (DeltaTable.forPath(spark, path).alias("t")
         .merge(rejected.alias("s"), on)
         .withSchemaEvolution()
         .whenNotMatchedInsertAll()
         .execute())
    print(f"rejected {n} events from {table}", flush=True)


def with_retries(action, what: str) -> None:
    """Retry on Delta write conflicts only; safe because every writer is idempotent."""
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            action()
            return
        except Exception as exc:
            text = f"{type(exc).__name__}: {exc}"
            if attempt == MAX_ATTEMPTS or not any(m in text for m in CONFLICT_MARKERS):
                raise
            delay = RETRY_BACKOFF_SECONDS * attempt
            print(f"concurrent write conflict on {what} (attempt {attempt}/{MAX_ATTEMPTS}), "
                  f"retrying in {delay}s", flush=True)
            time.sleep(delay)


def apply_table(spark: SparkSession, parsed: DataFrame, rejected: DataFrame, table: str) -> None:
    write_rejects(spark, rejected, table)
    if table == "products":
        apply_products_scd2(spark, parsed)
    else:
        if table == "orders":
            append_status_history(spark, parsed)
        upsert_current(spark, mask_customers(parsed) if table == "customers" else parsed, table)


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
        with_retries(lambda: apply_table(spark, parsed, rejected, table), table)
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
