"""Lake housekeeping: compact small files (OPTIMIZE) and remove unreferenced ones (VACUUM).

Streaming writes produce many small files (one or more per micro-batch). Compaction keeps reads
fast and S3 request counts low; VACUUM deletes files older than the retention window that no
table version references any more, so time travel still works within that window.

What the 168 h retention has to cover:
* silver lag: OPTIMIZE on bronze marks the small files it compacts as removed, and the silver
  stream still reads those original files for versions it has not processed yet. If silver falls
  more than the retention behind, VACUUM deletes them and silver fails on restart (it would have to
  be rebuilt from bronze with a fresh checkpoint). Keep silver outages well under 7 days.
* Kafka retention is the other bound: bronze must catch up before Kafka drops the events.
* gold audit reads silver by version (time travel); minutes apart, far inside the window.

OPTIMIZE commits can conflict with silver's concurrent MERGEs; silver retries those conflicts,
and a failed OPTIMIZE here is retried by the Airflow task.
"""

import os

from delta.tables import DeltaTable

from common import lake_path, spark_session, table_exists

RETENTION_HOURS = int(os.environ.get("VACUUM_RETENTION_HOURS", "168"))   # 7 days, Delta's default

TABLES = [
    ("bronze", "cdc_events"),
    *[("silver", t) for t in ["customers", "orders", "order_items", "payments", "shipments",
                               "products_history", "order_status_history"]],
    *[("gold", t) for t in ["daily_sales", "payment_provider_daily", "payment_failure_reasons",
                             "late_shipments", "order_lead_times", "customer_ltv"]],
]


def main() -> None:
    spark = spark_session("lake_maintenance")
    spark.sparkContext.setLogLevel("WARN")
    for layer, name in TABLES:
        path = lake_path(layer, name)
        if not table_exists(spark, path):
            print(f"skip {layer}.{name}: not created yet", flush=True)
            continue
        table = DeltaTable.forPath(spark, path)
        metrics = table.optimize().executeCompaction().select("metrics.numFilesRemoved",
                                                              "metrics.numFilesAdded").first()
        table.vacuum(RETENTION_HOURS)
        print(f"{layer}.{name}: compacted {metrics.numFilesRemoved} files into {metrics.numFilesAdded}; "
              f"vacuumed files older than {RETENTION_HOURS} h", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
