"""Lake housekeeping: compact small files (OPTIMIZE) and remove unreferenced ones (VACUUM).

Streaming writes produce many small files (one or more per micro-batch). Compaction keeps reads
fast and S3 request counts low; VACUUM deletes files older than the retention window that no
table version references any more, so time travel still works within that window.
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
