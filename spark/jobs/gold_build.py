"""silver -> gold with the write-audit-publish pattern.

    python jobs/gold_build.py build     # rebuild every gold table into gold/_staging/
    python jobs/gold_build.py audit     # data quality checks on staging; exits 1 if any fails
    python jobs/gold_build.py publish   # copy staging over the published gold tables

Gold tables are small aggregates, so each run rebuilds them in full from silver: simple,
idempotent, and self-healing if silver is corrected. Consumers (Athena, the AI agent) only
ever see data that passed the audit. Gold holds no PII.
"""

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import lake_path, spark_session

STAGING = "_staging"
REVENUE_STATUSES = "('paid', 'shipped', 'delivered')"

GOLD_SQL = {
    # 1. How much did we sell, by day, country and category?
    "daily_sales": f"""
        SELECT to_date(o.created_at)                 AS order_date,
               c.country,
               p.category,
               count(DISTINCT o.order_id)            AS orders,
               sum(oi.quantity)                      AS units,
               CAST(sum(oi.quantity * oi.unit_price) AS DECIMAL(14,2)) AS revenue
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.order_id
        JOIN customers c    ON c.customer_id = o.customer_id
        JOIN products p     ON p.product_id = oi.product_id
        WHERE o.status IN {REVENUE_STATUSES}
        GROUP BY 1, 2, 3""",

    # 2a. Which share of payment attempts fail, by provider?
    "payment_provider_daily": """
        SELECT to_date(created_at)                          AS attempt_date,
               provider,
               count(*)                                     AS attempts,
               count_if(status = 'failed')                  AS failed,
               count_if(status IN ('captured', 'refunded')) AS captured,
               CAST(count_if(status = 'failed') / count(*) AS DECIMAL(5,4)) AS failure_rate,
               CAST(coalesce(sum(CASE WHEN status = 'captured' THEN amount END), 0) AS DECIMAL(14,2))
                                                            AS captured_amount
        FROM payments
        GROUP BY 1, 2""",

    # 2b. ...and why?
    "payment_failure_reasons": """
        SELECT to_date(created_at) AS attempt_date,
               provider,
               failure_code,
               count(*)            AS failures
        FROM payments
        WHERE status = 'failed'
        GROUP BY 1, 2, 3""",

    # 3. Which orders are late against the promised delivery date?
    "late_shipments": """
        SELECT s.shipment_id,
               s.order_id,
               s.carrier,
               c.country,
               s.status,
               s.promised_at,
               s.shipped_at,
               s.delivered_at,
               CASE WHEN s.status = 'delivered' THEN 'delivered_late' ELSE 'open_overdue' END AS late_type,
               CAST((unix_timestamp(coalesce(s.delivered_at, current_timestamp()))
                     - unix_timestamp(s.promised_at)) / 3600 AS DECIMAL(10,2))             AS hours_late,
               current_timestamp()                                                         AS snapshot_at
        FROM shipments s
        JOIN orders o    ON o.order_id = s.order_id
        JOIN customers c ON c.customer_id = o.customer_id
        WHERE s.delivered_at > s.promised_at
           OR (s.status IN ('label_created', 'in_transit') AND current_timestamp() > s.promised_at)""",

    # 4. How long does each step of an order take?
    "order_lead_times": """
        WITH t AS (
            SELECT order_id,
                   min(CASE WHEN to_status = 'pending'   THEN changed_at END) AS created_at,
                   min(CASE WHEN to_status = 'paid'      THEN changed_at END) AS paid_at,
                   min(CASE WHEN to_status = 'shipped'   THEN changed_at END) AS shipped_at,
                   min(CASE WHEN to_status = 'delivered' THEN changed_at END) AS delivered_at,
                   min(CASE WHEN to_status = 'cancelled' THEN changed_at END) AS cancelled_at
            FROM order_status_history
            GROUP BY order_id)
        SELECT t.*,
               o.status AS current_status,
               c.country,
               CAST((unix_timestamp(t.paid_at)      - unix_timestamp(t.created_at)) / 3600 AS DECIMAL(10,2)) AS hours_to_pay,
               CAST((unix_timestamp(t.shipped_at)   - unix_timestamp(t.paid_at))    / 3600 AS DECIMAL(10,2)) AS hours_to_ship,
               CAST((unix_timestamp(t.delivered_at) - unix_timestamp(t.shipped_at)) / 3600 AS DECIMAL(10,2)) AS hours_in_transit,
               CAST((unix_timestamp(t.delivered_at) - unix_timestamp(t.created_at)) / 3600 AS DECIMAL(10,2)) AS hours_end_to_end
        FROM t
        JOIN orders o    ON o.order_id = t.order_id
        JOIN customers c ON c.customer_id = o.customer_id""",

    # 5. Who are the most valuable customers? (customer_id only, no PII)
    # Revenue is always the sum of order lines (as in daily_sales), never orders.total_amount,
    # so every gold table agrees even if the source header and lines drift apart.
    "customer_ltv": f"""
        WITH order_revenue AS (
            SELECT o.order_id, o.customer_id, o.created_at, sum(oi.quantity * oi.unit_price) AS revenue
            FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
            WHERE o.status IN {REVENUE_STATUSES}
            GROUP BY 1, 2, 3)
        SELECT c.customer_id,
               c.country,
               c.email_domain,
               count(r.order_id)                                  AS orders,
               CAST(sum(r.revenue) AS DECIMAL(14,2))              AS revenue,
               CAST(avg(r.revenue) AS DECIMAL(12,2))              AS avg_order_value,
               min(r.created_at)                                  AS first_order_at,
               max(r.created_at)                                  AS last_order_at
        FROM customers c
        JOIN order_revenue r ON r.customer_id = c.customer_id
        GROUP BY 1, 2, 3""",
}

KEYS = {
    "daily_sales": ["order_date", "country", "category"],
    "payment_provider_daily": ["attempt_date", "provider"],
    "payment_failure_reasons": ["attempt_date", "provider", "failure_code"],
    "late_shipments": ["shipment_id"],
    "order_lead_times": ["order_id"],
    "customer_ltv": ["customer_id"],
}
MAY_BE_EMPTY = {"late_shipments", "payment_failure_reasons"}


def register_silver(spark: SparkSession) -> None:
    """Expose silver as views: live rows only, products at their current version."""
    for table in ["customers", "orders", "order_items", "payments", "shipments"]:
        (spark.read.format("delta").load(lake_path("silver", table))
         .where("NOT _is_deleted").createOrReplaceTempView(table))
    (spark.read.format("delta").load(lake_path("silver", "products_history"))
     .where("is_current").createOrReplaceTempView("products"))
    spark.read.format("delta").load(lake_path("silver", "order_status_history")).createOrReplaceTempView(
        "order_status_history")


def staging(name: str) -> str:
    return lake_path("gold", f"{STAGING}/{name}")


def build(spark: SparkSession) -> None:
    register_silver(spark)
    for name, sql in GOLD_SQL.items():
        df = spark.sql(sql)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(staging(name))
        print(f"built {name}", flush=True)


# --- Audit -----------------------------------------------------------------------------

def audit(spark: SparkSession) -> bool:
    register_silver(spark)
    t = {name: spark.read.format("delta").load(staging(name)) for name in GOLD_SQL}
    results: list[tuple[str, bool, str]] = []

    def check(name: str, passed: bool, detail: str = "") -> None:
        results.append((name, bool(passed), detail))

    for name, df in t.items():
        keys = KEYS[name]
        rows = df.count()
        check(f"{name}: not empty", rows > 0 or name in MAY_BE_EMPTY, f"{rows} rows")
        null_keys = df.where(" OR ".join(f"{k} IS NULL" for k in keys)).count()
        check(f"{name}: no null keys", null_keys == 0, f"{null_keys} rows with a null key")
        dupes = df.groupBy(*keys).count().where("count > 1").count()
        check(f"{name}: unique on {', '.join(keys)}", dupes == 0, f"{dupes} duplicated keys")

    # Reconciliation: the same revenue, computed three independent ways, must match.
    silver_revenue = spark.sql(f"""
        SELECT coalesce(sum(oi.quantity * oi.unit_price), 0) AS v
        FROM orders o JOIN order_items oi ON oi.order_id = o.order_id
        WHERE o.status IN {REVENUE_STATUSES}""").first().v
    sales_revenue = t["daily_sales"].agg(F.coalesce(F.sum("revenue"), F.lit(0)).alias("v")).first().v
    ltv_revenue = t["customer_ltv"].agg(F.coalesce(F.sum("revenue"), F.lit(0)).alias("v")).first().v
    check("daily_sales revenue = silver order lines", sales_revenue == silver_revenue,
          f"{sales_revenue} vs {silver_revenue}")
    check("customer_ltv revenue = silver order lines", ltv_revenue == silver_revenue,
          f"{ltv_revenue} vs {silver_revenue}")

    drift = spark.sql("""
        SELECT count(*) AS n FROM orders o
        LEFT JOIN (SELECT order_id, sum(quantity * unit_price) AS lines FROM order_items GROUP BY 1) l
          ON l.order_id = o.order_id
        WHERE o.total_amount <> coalesce(l.lines, 0)""").first().n
    print(f"INFO  source orders whose total_amount differs from their lines: {drift}", flush=True)

    # Business rules
    bad_rates = t["payment_provider_daily"].where("failure_rate < 0 OR failure_rate > 1").count()
    check("failure_rate between 0 and 1", bad_rates == 0, f"{bad_rates} rows out of range")
    attempts = t["payment_provider_daily"].agg(F.sum("attempts")).first()[0]
    silver_attempts = spark.table("payments").count()
    check("payment attempts = silver payments", attempts == silver_attempts, f"{attempts} vs {silver_attempts}")
    negative = t["daily_sales"].where("revenue < 0 OR units <= 0").count()
    check("daily_sales revenue >= 0 and units > 0", negative == 0, f"{negative} rows")
    backwards = t["order_lead_times"].where(
        "hours_to_pay < 0 OR hours_to_ship < 0 OR hours_in_transit < 0").count()
    check("order steps happen in order", backwards == 0, f"{backwards} orders with negative durations")
    # Same predicate as the build, not the rounded hours_late (a parcel can be seconds late).
    not_late = t["late_shipments"].where(
        "NOT (delivered_at > promised_at OR (status IN ('label_created', 'in_transit') AND snapshot_at > promised_at))"
    ).count()
    check("late_shipments are actually late", not_late == 0, f"{not_late} rows not late")

    width = max(len(n) for n, _, _ in results)
    for name, passed, detail in results:
        print(f"{'PASS' if passed else 'FAIL'}  {name:<{width}}  {detail}", flush=True)
    failed = [n for n, p, _ in results if not p]
    print(f"{len(results) - len(failed)}/{len(results)} checks passed", flush=True)
    return not failed


def publish(spark: SparkSession) -> None:
    """Each table is replaced atomically (one Delta commit); readers never see a partial table."""
    for name in GOLD_SQL:
        df = spark.read.format("delta").load(staging(name))
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(lake_path("gold", name))
        print(f"published {name}", flush=True)


def main() -> None:
    step = sys.argv[1] if len(sys.argv) > 1 else ""
    if step not in {"build", "audit", "publish"}:
        sys.exit(__doc__)
    spark = spark_session(f"gold_{step}")
    spark.sparkContext.setLogLevel("WARN")
    if step == "build":
        build(spark)
    elif step == "audit" and not audit(spark):
        sys.exit(1)
    elif step == "publish":
        publish(spark)
    spark.stop()


if __name__ == "__main__":
    main()
