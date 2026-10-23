"""Business-question tools over the gold layer (design: docs/agent-tools.md).

Every tool validates its arguments against closed lists and ranges *before* any query runs,
passes values to Athena as ExecutionParameters, and only splices in SQL fragments chosen from
fixed maps (group-by dimensions) or already-validated integers (LIMIT).
"""

from datetime import date, datetime, timedelta, timezone

from athena import AthenaRunner

COUNTRIES = ("AR", "MX", "CL", "CO", "UY", "PE")
CATEGORIES = ("home", "electronics", "sports", "books", "beauty")
PROVIDERS = ("mercadopago", "stripe", "paypal")
CARRIERS = ("Andreani", "OCA", "Correo Argentino", "Estafeta", "Chilexpress", "Servientrega")
MAX_RANGE_DAYS = 92
ALL = "ALL"

TIME_NOTE = ("Durations are real hours under a compressed simulation clock "
             "(1 simulated hour = 1 real minute): multiply by 60 to read them as simulated hours.")
USD_NOTE = "Amounts are in USD (the source currency default; no conversion is applied)."


class ValidationError(ValueError):
    pass


# --- Validation ------------------------------------------------------------------------

def _today() -> date:
    return datetime.now(timezone.utc).date()


def _date(value: str | None, default: date, name: str) -> date:
    if value is None:
        return default
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValidationError(f"{name} must be an ISO date (YYYY-MM-DD), got {value!r}") from None


def _date_range(date_from: str | None, date_to: str | None, default_days_back: int) -> tuple[str, str]:
    end = _date(date_to, _today(), "date_to")
    start = _date(date_from, end - timedelta(days=default_days_back), "date_from")
    if start > end:
        raise ValidationError("date_from must be on or before date_to")
    if (end - start).days > MAX_RANGE_DAYS:
        raise ValidationError(f"the date range can span at most {MAX_RANGE_DAYS} days")
    return start.isoformat(), end.isoformat()


def _choice(value: str | None, allowed: tuple[str, ...], name: str) -> str:
    if value is None:
        return ALL
    if value not in allowed:
        raise ValidationError(f"{name} must be one of {', '.join(allowed)}; got {value!r}")
    return value


def _int(value: int, low: int, high: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise ValidationError(f"{name} must be an integer between {low} and {high}")
    return value


def _result(raw: dict, table: str, notes: list[str]) -> dict:
    if not raw["rows"]:
        notes = notes + ["No rows for these filters. Do not report zero: suggest a wider range or fewer "
                         "filters, and check data_freshness for the latest available data."]
    return {"columns": raw["columns"], "rows": raw["rows"],
            "meta": {"table": f"shopflow_gold.{table}", "rows_returned": len(raw["rows"]),
                     "scanned_bytes": raw["scanned_bytes"], "notes": notes}}


def _grouped(select: str, dims: list[str], where: str, order: str, limit: int = 50) -> str:
    dims_sql = "".join(f"{d}, " for d in dims)
    group = f"GROUP BY {', '.join(dims)} ORDER BY {order}" if dims else ""
    return f"SELECT {dims_sql}{select} {where} {group} LIMIT {limit}"


# --- Tools -----------------------------------------------------------------------------

SALES_GROUPS = {
    "none": ([], ""),
    "day": (["order_date"], "order_date"),
    "country": (["country"], "revenue_usd DESC"),
    "category": (["category"], "revenue_usd DESC"),
    "country_category": (["country", "category"], "revenue_usd DESC"),
    "day_country": (["order_date", "country"], "order_date, revenue_usd DESC"),
}


def sales_summary(db: AthenaRunner, date_from=None, date_to=None, country=None, category=None,
                  group_by="country") -> dict:
    start, end = _date_range(date_from, date_to, default_days_back=0)
    country, category = _choice(country, COUNTRIES, "country"), _choice(category, CATEGORIES, "category")
    if group_by not in SALES_GROUPS:
        raise ValidationError(f"group_by must be one of {', '.join(SALES_GROUPS)}")
    dims, order = SALES_GROUPS[group_by]
    by_category = "category" in dims
    orders_col = "orders_with_category" if by_category else "orders"
    sql = _grouped(
        f"""sum(orders) AS {orders_col}, sum(units) AS units,
            CAST(sum(revenue) AS DECIMAL(14,2)) AS revenue_usd,
            CAST(sum(revenue) / nullif(sum(orders), 0) AS DECIMAL(12,2)) AS avg_order_value_usd""",
        dims,
        """FROM daily_sales
           WHERE order_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
             AND (? = 'ALL' OR country = ?) AND (? = 'ALL' OR category = ?)""",
        order)
    notes = [USD_NOTE, "Counts orders currently paid, shipped or delivered, by order creation date (UTC)."]
    if by_category:
        notes.append("An order with several categories is counted in each: order counts are not additive "
                     "across categories (revenue is).")
    return _result(db.run(sql, [start, end, country, country, category, category]), "daily_sales", notes)


FAILURE_GROUPS = {"provider": (["provider"], "failure_rate DESC"),
                  "day_provider": (["attempt_date", "provider"], "attempt_date, provider")}


def payment_failure_rate(db: AthenaRunner, date_from=None, date_to=None, provider=None,
                         group_by="provider") -> dict:
    start, end = _date_range(date_from, date_to, default_days_back=6)
    provider = _choice(provider, PROVIDERS, "provider")
    if group_by not in FAILURE_GROUPS:
        raise ValidationError(f"group_by must be one of {', '.join(FAILURE_GROUPS)}")
    dims, order = FAILURE_GROUPS[group_by]
    sql = _grouped(
        """sum(attempts) AS attempts, sum(failed) AS failed, sum(captured) AS captured,
           CAST(sum(failed) * 1.0 / nullif(sum(attempts), 0) AS DECIMAL(5,4)) AS failure_rate,
           CAST(sum(captured_amount) AS DECIMAL(14,2)) AS captured_amount_usd""",
        dims,
        """FROM payment_provider_daily
           WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
             AND (? = 'ALL' OR provider = ?)""",
        order)
    notes = [USD_NOTE, "failure_rate is a fraction of payment attempts (0.0842 = 8.42%); a retried payment "
             "counts as several attempts."]
    return _result(db.run(sql, [start, end, provider, provider]), "payment_provider_daily", notes)


def payment_failure_reasons(db: AthenaRunner, date_from=None, date_to=None, provider=None, top_n=10) -> dict:
    start, end = _date_range(date_from, date_to, default_days_back=6)
    provider, top_n = _choice(provider, PROVIDERS, "provider"), _int(top_n, 1, 50, "top_n")
    sql = f"""
        WITH reasons AS (
            SELECT provider, failure_code, sum(failures) AS failures
            FROM payment_failure_reasons
            WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE) AND (? = 'ALL' OR provider = ?)
            GROUP BY provider, failure_code),
        attempts AS (
            SELECT provider, sum(attempts) AS attempts
            FROM payment_provider_daily
            WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE) AND (? = 'ALL' OR provider = ?)
            GROUP BY provider)
        SELECT r.provider, coalesce(r.failure_code, 'unknown') AS failure_code, r.failures,
               CAST(r.failures * 1.0 / sum(r.failures) OVER (PARTITION BY r.provider) AS DECIMAL(5,4))
                   AS share_of_provider_failures,
               CAST(r.failures * 1.0 / nullif(a.attempts, 0) AS DECIMAL(5,4)) AS share_of_provider_attempts
        FROM reasons r JOIN attempts a ON a.provider = r.provider
        ORDER BY r.failures DESC
        LIMIT {top_n}"""
    params = [start, end, provider, provider] * 2
    return _result(db.run(sql, params), "payment_failure_reasons", ["Shares are fractions (0.25 = 25%)."])


LATE_TYPES = ("open_overdue", "delivered_late", "all")


def late_shipments(db: AthenaRunner, late_type="open_overdue", carrier=None, country=None, limit=20) -> dict:
    if late_type not in LATE_TYPES:
        raise ValidationError(f"late_type must be one of {', '.join(LATE_TYPES)}")
    carrier, country = _choice(carrier, CARRIERS, "carrier"), _choice(country, COUNTRIES, "country")
    limit = _int(limit, 1, 50, "limit")
    sql = f"""
        SELECT shipment_id, order_id, carrier, country, status, late_type, promised_at, shipped_at,
               delivered_at, hours_late, snapshot_at, count(*) OVER () AS total_matching
        FROM late_shipments
        WHERE (? = 'all' OR late_type = ?) AND (? = 'ALL' OR carrier = ?) AND (? = 'ALL' OR country = ?)
        ORDER BY hours_late DESC
        LIMIT {limit}"""
    notes = ["hours_late is measured at snapshot_at (the last gold build), not now. " + TIME_NOTE,
             "Only late shipments are stored, so a '% late' cannot be computed from this tool. "
             "Use total_matching to say 'showing N of M'."]
    return _result(db.run(sql, [late_type, late_type, carrier, carrier, country, country]),
                   "late_shipments", notes)


def late_shipments_by_carrier(db: AthenaRunner, late_type="open_overdue", country=None) -> dict:
    if late_type not in LATE_TYPES:
        raise ValidationError(f"late_type must be one of {', '.join(LATE_TYPES)}")
    country = _choice(country, COUNTRIES, "country")
    sql = """
        SELECT carrier, country, count(*) AS late_shipments,
               CAST(avg(hours_late) AS DECIMAL(10,2)) AS avg_hours_late,
               max(hours_late) AS max_hours_late, max(snapshot_at) AS snapshot_at
        FROM late_shipments
        WHERE (? = 'all' OR late_type = ?) AND (? = 'ALL' OR country = ?)
        GROUP BY carrier, country
        ORDER BY late_shipments DESC
        LIMIT 50"""
    notes = ["Ranks carriers by number of late parcels, not by late rate (on-time parcels are not stored). "
             + TIME_NOTE]
    return _result(db.run(sql, [late_type, late_type, country, country]), "late_shipments", notes)


LEAD_GROUPS = {"none": ([], ""), "country": (["country"], "country")}


def order_lead_times(db: AthenaRunner, date_from=None, date_to=None, country=None, group_by="none") -> dict:
    start, end = _date_range(date_from, date_to, default_days_back=6)
    country = _choice(country, COUNTRIES, "country")
    if group_by not in LEAD_GROUPS:
        raise ValidationError(f"group_by must be one of {', '.join(LEAD_GROUPS)}")
    dims, order = LEAD_GROUPS[group_by]
    sql = _grouped(
        """count(*) AS orders_created, count(hours_end_to_end) AS orders_delivered,
           CAST(avg(hours_to_pay) AS DECIMAL(10,2)) AS avg_hours_to_pay,
           CAST(avg(hours_to_ship) AS DECIMAL(10,2)) AS avg_hours_to_ship,
           CAST(avg(hours_in_transit) AS DECIMAL(10,2)) AS avg_hours_in_transit,
           CAST(avg(hours_end_to_end) AS DECIMAL(10,2)) AS avg_hours_end_to_end,
           CAST(approx_percentile(hours_end_to_end, 0.5) AS DECIMAL(10,2)) AS p50_hours_end_to_end,
           CAST(approx_percentile(hours_end_to_end, 0.9) AS DECIMAL(10,2)) AS p90_hours_end_to_end""",
        dims,
        """FROM order_lead_times
           WHERE CAST(created_at AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
             AND (? = 'ALL' OR country = ?)""",
        order)
    notes = [TIME_NOTE, "Only orders that reached each stage are measured; recent undelivered orders are "
             "excluded, which biases short, recent ranges toward faster times."]
    return _result(db.run(sql, [start, end, country, country]), "order_lead_times", notes)


def top_customers_ltv(db: AthenaRunner, top_n=10, country=None, min_orders=1) -> dict:
    top_n, min_orders = _int(top_n, 1, 50, "top_n"), _int(min_orders, 1, 1000, "min_orders")
    country = _choice(country, COUNTRIES, "country")
    # Explicit column list is the PII guardrail: email_domain exists in gold but is never returned.
    sql = f"""
        SELECT customer_id, country, orders, revenue AS lifetime_revenue_usd,
               avg_order_value AS avg_order_value_usd, first_order_at, last_order_at,
               rank() OVER (ORDER BY revenue DESC) AS revenue_rank
        FROM customer_ltv
        WHERE (? = 'ALL' OR country = ?) AND orders >= ?
        ORDER BY revenue DESC, customer_id
        LIMIT {top_n}"""
    notes = [USD_NOTE, "Customers are identified by customer_id only; names and emails are not available to "
             "the agent by design. Revenue is all-time."]
    return _result(db.run(sql, [country, country, min_orders]), "customer_ltv", notes)


def data_freshness(db: AthenaRunner) -> dict:
    sql = """
        SELECT 'daily_sales' AS table_name, CAST(max(order_date) AS TIMESTAMP) AS latest_data_at,
               CAST(NULL AS TIMESTAMP) AS built_at FROM daily_sales
        UNION ALL SELECT 'payment_provider_daily', CAST(max(attempt_date) AS TIMESTAMP), CAST(NULL AS TIMESTAMP)
            FROM payment_provider_daily
        UNION ALL SELECT 'late_shipments', max(coalesce(delivered_at, shipped_at, promised_at)), max(snapshot_at)
            FROM late_shipments
        UNION ALL SELECT 'order_lead_times', max(coalesce(delivered_at, shipped_at, paid_at, created_at)),
            CAST(NULL AS TIMESTAMP) FROM order_lead_times
        UNION ALL SELECT 'customer_ltv', max(last_order_at), CAST(NULL AS TIMESTAMP) FROM customer_ltv"""
    notes = ["Gold is rebuilt on a schedule, so it is a snapshot: say 'data as of <latest_data_at>' rather "
             "than presenting results as live. 'Today' means the current UTC date."]
    return _result(db.run(sql, []), "*", notes)
