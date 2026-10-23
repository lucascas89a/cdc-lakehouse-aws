# AI agent tools over the gold layer

Implemented as an MCP server in [`agent/`](../agent) (`server.py`, `tools.py`, `athena.py`, offline tests in
`test_tools.py`). Implementation notes that differ from the first design:

- `LIMIT` values (`top_n`, `limit`) are validated integers spliced into the SQL instead of `?`
  placeholders; every other value goes through `ExecutionParameters`.
- `data_freshness` covers five tables (`payment_failure_reasons` is left out: it shares dates with
  `payment_provider_daily`) and casts `NULL` explicitly.
- The agent runs as a dedicated IAM user that can only call Athena in the `cdc-lakehouse` workgroup;
  it reaches Glue and S3 exclusively through Athena (`aws:CalledVia`), and is denied `gold/_staging/`,
  `silver/` and `bronze/`.

The agent never writes SQL. It calls a fixed set of read-only tools; each tool maps to one business
question and runs one parameterized Athena query against `shopflow_gold` (Athena engine v3, Trino SQL,
workgroup `cdc-lakehouse`, 1 GB scan limit per query).

## Conventions shared by all tools

- **Execution.** `StartQueryExecution` with `QueryString` containing `?` placeholders and
  `ExecutionParameters` (strings, in placeholder order). The wrapper sends string values as SQL
  literals (`'AR'`, `'2026-10-01'`), integers as bare numbers. The `?` placeholders are never
  concatenated. The only text substituted into SQL are fixed snippets chosen from an allow-list in the
  wrapper (the `{dims}` / `{group_expr}` fragments below), never user input.
- **Validation (before any query).** Dates must be ISO `YYYY-MM-DD`, `date_from <= date_to`, range
  at most 92 days. Enumerated values are checked against closed lists:
  - `country`: AR, MX, CL, CO, UY, PE
  - `category`: home, electronics, sports, books, beauty
  - `provider`: mercadopago, stripe, paypal
  - `carrier`: Andreani, OCA, Correo Argentino, Estafeta, Chilexpress, Servientrega
  - `limit` / `top_n`: integer 1..50.
  Anything else returns a validation error that lists the allowed values; no query is run.
- **Optional filters.** An omitted filter is passed as the string `ALL`, and the SQL uses
  `(? = 'ALL' OR col = ?)`, so the placeholder count is fixed. Each such filter therefore consumes two
  parameters (same value twice).
- **Defaults for dates.** When `date_from` / `date_to` are omitted the wrapper resolves them to the
  current UTC date (`today`), so the SQL always receives explicit dates.
- **Output.** Rows as JSON (`columns`, `rows`) plus `meta`: `table`, `rows_returned`, `data_as_of`
  (see Guardrails), `currency` where relevant, and `notes` (strings the LLM must relay).
- **Time scale warning.** The generator compresses time (1 simulated hour = 1 real minute). Every
  `hours_*` value is in real hours and is small (minutes to a few hours); it must not be read as
  production-realistic delivery times.

---

## 1. `get_sales_summary`

**Description for the LLM.** Revenue, units and order count for a date range, optionally broken down by
day, country and/or category. Use it for "how much did we sell today / this week". It counts only
orders currently in status paid, shipped or delivered (not pending or cancelled), by order creation
date. It does not answer profit, margin, refunds or per-customer questions.

| Parameter | Type | Required | Allowed / limits | Default |
|---|---|---|---|---|
| `date_from` | string (ISO date) | no | range <= 92 days | today (UTC) |
| `date_to` | string (ISO date) | no | >= `date_from` | today (UTC) |
| `country` | string | no | AR, MX, CL, CO, UY, PE | all |
| `category` | string | no | home, electronics, sports, books, beauty | all |
| `group_by` | enum | no | `none`, `day`, `country`, `category`, `country_category`, `day_country` | `country` |

`{dims}` and `{group_expr}` come from a fixed map keyed by `group_by`
(e.g. `country` -> dims `country`, `ORDER BY revenue DESC`; `none` -> `'all'` literal, no dimension).

```sql
SELECT {dims}
       sum(orders)                         AS orders,
       sum(units)                          AS units,
       CAST(sum(revenue) AS DECIMAL(14,2)) AS revenue_usd,
       CAST(sum(revenue) / nullif(sum(orders), 0) AS DECIMAL(12,2)) AS avg_order_value_usd
FROM shopflow_gold.daily_sales
WHERE order_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
  AND (? = 'ALL' OR country  = ?)                              -- country, country
  AND (? = 'ALL' OR category = ?)                              -- category, category
GROUP BY {group_expr}
ORDER BY {order_expr}
LIMIT 50
```

Note: `sum(orders)` over a category breakdown double counts an order that contains several categories.
The tool only reports the order total when `group_by` does not include `category`; otherwise it returns
`orders_with_category` and the notes say orders are not additive across categories.

**Output columns.** `[order_date] [country] [category] orders, units, revenue_usd, avg_order_value_usd`.
**Example.** "How much did we sell this week in Mexico, by category?"
-> `{date_from: monday, date_to: today, country: "MX", group_by: "category"}`.

---

## 2. `get_payment_failure_rate`

**Description for the LLM.** Share of payment attempts that failed, by provider (optionally by day).
Use it for "what percentage of payments fail". Counts attempts, so a retried payment counts as several
attempts; it is not the share of orders that could not be paid. For the reason behind failures use
`get_payment_failure_reasons`.

| Parameter | Type | Required | Allowed / limits | Default |
|---|---|---|---|---|
| `date_from` | string (ISO date) | no | range <= 92 days | today - 6 days |
| `date_to` | string (ISO date) | no | >= `date_from` | today (UTC) |
| `provider` | string | no | mercadopago, stripe, paypal | all |
| `group_by` | enum | no | `provider`, `day_provider` | `provider` |

```sql
SELECT {dims}
       sum(attempts)                                                   AS attempts,
       sum(failed)                                                     AS failed,
       sum(captured)                                                   AS captured,
       CAST(sum(failed) * 1.0 / nullif(sum(attempts), 0) AS DECIMAL(5,4)) AS failure_rate,
       CAST(sum(captured_amount) AS DECIMAL(14,2))                     AS captured_amount_usd
FROM shopflow_gold.payment_provider_daily
WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
  AND (? = 'ALL' OR provider = ?)                                -- provider, provider
GROUP BY {group_expr}
ORDER BY {order_expr}
LIMIT 50
```

The rate is always recomputed as `sum(failed) / sum(attempts)`; averaging the daily `failure_rate`
column would be wrong.

**Output columns.** `[attempt_date] provider, attempts, failed, captured, failure_rate, captured_amount_usd`.
**Example.** "Which payment provider fails the most this week?"

---

## 3. `get_payment_failure_reasons`

**Description for the LLM.** Why payments fail: failure count per `failure_code`, per provider, with the
share of all failures and the share of all attempts. Use it after `get_payment_failure_rate`, or for
"what is the main reason payments fail on Stripe". It does not give per-customer or per-order detail.

| Parameter | Type | Required | Allowed / limits | Default |
|---|---|---|---|---|
| `date_from` | string (ISO date) | no | range <= 92 days | today - 6 days |
| `date_to` | string (ISO date) | no | >= `date_from` | today (UTC) |
| `provider` | string | no | mercadopago, stripe, paypal | all |
| `top_n` | integer | no | 1..50 | 10 |

`failure_code` is free text from the source (e.g. `insufficient_funds`, `card_declined`), so it is
returned as data and not validated against a list.

```sql
WITH reasons AS (
    SELECT provider, failure_code, sum(failures) AS failures
    FROM shopflow_gold.payment_failure_reasons
    WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
      AND (? = 'ALL' OR provider = ?)                                -- provider, provider
    GROUP BY provider, failure_code
),
attempts AS (
    SELECT provider, sum(attempts) AS attempts
    FROM shopflow_gold.payment_provider_daily
    WHERE attempt_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
      AND (? = 'ALL' OR provider = ?)                                -- provider, provider
    GROUP BY provider
)
SELECT r.provider,
       coalesce(r.failure_code, 'unknown')                                   AS failure_code,
       r.failures,
       CAST(r.failures * 1.0 / sum(r.failures) OVER (PARTITION BY r.provider) AS DECIMAL(5,4)) AS share_of_provider_failures,
       CAST(r.failures * 1.0 / nullif(a.attempts, 0) AS DECIMAL(5,4))        AS share_of_provider_attempts
FROM reasons r
JOIN attempts a ON a.provider = r.provider
ORDER BY r.failures DESC
LIMIT ?                                                                      -- top_n
```

**Output columns.** `provider, failure_code, failures, share_of_provider_failures, share_of_provider_attempts`.
**Example.** "What are the top reasons payments fail with MercadoPago?"

---

## 4. `get_late_shipments`

**Description for the LLM.** Shipments late against their promised date, as of the last gold build.
`open_overdue` = not delivered yet and past `promised_at`; `delivered_late` = delivered after
`promised_at`. Use it for "which orders are delayed now". It does not return on-time shipments, so it
cannot give a "% late"; returned shipments are not included. No customer data is returned.

| Parameter | Type | Required | Allowed / limits | Default |
|---|---|---|---|---|
| `late_type` | enum | no | `open_overdue`, `delivered_late`, `all` | `open_overdue` |
| `carrier` | string | no | Andreani, OCA, Correo Argentino, Estafeta, Chilexpress, Servientrega | all |
| `country` | string | no | AR, MX, CL, CO, UY, PE | all |
| `limit` | integer | no | 1..50 | 20 |

```sql
SELECT shipment_id,
       order_id,
       carrier,
       country,
       status,
       late_type,
       promised_at,
       shipped_at,
       delivered_at,
       hours_late,
       snapshot_at,
       count(*) OVER () AS total_matching
FROM shopflow_gold.late_shipments
WHERE (? = 'all' OR late_type = ?)        -- late_type, late_type
  AND (? = 'ALL' OR carrier = ?)          -- carrier, carrier
  AND (? = 'ALL' OR country = ?)          -- country, country
ORDER BY hours_late DESC
LIMIT ?                                   -- limit
```

Notes the tool adds: `hours_late` is measured at `snapshot_at`, not now, so open shipments keep
growing until the next build; units are real hours under the compressed clock. `total_matching` lets
the agent say "showing 20 of 134".

**Output columns.** `shipment_id, order_id, carrier, country, status, late_type, promised_at, shipped_at, delivered_at, hours_late, snapshot_at, total_matching`.
**Example.** "Which orders are late right now and with which carrier?"

---

## 5. `get_late_shipments_by_carrier`

**Description for the LLM.** Count and average delay of late shipments grouped by carrier and country,
for a quick "who is failing" view. Use `get_late_shipments` for the individual orders. Counts only late
shipments (no denominator), so it ranks carriers by late volume, not by late rate.

| Parameter | Type | Required | Allowed / limits | Default |
|---|---|---|---|---|
| `late_type` | enum | no | `open_overdue`, `delivered_late`, `all` | `open_overdue` |
| `country` | string | no | AR, MX, CL, CO, UY, PE | all |

```sql
SELECT carrier,
       country,
       count(*)                                    AS late_shipments,
       CAST(avg(hours_late) AS DECIMAL(10,2))      AS avg_hours_late,
       max(hours_late)                             AS max_hours_late,
       max(snapshot_at)                            AS snapshot_at
FROM shopflow_gold.late_shipments
WHERE (? = 'all' OR late_type = ?)   -- late_type, late_type
  AND (? = 'ALL' OR country = ?)     -- country, country
GROUP BY carrier, country
ORDER BY late_shipments DESC
LIMIT 50
```

**Output columns.** `carrier, country, late_shipments, avg_hours_late, max_hours_late, snapshot_at`.
**Example.** "Which carrier has the most delayed parcels in Argentina?"

---

## 6. `get_order_lead_times`

**Description for the LLM.** How long orders take from `pending` to `delivered`, and per stage
(pending to paid, paid to shipped, shipped to delivered): average, median and p90 in real hours, for
orders created in a date range. Only orders that reached the stage are included (`orders_measured`).
Hours are real hours under a compressed simulation clock (1 simulated hour = 1 real minute), so
multiply by 60 to read them as simulated hours. Not suited to individual order tracking.

| Parameter | Type | Required | Allowed / limits | Default |
|---|---|---|---|---|
| `date_from` | string (ISO date) | no | range <= 92 days; applies to order creation date | today - 6 days |
| `date_to` | string (ISO date) | no | >= `date_from` | today (UTC) |
| `country` | string | no | AR, MX, CL, CO, UY, PE | all (one row per country if `group_by=country`) |
| `group_by` | enum | no | `none`, `country` | `none` |

```sql
SELECT {dims}
       count(*)                                         AS orders_created,
       count(hours_end_to_end)                          AS orders_delivered,
       CAST(avg(hours_to_pay)       AS DECIMAL(10,2))   AS avg_hours_to_pay,
       CAST(avg(hours_to_ship)      AS DECIMAL(10,2))   AS avg_hours_to_ship,
       CAST(avg(hours_in_transit)   AS DECIMAL(10,2))   AS avg_hours_in_transit,
       CAST(avg(hours_end_to_end)   AS DECIMAL(10,2))   AS avg_hours_end_to_end,
       CAST(approx_percentile(hours_end_to_end, 0.5) AS DECIMAL(10,2)) AS p50_hours_end_to_end,
       CAST(approx_percentile(hours_end_to_end, 0.9) AS DECIMAL(10,2)) AS p90_hours_end_to_end
FROM shopflow_gold.order_lead_times
WHERE CAST(created_at AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)   -- date_from, date_to
  AND (? = 'ALL' OR country = ?)                                             -- country, country
GROUP BY {group_expr}
ORDER BY {order_expr}
LIMIT 50
```

`approx_percentile` returns `double`; wrapper casts as shown. Averages ignore NULLs, so each stage has its
own implicit sample size; the tool also returns `orders_delivered` and the notes warn that recent
orders not yet delivered are excluded (right-censoring biases short ranges toward faster times).

**Output columns.** `[country] orders_created, orders_delivered, avg_hours_to_pay, avg_hours_to_ship, avg_hours_in_transit, avg_hours_end_to_end, p50_hours_end_to_end, p90_hours_end_to_end`.
**Example.** "How long does an order take from pending to delivered?"

---

## 7. `get_top_customers_ltv`

**Description for the LLM.** Top customers by lifetime revenue (sum of order lines for paid, shipped or
delivered orders), identified only by `customer_id`. Use it for "who are our best customers". It never
returns names, emails or email domains; to find out who a customer is, a human must look up the id in
the source system. Revenue is all-time, not limited to a period.

| Parameter | Type | Required | Allowed / limits | Default |
|---|---|---|---|---|
| `top_n` | integer | no | 1..50 | 10 |
| `country` | string | no | AR, MX, CL, CO, UY, PE | all |
| `min_orders` | integer | no | 1..1000 | 1 |

```sql
SELECT customer_id,
       country,
       orders,
       revenue                AS lifetime_revenue_usd,
       avg_order_value        AS avg_order_value_usd,
       first_order_at,
       last_order_at,
       rank() OVER (ORDER BY revenue DESC) AS revenue_rank
FROM shopflow_gold.customer_ltv
WHERE (? = 'ALL' OR country = ?)   -- country, country
  AND orders >= ?                  -- min_orders
ORDER BY revenue DESC, customer_id
LIMIT ?                            -- top_n
```

The explicit column list is the PII guardrail: `email_domain` exists in gold but is deliberately not
selected (quasi-identifier). `revenue_rank` is computed after the filters, so it is the rank within the
filtered set.

**Output columns.** `customer_id, country, orders, lifetime_revenue_usd, avg_order_value_usd, first_order_at, last_order_at, revenue_rank`.
**Example.** "Who are our 10 best customers in Chile?"

---

## 8. `data_freshness` (optional, metadata)

**Description for the LLM.** Reports the latest data date in each gold table. Call it first when the user
asks about "today" or "now", or whenever a result looks empty or stale. Takes no parameters.

```sql
SELECT 'daily_sales' AS table_name, CAST(max(order_date) AS TIMESTAMP) AS latest_data_at, NULL AS built_at
FROM shopflow_gold.daily_sales
UNION ALL
SELECT 'payment_provider_daily', CAST(max(attempt_date) AS TIMESTAMP), NULL
FROM shopflow_gold.payment_provider_daily
UNION ALL
SELECT 'payment_failure_reasons', CAST(max(attempt_date) AS TIMESTAMP), NULL
FROM shopflow_gold.payment_failure_reasons
UNION ALL
SELECT 'late_shipments', max(coalesce(delivered_at, shipped_at, promised_at)), max(snapshot_at)
FROM shopflow_gold.late_shipments
UNION ALL
SELECT 'order_lead_times', max(coalesce(delivered_at, shipped_at, paid_at, created_at)), NULL
FROM shopflow_gold.order_lead_times
UNION ALL
SELECT 'customer_ltv', max(last_order_at), NULL
FROM shopflow_gold.customer_ltv
```

Types differ across branches, hence the explicit casts; `NULL` in the first branch is cast by Athena to
the type of the later branches (`TIMESTAMP`). If that fails in practice, write `CAST(NULL AS TIMESTAMP)`.

**Output columns.** `table_name, latest_data_at, built_at`.
**Example.** "Is the data up to date?" / "Up to which date do we have sales?"

---

## Guardrails

- **Hard limits.** Read-only role (Athena `athena:StartQueryExecution` and Glue read on `shopflow_gold`
  only, S3 read on `gold/`), workgroup scan cap of 1 GB, at most 50 rows per call, date ranges up to 92
  days, 1 query per tool call, tool timeout (e.g. 30 s) with a clear error message. Cache identical
  calls for a short TTL (60 s) to cap cost.
- **Zero rows.** Never invent numbers. Return `rows: []` plus a note: "No rows for these filters
  between X and Y; data available until Z (see data_freshness)." The agent should say that, suggest a
  wider range or removing a filter, and not treat 0 rows as 0 sales. `late_shipments` and
  `payment_failure_reasons` can legitimately be empty (the audit allows it); for these, empty means
  "nothing late / no failures as of the last build".
- **Units and currency.** All money is USD: `orders.currency` defaults to `'USD'` in the source schema
  but is not carried into gold, so the tools label every amount `_usd` and state the assumption; no
  currency conversion is done. `failure_rate` is a fraction (0.0842 = 8.42%); the agent must show it as
  a percentage. `hours_*` are real hours under the compressed clock (1 simulated hour = 1 real minute).
- **Freshness.** Gold is a full rebuild by a scheduled job, so it is a snapshot, not real time. Every
  response carries `data_as_of`: for `late_shipments` it is `snapshot_at`; for the other tables it is the
  latest date available (`max(order_date)`, `max(attempt_date)`, `max(last_order_at)`, ...). If the
  requested `date_to` is later than `data_as_of`, or `data_as_of` is more than the expected build
  interval old, the agent must say "data as of ..." instead of presenting it as live. "Today" means the
  current UTC date, which may not match a user's local date in LATAM.
- **PII.** Gold has no names or emails; the tools additionally never select `email_domain`. Customer
  ids are the only identifier returned; shipment and order ids are allowed.
- **Fixed SQL.** No tool accepts free text that reaches SQL. Enumerations and ranges are validated in
  the wrapper before the query, values go through `ExecutionParameters`, and the few structural
  fragments (`group_by`) are picked from an allow-list.
- **Consistency across tables.** Publish is atomic per table, not across tables, so right after a build
  two tools may briefly disagree. Revenue in `daily_sales` and `customer_ltv` is reconciled by the audit
  and should match when summed over all time.

## Data limitations found (gaps against the 5 questions)

1. **Late rate impossible.** `late_shipments` stores only late rows; there is no on-time count per
   carrier/country, so Q3 can list and rank by volume but not give "% late". Fix: add a
   `shipments_daily` (date, carrier, country, shipments, delivered, late) gold table.
2. **Late status is frozen at build.** `hours_late` and `open_overdue` use `current_timestamp()` at build
   time; a shipment that became overdue after the last build does not show up until the next one.
   Returned shipments (`status = 'returned'`) are excluded entirely.
3. **No build timestamp except `snapshot_at`** (only in `late_shipments`, which can be empty). Suggest a
   `gold_build_info` table (table_name, built_at, silver versions from the manifest) for `data_freshness`.
4. **Failure reasons have no denominator by themselves.** Reasons by provider need a join with
   `payment_provider_daily` (done in tool 3). The grain is payment attempt, not order, so retries inflate
   attempts; there is no order-level "payment ultimately failed" metric. `failure_code` is free text and
   can be NULL (shown as `unknown`).
5. **Sales are by order creation date and current status.** An order that was paid and later cancelled
   leaves the figures retroactively; there is no payment-date view and no refunds/cancellations in
   `daily_sales`. Orders are not additive across categories (a multi-category order is counted in each).
6. **No currency column in gold**; USD is an assumption taken from the source DDL default.
7. **Lead times: no category, no carrier, only country**; stages are the first occurrence of each status
   (`min(changed_at)`); cancelled orders have `cancelled_at` but no duration. Averages over short, recent
   windows are biased low because undelivered orders are excluded. Under the compressed clock all
   durations are tiny and not realistic.
8. **`customer_ltv` has `email_domain`**, a quasi-identifier, which is excluded from the tool; LTV is
   all-time and cannot be filtered by period. No customer segment or name exists in gold (by design).
