"""MCP server exposing read-only business tools over the ShopFlow gold layer.

Runs over stdio, so any MCP client (Claude Code, Claude Desktop, an agent framework) can launch it.
Credentials: the cdc-lakehouse-agent IAM user, which can only run Athena queries in the lake
workgroup and reaches gold exclusively through Athena.
"""

import os
from typing import Literal

from mcp.server.mcpserver import MCPServer
from mcp_types import ToolAnnotations

import tools
from athena import AthenaRunner

INSTRUCTIONS = """\
Answer questions about ShopFlow, a LATAM e-commerce, using only these tools; never guess numbers.
Data comes from the gold layer of a lakehouse that is rebuilt on a schedule: call data_freshness when the
user asks about "today" or "now", and say "data as of ..." when results may lag.
Money is in USD. Rates are fractions: show them as percentages. Durations come from a simulation with a
compressed clock (1 simulated hour = 1 real minute). Customers are identified only by customer_id.
Relay the notes returned in each result's meta when they change how the numbers should be read.
"""

db = AthenaRunner(workgroup=os.environ.get("ATHENA_WORKGROUP", "cdc-lakehouse"),
                  database=os.environ.get("ATHENA_DATABASE", "shopflow_gold"))
mcp = MCPServer("shopflow-gold", instructions=INSTRUCTIONS)
READ_ONLY = ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False)

Country = Literal["AR", "MX", "CL", "CO", "UY", "PE"]
Category = Literal["home", "electronics", "sports", "books", "beauty"]
Provider = Literal["mercadopago", "stripe", "paypal"]
Carrier = Literal["Andreani", "OCA", "Correo Argentino", "Estafeta", "Chilexpress", "Servientrega"]
LateType = Literal["open_overdue", "delivered_late", "all"]


@mcp.tool(annotations=READ_ONLY)
def get_sales_summary(
    date_from: str | None = None,
    date_to: str | None = None,
    country: Country | None = None,
    category: Category | None = None,
    group_by: Literal["none", "day", "country", "category", "country_category", "day_country"] = "country",
) -> dict:
    """Revenue, units and order count for a date range (ISO dates, default today UTC, max 92 days),
    optionally by day, country and/or category. Counts orders currently paid, shipped or delivered, by
    creation date. Does not answer profit, margin, refunds or per-customer questions."""
    return tools.sales_summary(db, date_from, date_to, country, category, group_by)


@mcp.tool(annotations=READ_ONLY)
def get_payment_failure_rate(
    date_from: str | None = None,
    date_to: str | None = None,
    provider: Provider | None = None,
    group_by: Literal["provider", "day_provider"] = "provider",
) -> dict:
    """Share of payment attempts that failed, by provider (optionally by day). Default: last 7 days.
    Counts attempts, so retries count several times. For why payments fail use
    get_payment_failure_reasons."""
    return tools.payment_failure_rate(db, date_from, date_to, provider, group_by)


@mcp.tool(annotations=READ_ONLY)
def get_payment_failure_reasons(
    date_from: str | None = None,
    date_to: str | None = None,
    provider: Provider | None = None,
    top_n: int = 10,
) -> dict:
    """Why payments fail: failures per failure_code and provider, with their share of the provider's
    failures and attempts. Default: last 7 days, top 10 (max 50). No per-order or per-customer detail."""
    return tools.payment_failure_reasons(db, date_from, date_to, provider, top_n)


@mcp.tool(annotations=READ_ONLY)
def get_late_shipments(
    late_type: LateType = "open_overdue",
    carrier: Carrier | None = None,
    country: Country | None = None,
    limit: int = 20,
) -> dict:
    """Shipments late against their promised date as of the last gold build: open_overdue (not delivered
    and past the promise) or delivered_late. Returns up to `limit` (max 50) with total_matching. Cannot
    give a '% late' (on-time parcels are not stored). No customer data."""
    return tools.late_shipments(db, late_type, carrier, country, limit)


@mcp.tool(annotations=READ_ONLY)
def get_late_shipments_by_carrier(late_type: LateType = "open_overdue", country: Country | None = None) -> dict:
    """Number and average delay of late shipments by carrier and country, for a 'who is failing' view.
    Ranks by late volume, not late rate. Use get_late_shipments for individual orders."""
    return tools.late_shipments_by_carrier(db, late_type, country)


@mcp.tool(annotations=READ_ONLY)
def get_order_lead_times(
    date_from: str | None = None,
    date_to: str | None = None,
    country: Country | None = None,
    group_by: Literal["none", "country"] = "none",
) -> dict:
    """How long orders take from pending to delivered and per stage (to pay, to ship, in transit):
    average, p50 and p90, for orders created in the date range (default last 7 days). Not for tracking
    individual orders."""
    return tools.order_lead_times(db, date_from, date_to, country, group_by)


@mcp.tool(annotations=READ_ONLY)
def get_top_customers_ltv(top_n: int = 10, country: Country | None = None, min_orders: int = 1) -> dict:
    """Top customers by all-time revenue, identified only by customer_id (never names or emails).
    top_n max 50."""
    return tools.top_customers_ltv(db, top_n, country, min_orders)


@mcp.tool(annotations=READ_ONLY)
def data_freshness() -> dict:
    """Latest data timestamp in each gold table. Call it first for questions about 'today' or 'now', or
    when a result is empty or looks stale."""
    return tools.data_freshness(db)


if __name__ == "__main__":
    mcp.run("stdio")
