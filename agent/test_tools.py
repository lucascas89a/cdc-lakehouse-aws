"""Offline tests for the agent tools: validation, parameter binding and PII guardrails.
No AWS access needed (Athena is replaced by a recorder).  Run: python -m unittest test_tools"""

import unittest

import tools
from athena import sql_literal


class Recorder:
    """Stands in for AthenaRunner and records what would have been sent."""

    def __init__(self):
        self.calls = []

    def run(self, sql, params):
        self.calls.append((sql, params))
        return {"columns": ["x"], "rows": [[1]], "scanned_bytes": 0}


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.db = Recorder()

    def test_rejects_values_outside_closed_lists(self):
        for call in (lambda: tools.sales_summary(self.db, country="BR"),
                     lambda: tools.payment_failure_rate(self.db, provider="visa"),
                     lambda: tools.late_shipments(self.db, carrier="DHL"),
                     lambda: tools.sales_summary(self.db, group_by="customer")):
            with self.assertRaises(tools.ValidationError):
                call()
        self.assertEqual(self.db.calls, [], "no query may run when validation fails")

    def test_injection_attempts_never_reach_sql(self):
        with self.assertRaises(tools.ValidationError):
            tools.sales_summary(self.db, country="AR' OR '1'='1")
        with self.assertRaises(tools.ValidationError):
            tools.sales_summary(self.db, date_from="2026-10-01; DROP TABLE x")

    def test_date_rules(self):
        with self.assertRaises(tools.ValidationError):
            tools.sales_summary(self.db, date_from="2026-10-10", date_to="2026-10-01")
        with self.assertRaises(tools.ValidationError):
            tools.sales_summary(self.db, date_from="2026-01-01", date_to="2026-10-01")   # > 92 days

    def test_limits(self):
        for bad in (0, 51, True, "10"):
            with self.assertRaises(tools.ValidationError):
                tools.top_customers_ltv(self.db, top_n=bad)

    def test_parameters_are_bound_not_concatenated(self):
        tools.sales_summary(self.db, date_from="2026-10-01", date_to="2026-10-07", country="MX")
        sql, params = self.db.calls[0]
        self.assertNotIn("MX", sql)
        self.assertNotIn("2026-10-01", sql)
        self.assertEqual(params, ["2026-10-01", "2026-10-07", "MX", "MX", "ALL", "ALL"])
        self.assertEqual(sql.count("?"), len(params))

    def test_placeholder_count_matches_for_every_tool(self):
        calls = [lambda: tools.sales_summary(self.db), lambda: tools.payment_failure_rate(self.db),
                 lambda: tools.payment_failure_reasons(self.db), lambda: tools.late_shipments(self.db),
                 lambda: tools.late_shipments_by_carrier(self.db), lambda: tools.order_lead_times(self.db),
                 lambda: tools.top_customers_ltv(self.db), lambda: tools.data_freshness(self.db)]
        for call in calls:
            call()
        for sql, params in self.db.calls:
            self.assertEqual(sql.count("?"), len(params), sql)

    def test_customer_tool_never_selects_pii(self):
        tools.top_customers_ltv(self.db)
        sql = self.db.calls[0][0].lower()
        for column in ("email", "full_name", "email_domain"):
            self.assertNotIn(column, sql)

    def test_category_breakdown_renames_non_additive_order_count(self):
        result = tools.sales_summary(self.db, group_by="category")
        self.assertIn("orders_with_category", self.db.calls[0][0])
        self.assertTrue(any("not additive" in n for n in result["meta"]["notes"]))

    def test_sql_literal_quoting(self):
        self.assertEqual(sql_literal("O'Brien"), "'O''Brien'")
        self.assertEqual(sql_literal(5), "5")


if __name__ == "__main__":
    unittest.main()
