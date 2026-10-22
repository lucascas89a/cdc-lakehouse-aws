#!/usr/bin/env bash
# Register (or refresh) the gold Delta tables in the Glue Data Catalog through Athena DDL.
# Athena reads each table's schema from its Delta transaction log, so re-running this after a
# gold schema change is all it takes to keep the catalog in sync. Safe to run any time.
#
# Needs an AWS identity allowed to run Athena DDL (not the pipeline user), and the gold tables
# already published by `jobs/gold_build.py publish`.
set -euo pipefail

WORKGROUP="${WORKGROUP:-cdc-lakehouse}"
DATABASE="${DATABASE:-shopflow_gold}"
LAKE_BUCKET="${LAKE_BUCKET:-cdc-lakehouse-lake-$(aws sts get-caller-identity --query Account --output text)}"
TABLES=(daily_sales payment_provider_daily payment_failure_reasons late_shipments order_lead_times customer_ltv)

run() {
  local id state
  id=$(aws athena start-query-execution --work-group "$WORKGROUP" \
         --query-execution-context "Database=$DATABASE" --query-string "$1" \
         --query QueryExecutionId --output text)
  while :; do
    state=$(aws athena get-query-execution --query-execution-id "$id" \
              --query 'QueryExecution.Status.[State,StateChangeReason]' --output text)
    case "$state" in
      SUCCEEDED*) return 0 ;;
      FAILED*|CANCELLED*) echo "query failed: $state" >&2; return 1 ;;
    esac
    sleep 1
  done
}

for t in "${TABLES[@]}"; do
  run "DROP TABLE IF EXISTS $t"
  run "CREATE EXTERNAL TABLE $t LOCATION 's3://$LAKE_BUCKET/gold/$t/' TBLPROPERTIES ('table_type' = 'DELTA')"
  echo "registered $DATABASE.$t"
done
