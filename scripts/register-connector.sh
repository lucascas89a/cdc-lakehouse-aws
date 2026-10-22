#!/usr/bin/env bash
# Create or update the Debezium connector (PUT is idempotent).
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG="$(dirname "$0")/../docker/connect/shopflow-postgres.json"

curl -fsS -X PUT -H "Content-Type: application/json" \
     --data @"$CONFIG" "$CONNECT_URL/connectors/shopflow-postgres/config"
echo
sleep 3
curl -fsS "$CONNECT_URL/connectors/shopflow-postgres/status"
echo
