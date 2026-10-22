#!/usr/bin/env bash
# Least-privilege role for Debezium: replication + read-only on the shop schema.
# The publication already exists (01_schema.sql), so Debezium does not need to own the tables.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v pw="$DEBEZIUM_PASSWORD" -v db="$POSTGRES_DB" <<'SQL'
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD :'pw';
GRANT CONNECT ON DATABASE :"db" TO debezium;
GRANT USAGE ON SCHEMA shop TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA shop GRANT SELECT ON TABLES TO debezium;
SQL
