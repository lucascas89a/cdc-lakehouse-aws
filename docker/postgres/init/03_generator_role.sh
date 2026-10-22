#!/usr/bin/env bash
# Role for the data generator: DML on the shop schema only, no DDL, no replication.
# On an existing volume, apply it once with:
#   docker compose exec -T -e GENERATOR_PASSWORD postgres bash /docker-entrypoint-initdb.d/03_generator_role.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v pw="$GENERATOR_PASSWORD" -v db="$POSTGRES_DB" <<'SQL'
CREATE ROLE generator WITH LOGIN PASSWORD :'pw';
GRANT CONNECT ON DATABASE :"db" TO generator;
GRANT USAGE ON SCHEMA shop TO generator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA shop TO generator;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA shop TO generator;
SQL
