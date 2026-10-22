#!/usr/bin/env bash
# Separate database and role for Airflow's metadata. The Debezium publication lives in the
# shopflow database, so Airflow's own writes are never captured as CDC events.
# On an existing volume, apply it once with:
#   docker compose exec -T postgres bash /docker-entrypoint-initdb.d/04_airflow_db.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres -v pw="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE airflow WITH LOGIN PASSWORD :'pw';
CREATE DATABASE airflow OWNER airflow;
REVOKE CONNECT ON DATABASE airflow FROM PUBLIC;
SQL
