#!/usr/bin/env bash
# By default every role (PUBLIC) may connect to every database, so the airflow role could open
# shopflow and read whatever PUBLIC can. Runs last so the explicit grants below are the only
# way in (shopflow_admin is superuser and bypasses this). Idempotent.
# On an existing volume, apply it once with:
#   docker compose exec -T postgres bash /docker-entrypoint-initdb.d/05_revoke_public_connect.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v db="$POSTGRES_DB" <<'SQL'
REVOKE CONNECT ON DATABASE :"db" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO debezium, generator;
SQL
