#!/bin/bash
# ─────────────────────────────────────────────────────────────
# docker/init_postgres.sh
# Runs once on first container start (Docker entrypoint hook).
# Creates all databases/users then applies DDL and seed data.
# ─────────────────────────────────────────────────────────────
set -e

echo ">>> [init] Creating databases and users..."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
  CREATE USER airflow   WITH PASSWORD 'airflow';
  CREATE DATABASE airflow   OWNER airflow;
  GRANT ALL PRIVILEGES ON DATABASE airflow   TO airflow;

  CREATE USER inventory WITH PASSWORD 'inventory';
  CREATE DATABASE inventory OWNER inventory;
  GRANT ALL PRIVILEGES ON DATABASE inventory TO inventory;

  CREATE USER metabase  WITH PASSWORD 'metabase';
  CREATE DATABASE metabase  OWNER metabase;
  GRANT ALL PRIVILEGES ON DATABASE metabase  TO metabase;
EOSQL

echo ">>> [init] Running DDL scripts..."
for f in $(ls /docker-entrypoint-initdb.d/ddl/*.sql | sort); do
  echo "    DDL: $f"
  psql -v ON_ERROR_STOP=1 --username inventory --dbname inventory < "$f"
done

echo ">>> [init] Loading seed data..."
for f in $(ls /docker-entrypoint-initdb.d/seeds/*.sql | sort); do
  echo "    Seed: $f"
  psql -v ON_ERROR_STOP=1 --username inventory --dbname inventory < "$f"
done

echo ">>> [init] Done. Inventory database is ready."
