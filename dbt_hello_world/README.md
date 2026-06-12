# dbt Hello World (Dockerized)

A minimal, self-contained dbt project. No local Python, no `~/.dbt/profiles.yml` — just Docker.

## Quick Start

```bash
docker compose up --build
```

That's it. Seeds load, models run, tests pass — all inside the container.

## Project Structure

```
dbt_hello_world/
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh          # runs seed → run → test
├── .dockerignore
├── dbt_project.yml
├── profiles.yml           # lives HERE, not in ~/.dbt/
├── packages.yml
├── seeds/
│   ├── customers.csv
│   └── orders.csv
├── models/
│   ├── schema.yml
│   ├── stg_customers.sql
│   ├── stg_orders.sql
│   └── customer_orders.sql
└── README.md
```

## What Happens

1. Docker builds a Python image with `dbt-duckdb`
2. `entrypoint.sh` runs the full pipeline: `dbt deps` → `dbt seed` → `dbt run` → `dbt test`
3. Results (compiled SQL, DuckDB file) land in `./target/` on your host via volume mount

## Run Individual Commands

```bash
# Interactive shell inside the container
docker compose run dbt sh

# Then inside:
dbt seed
dbt run
dbt test
dbt run --select customer_orders
```

## Why This Works Without ~/.dbt/profiles.yml

The `Dockerfile` sets `DBT_PROFILES_DIR=/dbt`, so dbt reads `profiles.yml` from the project root inside the container. Nothing touches your home directory.

## Prerequisites

- Docker & Docker Compose (that's all)
