# Database Migrations

Place future schema changes here as numbered SQL files:

```
migrations/
├── V001__add_batch_id_to_pipeline_runs.sql
├── V002__add_inventory_forecast_table.sql
└── ...
```

## Naming convention

`V{version}__{description}.sql`  — double underscore separator.

## Applying migrations

```bash
# Using psql directly
psql -U inventory -d inventory -f database/migrations/V001__add_batch_id.sql

# Or via Docker
docker exec -i inventory_postgres \
  psql -U inventory -d inventory \
  < database/migrations/V001__add_batch_id.sql
```

For teams: consider adopting [Flyway](https://flywaydb.org/) or
[Liquibase](https://www.liquibase.org/) for automated migration management.
