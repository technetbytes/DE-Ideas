#!/bin/sh
set -e

echo "=== Installing dbt packages ==="
dbt deps

echo ""
echo "=== Loading seed data ==="
dbt seed

echo ""
echo "=== Running models ==="
dbt run

echo ""
echo "=== Running tests ==="
dbt test

echo ""
echo "=== Done! All models built and tested successfully ==="
