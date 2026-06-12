FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-duckdb

WORKDIR /dbt

COPY . /dbt

# profiles.yml lives inside the project — tell dbt where to find it
ENV DBT_PROFILES_DIR=/dbt

ENTRYPOINT ["sh", "entrypoint.sh"]
