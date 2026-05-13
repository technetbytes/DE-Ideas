# Retail Price Intelligence Pipeline

## Project Overview
This project demonstrates an end-to-end Data Engineering pipeline using:
- Python
- PostgreSQL
- Apache Airflow
- Docker
- Metabase

## Features
- Extract retail grocery data
- Transform and clean data
- Load into PostgreSQL warehouse
- Automate ETL using Airflow
- Create dashboards using Metabase

## Real-World Structure

A mature Airflow project often looks like:

```
airflow/
├── dags/
│   ├── etl/
│   │   ├── users_etl.py
│   │   ├── orders_etl.py
│   │   └── products_etl.py
│   ├── reporting/
│   │   ├── monthly_sales_report.py
│   │   └── churn_analysis.py
│   ├── sensors/
│   │   └── external_file_sensor.py
│   └── main_workflow.py
│
├── plugins/
│   ├── custom_operators/
│   │   ├── slack_operator.py
│   │   └── sftp_upload_operator.py
│   └── hooks/
│       └── jira_hook.py
│
├── tasks/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── sql/
│   ├── etl_schema.sql
│   └── views.sql
│
├── data/
│   └── raw/
│   └── processed/
│
├── tests/
│   ├── test_operators.py
│   └── test_hooks.py
│
├── config/
│   ├── settings.yaml
│   └── connections.yaml
│
├── scripts/
│   ├── setup.sh
│   └── run_etl.py
│
└── README.md
```

## Setup Instructions

### Step 1 — Install Dependencies
pip install -r requirements.txt

### Step 2 — Start Docker Services
docker-compose up -d

### Step 3 — Create Database Schema
Connect PostgreSQL container:
docker exec -it retail_postgres psql -U retail -d retail_dw

Then run SQL from:
sql/schema.sql

### Step 4 — Run ETL Pipeline
python scripts/extract.py
python scripts/transform.py
python scripts/load.py

### Step 5 — Airflow DAG
Copy dags/retail_etl_dag.py into Airflow DAG folder.

## PostgreSQL Credentials
Host: localhost
Port: 5432
User: retail
Password: retail123
Database: retail_dw

## Metabase
URL:
http://localhost:3000