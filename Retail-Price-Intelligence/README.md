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