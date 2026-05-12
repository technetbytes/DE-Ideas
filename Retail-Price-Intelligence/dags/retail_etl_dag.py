from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="retail_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/scripts/extract.py"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/scripts/transform.py"
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/scripts/load.py"
    )

    extract >> transform >> load