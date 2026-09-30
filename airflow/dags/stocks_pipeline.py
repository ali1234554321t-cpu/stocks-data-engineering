from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="stocks_pipeline",
    start_date=datetime(2026, 9, 29),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["stocks", "data-engineering"],
) as dag:

    kafka_producer = BashOperator(
        task_id="kafka_producer",
        bash_command="cd /opt/airflow && python -m ingestion.kafka_producer_batch",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt build "
            "--project-dir /opt/airflow/dbt "
            "--profiles-dir /home/airflow/.dbt"
        ),
    )

    kafka_producer >> dbt_build