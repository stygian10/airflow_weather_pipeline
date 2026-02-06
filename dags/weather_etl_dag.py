from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def start_pipeline():
    print("Weather ETL pipeline started")


def validate_environment():
    print("Environment validated successfully")
    validate = PythonOperator(
    task_id="validate_environment",
    python_callable=validate_environment,


def end_pipeline():
    print("Weather ETL pipeline finished")


with DAG(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2026, 2, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "weather"],
) as dag:

    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )

    validate = PythonOperator(
        task_id="validate_environment",
        python_callable=validate_environment,
    )

    end = PythonOperator(
        task_id="end_pipeline",
        python_callable=end_pipeline,
    )

    start >> validate >> end
