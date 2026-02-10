from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import utility function from dags/utils
from utils.extract_weather import extract_weather_data


def end_pipeline():
    """
    Final task to mark successful DAG completion.
    """
    print("Weather ETL pipeline completed successfully")



# Default DAG arguments

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# DAG definition

with DAG(
    dag_id="weather_etl_basic",
    description="Basic Airflow DAG for weather data extraction",
    default_args=default_args,
    start_date=datetime(2026, 2, 3),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "weather", "airflow"],
) as dag:

    extract_weather = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
    )

    end_task = PythonOperator(
        task_id="end_pipeline",
        python_callable=end_pipeline,
    )

    # Task dependencies
    extract_weather >> end_task
