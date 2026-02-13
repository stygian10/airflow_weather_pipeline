from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.extract_weather import extract_weather_data
from tasks.transform_weather import transform_weather_data
from tasks.load_weather import load_weather_data
from tasks.validate_weather import validate_weather_data



default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def end_pipeline():
    print("Weather ETL pipeline finished")


with DAG(
    dag_id="weather_etl_basic",
    default_args=default_args,
    start_date=datetime(2026, 2, 12),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "weather", "airflow"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
    )

    transform_task = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather_data,
    )

    load_task = PythonOperator(
        task_id="load_weather_data",
        python_callable=load_weather_data,
    )

    validate_task = PythonOperator(
        task_id="validate_weather_data",
        python_callable=validate_weather_data,
    )


    end_task = PythonOperator(
        task_id="end_pipeline",
        python_callable=end_pipeline,
    )

    


extract_task >> transform_task >> load_task >> validate_task >> end_task


