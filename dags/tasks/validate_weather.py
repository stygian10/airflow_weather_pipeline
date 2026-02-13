import psycopg2


def validate_weather_data():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow",
        port=5432
    )

    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM weather_data;")
    row_count = cursor.fetchone()[0]

    if row_count == 0:
        raise ValueError("Validation failed: weather_data table is empty")

    print(f"Validation passed: {row_count} rows present in weather_data")

    cursor.close()
    conn.close()
