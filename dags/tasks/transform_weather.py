import pandas as pd
from pathlib import Path


def transform_weather_data():
    raw_dir = Path("/opt/airflow/data/raw")
    output_dir = Path("/opt/airflow/dags/outputs/processed")

    output_dir.mkdir(parents=True, exist_ok=True)

    raw_files = sorted(raw_dir.glob("weather_raw_*.csv"))
    if not raw_files:
        raise FileNotFoundError("No raw weather files found")

    latest_file = raw_files[-1]
    df = pd.read_csv(latest_file)

    df.columns = [c.lower() for c in df.columns]
    df.dropna(inplace=True)

    if df.empty:
        raise ValueError("Transformed dataset is empty")

    output_path = output_dir / latest_file.name.replace("raw", "processed")
    df.to_csv(output_path, index=False)

    print(f"Transformed data saved to {output_path}")
