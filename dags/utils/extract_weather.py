import requests
import pandas as pd
from datetime import datetime
from pathlib import Path


def extract_weather_data():
    """
    Extract hourly weather data from Open-Meteo API
    and save it as a raw CSV file.

    This function is designed to be executed
    inside an Airflow PythonOperator.
    """

    # -----------------------------
    # API configuration
    # -----------------------------
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 51.5072,     # London
        "longitude": -0.1276,
        "hourly": [
            "temperature_2m",
            "relativehumidity_2m",
            "windspeed_10m"
        ],
        "timezone": "UTC"
    }

    # -----------------------------
    # Call API
    # -----------------------------
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    # -----------------------------
    # Convert to DataFrame
    # -----------------------------
    hourly_data = data.get("hourly")

    if not hourly_data:
        raise ValueError("No hourly data returned from API")

    df = pd.DataFrame(hourly_data)

    # -----------------------------
    # Output path (Airflow volume)
    # -----------------------------
    output_dir = Path("/opt/airflow/data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"weather_raw_{timestamp}.csv"

    # -----------------------------
    # Save CSV
    # -----------------------------
    df.to_csv(output_file, index=False)

    # -----------------------------
    # Logging (visible in Airflow UI)
    # -----------------------------
    print("Weather data extraction completed successfully")
    print(f"Rows extracted: {len(df)}")
    print(f"File saved to: {output_file}")
