import os
import requests
import psycopg2
from datetime import datetime

API_URL = "https://api.open-meteo.com/v1/forecast"

def extract_and_load_weather():
    # Fetch weather data from the API
    params = {
        "latitude": 27.7172,
        "longitude": 85.3240,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Asia/Kathmandu",
        "past_days": 7
    }
    
    r = requests.get(API_URL, params=params, timeout=30)
    r.raise_for_status()
    payload=r.json()
    
    days = payload['daily']['time']
    tmax = payload['daily']['temperature_2m_max']
    tmin = payload['daily']['temperature_2m_min']
    psum = payload['daily']['precipitation_sum']
    
    conn = psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST"),
        port=os.getenv("WAREHOUSE_PORT"),
        dbname=os.getenv("WAREHOUSE_DB"),
        user=os.getenv("WAREHOUSE_USER"),
        password=os.getenv("WAREHOUSE_PASSWORD")
    )
    
    conn.autocommit = True
    ingested_at = datetime.now()
    
    with conn.cursor() as cur:
        for i in range(len(days)):
            cur.execute(
                """
                INSERT INTO bronze.weather_raw
                (ingested_at, latitude, longitude, day, temp_max, temp_min, precipitation_sum)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    ingested_at,
                    payload.get("latitude"),
                    payload.get("longitude"),
                    days[i],
                    tmax[i],
                    tmin[i],
                    psum[i]
                ),
            )
    conn.close()