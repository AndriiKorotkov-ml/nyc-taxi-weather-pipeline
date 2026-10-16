import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()
TAXI_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)

WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"

WEATHER_LATITUDE = 40.7128
WEATHER_LONGITUDE = -74.0060
WEATHER_TIMEZONE = "America/New_York"

WEATHER_HOURLY_VARIABLES = (
    "temperature_2m",
    "precipitation",
    "weather_code",
)

YELLOW_TAXI_LOCAL_PATH_TEMPLATE =Path("data/raw/taxi/yellow") / "yellow_tripdata_{year}-{month:02d}.parquet"
YELLOW_TAXI_S3_FILE_PATH_TEMPLATE = ("bronze/taxi/yellow/year={year}/month={month:02d}/yellow_tripdata_{year}-{month:02d}.parquet")
WEATHER_LOCAL_PATH = Path("data/raw/weather/nyc")
WEATHER_S3_FILE_PATH_TEMPLATE = (
    "bronze/weather/location=nyc/"
    "start_date={start_date}/end_date={end_date}/"
    "response.json")

S3_BUCKET=os.getenv("S3_BUCKET")

REQUIRED_TAXI_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "tolls_amount",
    "total_amount",
}