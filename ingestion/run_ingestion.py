import argparse
from calendar import monthrange
from datetime import date
from pathlib import Path

from ingestion import config
from ingestion.ingest_hourly_weather_s3 import (
    build_local_path,
    build_weather_param,
    build_weather_s3_key,
    fetch_to_local_weather,
    upload_weather_json_to_s3,
)
from ingestion.ingest_yellow_NYC_to_s3 import (
    build_dest_url,
    build_taxi_s3_key,
    build_taxi_url,
    download_taxi_month,
    upload_to_s3,
)
from ingestion.validate_fetched_data import (
    validate_json_weather_file,
    validate_taxi_file,
)


def main(
    year: int,
    month: int,
    upload_s3: bool = False,
    bucket: str | None = None,
    keep_local: bool = False,
) -> None:
    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12")
    if upload_s3 and not bucket:
        raise ValueError("Bucket is required when upload_s3=True")

    last_day = monthrange(year, month)[1]
    start_date = date(year, month, 1).isoformat()
    end_date = date(year, month, last_day).isoformat()
    print(f"Starting ingestion: {year}-{month:02d} ({start_date} -> {end_date})")

    taxi_url = build_taxi_url(url=config.TAXI_URL_TEMPLATE, year=year, month=month)
    taxi_path = build_dest_url(
        path=config.YELLOW_TAXI_LOCAL_PATH_TEMPLATE, year=year, month=month
    )
    print(f"Downloading taxi: {taxi_url}")
    download_taxi_month(url=taxi_url, dest_path=taxi_path)
    rows = validate_taxi_file(Path(taxi_path), config.REQUIRED_TAXI_COLUMNS)
    print(f"Taxi OK: {rows} rows -> {taxi_path}")

    weather_params = build_weather_param(
        start_date=start_date,
        end_date=end_date,
        latitude=config.WEATHER_LATITUDE,
        longitude=config.WEATHER_LONGITUDE,
        timezone=config.WEATHER_TIMEZONE,
        hourly_variables=config.WEATHER_HOURLY_VARIABLES,
    )
    weather_path = build_local_path(
        local_path=config.WEATHER_LOCAL_PATH,
        start_date=start_date,
        end_date=end_date,
    )
    print("Downloading weather")
    fetch_to_local_weather(
        url=config.WEATHER_API_URL, params=weather_params, dest_path=weather_path
    )
    records = validate_json_weather_file(
        weather_path, config.WEATHER_HOURLY_VARIABLES, start_date, end_date
    )
    print(f"Weather OK: {records} hourly records -> {weather_path}")

    if upload_s3:
        taxi_key = build_taxi_s3_key(
            path=config.YELLOW_TAXI_S3_FILE_PATH_TEMPLATE, year=year, month=month
        )
        weather_key = build_weather_s3_key(
            path=config.WEATHER_S3_FILE_PATH_TEMPLATE,
            start_date=start_date,
            end_date=end_date,
        )
        print(f"Taxi uploaded: {upload_to_s3(bucket=bucket, filename=taxi_path, s3_key=taxi_key)}")
        print(
            "Weather uploaded: "
            f"{upload_weather_json_to_s3(bucket=bucket, filename=weather_path, s3_key=weather_key)}"
        )
        if not keep_local:
            Path(taxi_path).unlink(missing_ok=True)
            Path(weather_path).unlink(missing_ok=True)
    print("Ingestion completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download NYC taxi + weather for a month")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--upload-s3", action="store_true")
    parser.add_argument("--keep-local", action="store_true")
    args = parser.parse_args()
    main(
        year=args.year,
        month=args.month,
        upload_s3=args.upload_s3,
        bucket=config.S3_BUCKET,
        keep_local=args.keep_local,
    )