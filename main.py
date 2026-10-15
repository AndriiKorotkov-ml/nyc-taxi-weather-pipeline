from calendar import monthrange
from datetime import date

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


def main(
    year: int,
    month: int,
    upload_s3: bool = False,
    bucket: str | None = None,
) -> None:
    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12")

    if upload_s3 and not bucket:
        raise ValueError("Bucket is required when upload_s3=True")

    last_day = monthrange(year, month)[1]
    start_date = date(year, month, 1).isoformat()
    end_date = date(year, month, last_day).isoformat()

    print(f"Starting ingestion: {year}-{month:02d}")
    print(f"Weather period: {start_date} → {end_date}")

    taxi_url = build_taxi_url(
        url=config.TAXI_URL_TEMPLATE,
        year=year,
        month=month,
    )
    taxi_path = build_dest_url(
        path=config.YELLOW_TAXI_LOCAL_PATH_TEMPLATE,
        year=year,
        month=month,
    )

    print(f"Downloading taxi: {taxi_url}")
    download_taxi_month(url=taxi_url, dest_path=taxi_path)
    print(f"Taxi saved locally: {taxi_path}")

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
        url=config.WEATHER_API_URL,
        params=weather_params,
        dest_path=weather_path,
    )
    print(f"Weather saved locally: {weather_path}")

    if upload_s3:
        taxi_key = build_taxi_s3_key(
            path=config.YELLOW_TAXI_S3_FILE_PATH_TEMPLATE,
            year=year,
            month=month,
        )
        weather_key = build_weather_s3_key(
            path=config.WEATHER_S3_FILE_PATH_TEMPLATE,
            start_date=start_date,
            end_date=end_date,
        )

        taxi_uri = upload_to_s3(
            bucket=bucket,
            filename=taxi_path,
            s3_key=taxi_key,
        )
        weather_uri = upload_weather_json_to_s3(
            bucket=bucket,
            filename=weather_path,
            s3_key=weather_key,
        )

        print(f"Taxi uploaded: {taxi_uri}")
        print(f"Weather uploaded: {weather_uri}")

    print("Ingestion completed")


if __name__ == "__main__":
    main(
        year=2026,
        month=3,
        upload_s3=True,
        bucket=config.S3_BUCKET,
    )