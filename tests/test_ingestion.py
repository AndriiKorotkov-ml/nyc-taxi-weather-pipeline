import json

import pytest

from ingestion.ingest_hourly_weather_s3 import build_weather_param
from ingestion.ingest_yellow_NYC_to_s3 import build_taxi_url
from ingestion.validate_fetched_data import validate_json_weather_file


def test_build_taxi_url_formats_year_and_month():
    url = "https://x/yellow_{year}-{month:02d}.parquet"
    result = build_taxi_url(url, year=2023, month=5)
    assert result == "https://x/yellow_2023-05.parquet"


def test_build_weather_params():

    result = build_weather_param(
        latitude=40.7128,
        longitude=-74.0060,
        start_date="2023-01-01",
        end_date="2023-01-31",
        timezone="America/New_York",
        hourly_variables=("temperature_2m", "precipitation", "weather_code"),
    )
    assert result == {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "start_date": "2023-01-01",
        "end_date": "2023-01-31",
        "timezone": "America/New_York",
        "hourly": "temperature_2m,precipitation,weather_code",
    }


def test_build_weather_params_with_different_variables():
    with pytest.raises(ValueError, match="Start date must not be later than end date"):
        build_weather_param(
            latitude=40.7128,
            longitude=-74.0060,
            start_date="2023-12-12",
            end_date="2023-01-31",
            timezone="America/New_York",
            hourly_variables=("temperature_2m", "windspeed_10m"),
        )


def test_valid_weather_file_returns_record_count(tmp_path):

    payload = {
        "hourly": {
            "time": ["2026-03-01T00:00", "2026-03-31T23:00"],
            "temperature_2m": [5.1, 7.3],
        }
    }
    file = tmp_path / "response.json"
    file.write_text(json.dumps(payload))

    result = validate_json_weather_file(
        file_path=file,
        required_variables=("temperature_2m",),
        start_date="2026-03-01",
        end_date="2026-03-31",
    )
    assert result == 2


def test_weather_file_without_hourly_raises(tmp_path):
    payload = {
        "latitude": 40.738136,
        "longitude": -74.04254,
        "generationtime_ms": 0.32639503479003906,
        "utc_offset_seconds": -14400,
        "timezone": "America/New_York",
        "timezone_abbreviation": "GMT-4",
        "elevation": 32.0,
    }
    file = tmp_path / "response.json"
    file.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="Weather file has no 'hourly' block"):
        validate_json_weather_file(
            file_path=file,
            required_variables=("temperature_2m",),
            start_date="2026-03-01",
            end_date="2026-03-31",
        )


def test_weather_file_not_found_raises(tmp_path):
    file = tmp_path / "missing.json"
    with pytest.raises(FileNotFoundError, match=f"Weather file was not found: {file}"):
        validate_json_weather_file(
            file_path=file,
            required_variables=("temperature_2m",),
            start_date="2026-03-01",
            end_date="2026-03-31",
        )


def test_weather_file_with_missing_variable_raises(tmp_path):
    payload = {
        "hourly": {
            "temperature_2m": [5.1, 7.3],
        }
    }
    file = tmp_path / "response.json"
    file.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="time"):
        validate_json_weather_file(
            file_path=file,
            required_variables=("temperature_2m",),
            start_date="2026-03-01",
            end_date="2026-03-31",
        )
