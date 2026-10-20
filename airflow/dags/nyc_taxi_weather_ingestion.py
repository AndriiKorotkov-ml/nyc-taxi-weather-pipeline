import logging
import os
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)
from airflow.sdk import Param, dag, get_current_context, task

logger = logging.getLogger(__name__)

@dag(
    dag_id="nyc_taxi_weather_ingestion_dag",
    description="Ingest monthly NYC taxi and weather data into S3",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "year": Param(
            type="integer",
            minimum=2009,
            description="Year to ingest",
        ),
        "month": Param(
            type="integer",
            minimum=1,
            maximum=12,
            description="Month to ingest",
        ),
    },
    tags=["nyc", "taxi", "weather", "s3"],
)

def nyc_taxi_weather_ingestion_dag():
    @task
    def prepare_period() -> dict:
       params = get_current_context()["params"]
       year = params["year"]
       month = params["month"]
       run_date = get_current_context()["dag_run"].start_date

       if run_date is None:
              raise ValueError("DAG run has no queued_at")
       run_date = run_date.astimezone(timezone.utc)
       latest_month_index = run_date.year*12 + run_date.month - 1 - 2
       latest_year,latest_month_zero_based = divmod(latest_month_index,12)
       latest_month = latest_month_zero_based + 1
       if (year,month) > (latest_year,latest_month):
              raise ValueError(
               "A two-calendar-month lag is required. "
               f"Latest allowed data month: "
               f"{latest_year}-{latest_month:02d}. "
               f"Requested: {year}-{month:02d}."
           )
       last_day = monthrange(year,month)[1]
       period = {
              "year": year,
              "month": month,
              "start_date":date(year,month,1).isoformat(),
              "end_date":date(year,month,last_day).isoformat(),
       }
       logger.info("Ingestion period: %s; latest allowed month: %s-%02d",period,latest_year,latest_month)
       return period
    @task
    def ingest_taxi_to_s3(period:dict) -> dict:
        from ingestion import config
        from ingestion.ingest_yellow_NYC_to_s3 import (
            build_taxi_s3_key,
            build_taxi_url,
            download_taxi_month,
            upload_to_s3,
        )
        from ingestion.validate_fetched_data import validate_taxi_file

        year = period["year"]
        month = period["month"]
        bucket = config.S3_BUCKET

        url = build_taxi_url(
              url=config.TAXI_URL_TEMPLATE,
              year = year,
              month=month,
              )
        s3_key = build_taxi_s3_key(
              path=config.YELLOW_TAXI_S3_FILE_PATH_TEMPLATE,
              year=year,
              month=month,
              )
        config.DATA_DIR.mkdir(parents=True,exist_ok=True)
        with TemporaryDirectory(
              prefix=f"taxi-{year}-{month:02d}-",
              dir=str(config.DATA_DIR)
        ) as temp_dir:
            local_path = (Path(temp_dir)/f"yellow_tripdata_{year}-{month:02d}.parquet")
            logger.info("Downloading taxi data: %s",url)
            download_taxi_month(
                url=url,
                dest_path=str(local_path))
            row_number=validate_taxi_file(
                file_path=local_path,
                required_columns=config.REQUIRED_TAXI_COLUMNS,
            )
            logger.info("Taxi validation passed: %s rows",row_number)
            s3_uri = upload_to_s3(
                bucket=bucket,
                filename=str(local_path),
                s3_key=s3_key,
            )
            logger.info("Taxi uploaded %s",s3_uri)
        return {
          "s3_uri":s3_uri,
          "rows":row_number
        }
    @task
    def ingest_weather_to_s3(period:dict) -> dict:
        from ingestion import config
        from ingestion.ingest_hourly_weather_s3 import (
            build_local_path,
            build_weather_param,
            build_weather_s3_key,
            fetch_to_local_weather,
            upload_weather_json_to_s3,
        )
        from ingestion.validate_fetched_data import (
            validate_json_weather_file,
        )
        start_date = period["start_date"]
        end_date = period["end_date"]
        bucket = config.S3_BUCKET

        params = build_weather_param(
                start_date=start_date,
                end_date=end_date,
                latitude=config.WEATHER_LATITUDE,
                longitude=config.WEATHER_LONGITUDE,
                timezone=config.WEATHER_TIMEZONE,
                hourly_variables=config.WEATHER_HOURLY_VARIABLES,
        )
        s3_key = build_weather_s3_key(
            path=config.WEATHER_S3_FILE_PATH_TEMPLATE,
            start_date=start_date,
            end_date=end_date,
        )
        config.DATA_DIR.mkdir(parents=True,exist_ok=True)

        with TemporaryDirectory(
                prefix="weather-",
                dir=str(config.DATA_DIR)
        ) as temp_dir:
            local_path = build_local_path(
                    local_path=Path(temp_dir),
                    start_date=start_date,
                    end_date=end_date,
                    )
            logger.info("Donloading weather: %s to %s",start_date,end_date)

            fetch_to_local_weather(
                url=config.WEATHER_API_URL,
                params=params,
                dest_path=local_path,
                )
            records = validate_json_weather_file(
                file_path=local_path,
                required_variables=config.WEATHER_HOURLY_VARIABLES,
                start_date=start_date,
                end_date=end_date,
            )
            logger.info("Weather vaildation passed: %s hourly records",records)

            s3_uri = upload_weather_json_to_s3(
                bucket=bucket,
                filename=local_path,
                s3_key=s3_key,
            )
            logger.info("Weather uploaded: %s",s3_uri)
        return {
            "s3_uri":s3_uri,
            "records":records
        }
    period = prepare_period()

    taxi_result = ingest_taxi_to_s3(period)
    weather_result = ingest_weather_to_s3(period)

    run_databricks_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=int(os.environ["DATABRICKS_JOB_ID"]),
        job_parameters={
            "year": (
                "{{ ti.xcom_pull(task_ids='prepare_period')['year'] "
                "| string }}"
            ),
            "month": (
                "{{ '%02d' | format("
                "ti.xcom_pull(task_ids='prepare_period')['month']) }}"
            ),
        },
        wait_for_termination=True,
        polling_period_seconds=30,
        retries=0,
    )

    [taxi_result, weather_result] >> run_databricks_job

nyc_taxi_weather_ingestion_dag()
      


