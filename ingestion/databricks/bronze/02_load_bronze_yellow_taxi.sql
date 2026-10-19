CREATE TABLE IF NOT EXISTS nyc_taxi_weather_project.bronze.taxi_yellow;
ALTER TABLE nyc_taxi_weather_project.bronze.taxi_yellow SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');

COPY INTO nyc_taxi_weather_project.bronze.taxi_yellow
FROM (
    SELECT *,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
    FROM <BUCKET>)
FILEFORMAT = PARQUET
PATTERN = '*/*/*.parquet'
COPY_OPTIONS ('mergeSchema' = 'true')