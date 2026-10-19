CREATE TABLE IF NOT EXISTS nyc_taxi_weather_project.bronze.weather_conditions;

COPY INTO nyc_taxi_weather_project.bronze.weather_conditions
FROM (
    SELECT *,
    _metadata.file_path as _source_file,
    current_timestamp() as _ingested_at
    FROM <BUCKET>
)
FILEFORMAT = JSON
PATTERN = "*/*/*/*.json"
FORMAT_OPTIONS  ('multiLine' = 'true')
COPY_OPTIONS('mergeSchema' = 'true');
