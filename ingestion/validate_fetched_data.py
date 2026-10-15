import json
from pathlib import Path

import pyarrow.parquet as pq


def validate_json_weather_file(file_path:Path,required_variables:tuple[str,...],start_date:str,end_date:str) -> int:
    if not file_path.is_file():
        raise FileNotFoundError(f"Wheather file was not found: {file_path}")
    if file_path.stat().st_size == 0:
        raise ValueError(f"File is empty: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if payload.get("error"):
        raise ValueError(f"API error: {payload.get('reason')}")
    hourly = payload.get("hourly")
    if not isinstance(hourly,dict):
        raise ValueError(" Weather file has no 'hourly' block ")  # noqa: TRY004
    required_vars = {"time",*required_variables}
    missing = []
    for vars in required_variables:
        if vars not in hourly:
            missing.append(vars)
    if missing:
        raise ValueError(f"Missing variables in 'hourly' block: {missing}")
    lengths = {}
    for name in required_vars:
        lengths[name] = len(hourly[name])
    if len(set(lengths.values())) != 1:
        raise ValueError("Different number of records for columns")
    num_records = lengths["time"]
    if num_records == 0:
        raise ValueError("Weather file has no records")
    first_date = hourly["time"][0][:10]
    last_date  = hourly["time"][-1][:10]

    if first_date != start_date or last_date != end_date:
        raise ValueError("Wrong period of time")
    return num_records

def validate_taxi_file(file_path:Path,required_columns:set[str]) -> int:
    if not file_path.is_file():
        raise FileNotFoundError(f"File was not found: {file_path}")
    if file_path.stat().st_size() == 0:
        raise ValueError(f"The file is empty: {file_path}")
    parquet_file = pq.ParquetFile(file_path)
    num_rows = parquet_file.metadata.num_rows
    if num_rows == 0:
        raise ValueError(f"Taxi file has no rows: {file_path}")
    columns = set(parquet_file.schema_arrow.names)

    missing = required_columns - columns
    if missing:
        raise ValueError(f"There are missing required columns is file: {missing}")
    return num_rows
    
