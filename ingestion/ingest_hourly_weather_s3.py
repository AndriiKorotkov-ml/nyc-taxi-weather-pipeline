from datetime import date
from pathlib import Path

import boto3
import requests


def build_weather_param(start_date: str,end_date: str,latitude: float,longitude: float,timezone: str,hourly_variables: tuple)->dict:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
         raise ValueError("Start date must not be later than end date")
    return {    "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": ",".join(hourly_variables),
                "timezone": timezone}

def build_local_path(local_path:Path,start_date:str,end_date:str) -> Path:
    return (
        local_path
        / f"start_date={start_date}"
        / f"end_date={end_date}"
        / "response.json"
    )

def build_weather_s3_key(path:str,start_date:str,end_date:str)-> str:
     return path.format(start_date=start_date,end_date=end_date)
     
def fetch_to_local_weather(url:str,params:dict,dest_path:Path) -> None:
        with requests.get(url=url,params=params,timeout=30) as response:
            response.raise_for_status()
            payload = response.json()
            if payload.get("error"):
                 raise ValueError(f"Error fetching data : {payload.get('reason')}")
            if not payload.get("hourly",{}).get("time"):
                 raise ValueError("Weather API did not return any weather metrics")
            dest_path.parent.mkdir(parents=True,exist_ok=True)
            with open(dest_path,"wb") as f:
                 f.write(response.content)

def upload_weather_json_to_s3(bucket:str,filename:Path,s3_key:str) -> str:
     s3_client = boto3.client("s3")
     s3_client.upload_file(Filename=filename,Bucket=bucket,Key=s3_key)
     return f"s3://{bucket}/{s3_key}"