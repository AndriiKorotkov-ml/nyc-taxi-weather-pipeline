from pathlib import Path

import boto3
import requests


def build_dest_url(path:Path,year:int,month:int)-> str:
    dest_path = Path(str(path).format(year=year, month=month))
    dest_path.parent.mkdir(parents=True,exist_ok=True)
    return str(dest_path)

def build_taxi_url(url:str,year:int,month:int)-> str:
    return url.format(year=year,month=month)

def build_taxi_s3_key(path:str,year:int,month:int)-> str:
    return path.format(year= year,month=month)

def download_taxi_month(url:str,dest_path: str)-> None:
    with requests.get(url,stream=True,timeout=60) as response:
        response.raise_for_status()
        with open(dest_path,"wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
def upload_to_s3(bucket:str,filename:str ,s3_key:str)-> str:
    s3_client = boto3.client("s3")
    s3_client.upload_file(Filename=(filename),Bucket=bucket,Key=s3_key)
    return f"s3://{bucket}/{s3_key}"