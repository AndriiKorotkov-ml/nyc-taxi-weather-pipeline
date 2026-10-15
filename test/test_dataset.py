import pyarrow.parquet as pq
import pandas as pd
import datetime

df = pq.ParquetFile("data/yellow_tripdata_2026-03.parquet")
#print(df.schema_arrow)
#print("rows",df.metadata.num_rows)

df = pd.read_parquet("data/yellow_tripdata_2026-03.parquet")
mask = df.filter((df["tpep_pickup_datetime"] >= "2026-03-01 00:00:00") & (df["tpep_pickup_datetime"] <= "2026-03-31 23:59:59"))
mask.head(10)