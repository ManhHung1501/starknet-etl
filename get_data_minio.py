import boto3
import json
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd

# MinIO configuration
MINIO_ENDPOINT = "http://192.168.8.94:9000"
ACCESS_KEY = "myminioadmin"
SECRET_KEY = "Ft3ch@2022"
BUCKET_NAME = "starknet-onchain"
PREFIX = "export/events/contract=0x00000005dd3d2f4429af886cd1a3b08289dbcea99a294197e9eb43b0e0325b4b/"

# ClickHouse configuration
CLICKHOUSE_HOST = "192.168.8.96"
CLICKHOUSE_PORT = "9000"
CLICKHOUSE_DB = "starknet_onchain"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "Ft3ch@2022"
CLICKHOUSE_TABLE = "events"

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

def list_minio_objects():
    """List all JSON files in the specified MinIO bucket and prefix."""

    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    if "Contents" not in response:
        print("No objects found.")
        return []
    
    return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".json")]

def read_json_from_minio(key):
    """Read and parse a JSON file from MinIO."""

    response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    data = pd.DataFrame(data)
    data['keys'] = data["keys"].apply(lambda x: json.dumps(x))
    data['data'] = data["data"].apply(lambda x: json.dumps(x))
    return data

def insert_events_to_clickhouse(df):
    """Insert events into the ClickHouse table."""
    client = Client(
        CLICKHOUSE_HOST,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        settings={"use_numpy": True},
    )
    
    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    client.execute(generate_create_table_query(df,CLICKHOUSE_DB,CLICKHOUSE_TABLE))
    
    # Insert data into ClickHouse
    client.insert_dataframe(
        f"INSERT INTO {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} VALUES",
        df
    )

def generate_create_table_query(df, target_db: str, target_table: str, engine: str ="MergeTree", primary_column: str = None) -> str:
    # Map pandas dtypes to ClickHouse data types
    dtype_mapping = {
        'uint64': 'UInt64',
        'int64': 'UInt64',
        'int32': 'Int32',
        'float64': 'Float64',
        'object': 'String',
        'datetime64[ns]': 'DateTime',
        'bool': 'UInt8',
    }

    # Start building the query
    create_table_query = f"CREATE TABLE IF NOT EXISTS {target_db}.{target_table} (\n"

    # Add columns and their corresponding types
    columns = []
    for col, dtype in df.dtypes.items():
        if dtype == 'datetime64[ns]' and (df[col].dt.time == datetime.time(0, 0)).all():
            clickhouse_type = 'Date' 
        else:
            clickhouse_type = dtype_mapping.get(str(dtype), 'String')
        columns.append(f"    {col} {clickhouse_type}")

    # Combine the column definitions
    create_table_query += ",\n".join(columns)

    # Add the engine and primary column (ORDER BY) if provided
    create_table_query += f"\n) ENGINE = {engine}()"

    if primary_column:
        create_table_query += f"\nORDER BY ({primary_column});"
    else:
        create_table_query += f"\nORDER BY tuple();"

    return create_table_query

def main():
    """Main function to load data from MinIO to ClickHouse."""
    objects = list_minio_objects()
    all_events = []
    
    for key in objects:
        start = datetime.now()
        print(f"Processing file: {key}")
        data = read_json_from_minio(key)
        insert_events_to_clickhouse(data)
        print(f"Complete import {len(data)} rows to clickhouse in {datetime.now()-start}")


if __name__ == "__main__":
    main()
