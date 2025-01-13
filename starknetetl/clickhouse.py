import logging
from datetime import datetime
from clickhouse_driver import Client

def init_connection(host: str, user: str, password: str):
    return Client(
        host,
        user=user,
        password=password,
        settings={"use_numpy": True},
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

def load_df(clickhouse_client, df, target_db: str, target_table: str, engine: str ="MergeTree", primary_column: str = None):
    start = datetime.now()
    tbl_name = f"{target_db}.{target_table}"
    logging.info(f'Starting to load dataframe to {tbl_name} ...')
    try:
        # Create database and table if not exist
        clickhouse_client.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
        clickhouse_client.excute(generate_create_table_query(df, target_db, target_table, engine, primary_column))
        clickhouse_client.insert_dataframe(f"INSERT INTO {tbl_name} VALUES", df)
        if primary_column:
            clickhouse_client.execute(f"OPTIMIZE TABLE {tbl_name} FINAL")
        
        time_process = datetime.now() - start
        logging.info(f'Complete Load data to Clickhouse in {time_process:.2f} with {len(df)} rows')
    except Exception as e:
        logging.error(f'Load data frame get error: {e}')
