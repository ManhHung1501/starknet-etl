import os, logging
from dotenv import load_dotenv
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator

from starknetetl.clickhouse import init_connection, load_df
from starknetetl.get_token_price import get_token_price, get_price

from scraper.scrape_token import crawl_token_detail
from scraper.scrape_utils import setup_driver

import numpy as np
import pandas as pd


load_dotenv()
# CHROME DRIVER PATH
chrome_path = os.getenv("CHROME_DRIVER_PATH")

# CLICKHOUSE
host = os.getenv('CLICKHOUSE_HOST')
port = os.getenv('CLICKHOUSE_PORT')
user = os.getenv('CLICKHOUSE_USER')
password = os.getenv('CLICKHOUSE_PASSWORD')
clickhouse_db = os.getenv('CLICKHOUSE_DB')
clickhouse_client = init_connection(host,user,password)
# MONGODB
mongo_url = os.getenv('MONGODB_URL')

def generate_top_token_24h(top_n: int = 30):
    logging.info(f'Starting to generate report ...')
    df = clickhouse_client.query_dataframe("""
        WITH events AS (
            SELECT
                event_date,
                arrayElement(parsed_data, 2) AS token_0,
                arrayElement(parsed_data, 3) AS token_1,
                arrayElement(parsed_data, 7) AS amount,
                arrayElement(parsed_data, 9) AS is_token1
            FROM
            (
                SELECT
                    toDateTime(block_timestamp) AS event_date,
                    JSONExtract(data, 'Array(String)') AS parsed_data
                FROM
                    starknet_onchain.events e
                JOIN starknet_onchain.blocks b ON e.block_number = b.block_number
            ) event
            WHERE event_date BETWEEN toDateTime(now() - INTERVAL 1 DAY) AND toDateTime(now())
            )
        SELECT
            token_0,
            token_1,
            is_token1,
            t0.symbol AS symbol0,
            t1.symbol AS symbol1,
            t0.decimals AS decimals0,
            t1.decimals AS decimals1,
            e.amount AS amount
        FROM events e
        LEFT JOIN starknet_onchain.token t0 ON e.token_0 = t0.token
        LEFT JOIN starknet_onchain.token t1 ON e.token_1 = t1.token
        ORDER BY e.event_date DESC;
    """)
    logging.info(f'Query data from clickhouse success!')
    df['is_token1'] = df['is_token1'].apply(lambda x: int(x, 16))
    df['decimals'] = np.where(df['is_token1']==1,df['decimals1'],df['decimals0'])
    df['token_address'] = np.where(df['is_token1']==1,df['token_1'],df['token_0'])
    new_tokens = []
    driver = setup_driver(chrome_path)
    for idx, row in df.iterrows():
        if pd.isna(row['decimals']):
            new_token_detail = crawl_token_detail(driver, row['token_address'])
            new_tokens.append(new_token_detail)
            df.loc[idx, 'decimals'] = new_token_detail['decimals']
            df.loc[idx, 'symbol'] = new_token_detail['symbol']
    driver.quit()
    df = df[df['decimals'] != 0]

    if new_tokens:
        df_new_token = pd.DataFrame(new_tokens)
        load_df(clickhouse_client,df_new_token,clickhouse_db,'token','ReplacingMergeTree','token')

    token_addresses = df['token_address'].unique()
    price_data = get_token_price(token_addresses)
    df['price'] = df['token_address'].apply(lambda x: get_price(price_data, x))

    df = df[(df['price'] != 0) & (df['price']!=None)]
    df['amount'] = df['amount'].apply(lambda x: int(x, 16))
    df['volumn'] = df.apply(lambda row: row['amount'] / (10 ** row['decimals']) * row['price'], axis=1)

    # Sort tokens to ensure (token_0, token_1) pairs are consistent regardless of order
    df['pair'] = df.apply(lambda row: '/'.join(sorted([row['symbol0'], row['symbol1']])), axis=1)

    # Group by token pairs and count occurrences
    df_summary = df.groupby('pair').agg(
        vol_24h=('volumn', 'sum'),
        txn_24h =('pair', 'size')
    ).reset_index()

    # Select the top 30 pairs based on count
    top_n_pairs = df_summary.sort_values(by='vol_24h', ascending=False).head(top_n)
    logging.info(f'Calculate data Success!')
    # Insert the data into ch
    report_table = 'top_txn_token_report'
    clickhouse_client.execute(f"TRUNCATE TABLE IF EXISTS {clickhouse_db}.{report_table}")
    load_df(clickhouse_client, top_n_pairs,clickhouse_db,report_table)
    
    logging.info(f'Insert data to success')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    "start_date": datetime(2025, 1, 12),
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define DAG
with DAG(
    dag_id='Generate_Ekubo_Swap_Report_DAG',
    default_args=default_args,
    tags=["Blockchain", "Ekubo", "Blocks", "Report"],
    schedule_interval=None,
    catchup=False,
) as dag:
    generate_top_token_24h_task = PythonOperator(task_id=f"generate_top_token_24h",
                                        python_callable=generate_top_token_24h,
                                        provide_context=True,
                                    )
    