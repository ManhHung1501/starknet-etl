import os, logging, json
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from starknetetl.clickhouse import load_df,init_connection

from scraper.scrape_token import crawl_token_detail
from scraper.scrape_utils import setup_driver

load_dotenv()

# Chrome driver
chrome_path = os.getenv("CHROME_DRIVER_PATH")

# RPC 
rpc_url = os.getenv('RPC_URL')
contract_address = os.getenv('CONTRACT_ADDRESS')

# CLICKHOUSE
host = os.getenv('CLICKHOUSE_HOST')
port = os.getenv('CLICKHOUSE_PORT')
user = os.getenv('CLICKHOUSE_USER')
password = os.getenv('CLICKHOUSE_PASSWORD')
clickhouse_db = os.getenv('CLICKHOUSE_DB')
clickhouse_client = init_connection(host,user,password)

def crawl_token():
    df = clickhouse_client.query_dataframe("""
        WITH A aS (SELECT arrayElement(parsed_data, 2) AS token_0,
                        arrayElement(parsed_data, 3) AS token_1
                FROM (
                            SELECT JSONExtract(data, 'Array(String)') AS parsed_data
                            FROM starknet_onchain.events e
                            JOIN starknet_onchain.blocks b ON e.block_number = b.block_number
                            )
        ), B AS (
            SELECT  token_0 AS token FROM A
            UNION ALL
            SELECT  token_1 FROM A
        )
        SELECT DISTINCT token from B
        """)
    
    driver = setup_driver(chrome_path)
    tokens = []
    for i,token in df.iterrows():
        tokens.append(crawl_token_detail(driver, token=token['token']))
    df_token = pd.DataFrame(tokens)
    driver.quit()
    load_df(clickhouse_client, df_token, clickhouse_db, "token", "ReplacingMergeTree", "token")


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
    dag_id='Crawl_Starknet_Token_DAG',
    default_args=default_args,
    tags=["Blockchain", "Ekubo", "Tokens", "Crawl"],
    schedule_interval=None,
    catchup=False,
) as dag:
    crawl_token_task = PythonOperator(task_id=f"crawl_token",
                                    python_callable=crawl_token,
                                    provide_context=True,
                                )