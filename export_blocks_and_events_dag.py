import os, logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from starknetetl.fetch_data import fetch_blocks_data,fetch_events_data,fetch_lastest_block
from starknetetl.clickhouse import load_df,init_connection



load_dotenv()

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

batch_size = 10000

def load_config(**context):
    latest_crawled_block = clickhouse_client.execute(f"SELECT MAX(block_number) FROM {clickhouse_db}.events")
    if latest_crawled_block and latest_crawled_block[0][0] is not None: 
        from_block = int(latest_crawled_block[0][0])
    else:
        from_block = 0
    to_block = fetch_lastest_block(rpc_url) - 1

    input_config = context['dag_run'].conf
    if 'from_block' in input_config:
        from_block = input_config['from_block']
    if 'to_block' in input_config:
        to_block = input_config['from_block']

    context['ti'].xcom_push(key="from_block", value=from_block)
    context['ti'].xcom_push(key="to_block", value=to_block)

    logging.info(f"From block: {from_block} - To block: {to_block}")

def etl_blocks(**context):
    from_block = context['ti'].xcom_pull(task_ids='load_config', value=from_block)
    to_block = context['ti'].xcom_pull(task_ids='load_config', value=to_block)

    for from_block in range(from_block, to_block, batch_size):
        to_block = min(from_block + batch_size - 1, to_block)
        data = fetch_blocks_data(rpc_url, from_block, to_block)
        df = pd.DataFrame(data)
        load_df(clickhouse_client, df, clickhouse_db, 'blocks', "ReplacingMergeTree", "block_number")

def etl_events(**context):
    from_block = context['ti'].xcom_pull(task_ids='load_config', value=from_block)
    to_block = context['ti'].xcom_pull(task_ids='load_config', value=to_block)

    for from_block in range(from_block, to_block, batch_size):
        to_block = min(from_block + batch_size - 1, to_block)
        data = fetch_events_data(rpc_url,contract_address, from_block, to_block)
        df = pd.DataFrame(data)
        load_df(clickhouse_client, df, clickhouse_db, 'events')
        
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
    dag_id='Export_Blocks_And_Events_Ekubo_DAG',
    default_args=default_args,
    tags=["Blockchain", "Ekubo", "Blocks", "Events"],
    schedule_interval="0 7 * * *",
    catchup=False,
) as dag:
    load_config_task = PythonOperator(task_id=f"load_config",
                                        python_callable=load_config,
                                        provide_context=True,
                                    )

    etl_events_task = PythonOperator(task_id=f"etl_events",
                                    python_callable=etl_events,
                                    provide_context=True,
                                )
    
    etl_blocks_task = PythonOperator(task_id=f"etl_blocks",
                                    python_callable=etl_blocks,
                                    provide_context=True,
                                )
    

    load_config_task >> etl_events_task >> etl_blocks_task
    