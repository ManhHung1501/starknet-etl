import pandas as pd
import json
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
from starknetetl.fetch_data import fetch_lastest_block
from dotenv import load_dotenv
import os

load_dotenv()


rpc_url = os.getenv('RPC_URL')
print(rpc_url)
print(fetch_lastest_block(rpc_url))