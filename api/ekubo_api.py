from fastapi import FastAPI, Query, HTTPException
from typing import List, Optional
from datetime import date, datetime

from dotenv import load_dotenv
from clickhouse_driver import Client
import os
load_dotenv()

host = os.getenv('CLICKHOUSE_HOST')
port = os.getenv('CLICKHOUSE_PORT')
user = os.getenv('CLICKHOUSE_USER')
password = os.getenv('CLICKHOUSE_PASSWORD')
clickhouse_db = os.getenv('CLICKHOUSE_DB')

client = Client(
        host,
        user=user,
        password=password,
        settings={"use_numpy": True},
    )

app = FastAPI()

@app.get("/top_n_txn_token")
def get_data(
    from_date: Optional[date] = Query(None), 
    to_date: Optional[date] = Query(None)
):
    # Default to today's date if no date is provided
    if from_date is None:
        from_date = date.today()
    if to_date is None:
        to_date = date.today()
    
    try:
        query = f"""
        SELECT pair, vol_24h, txn_24h
        FROM {clickhouse_db}.top_txn_token_report
        WHERE report_date BETWEEN '{from_date}' AND '{to_date}'
        ORDER BY vol_24h DESC
        """
        result = client.execute(query)

        columns = ["pair", "vol_24h", "txn_24h"]
        formatted_result = [dict(zip(columns, row)) for row in result]
        
        if not formatted_result:
            return {"message": "No data found for the given date range"}
        
        return {"top_tokens": formatted_result}
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))