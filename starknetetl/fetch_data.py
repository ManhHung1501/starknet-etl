from starknetetl.utils.send_request import fetch_response
import logging, time
from datetime import datetime

def fetch_lastest_block(rpc_url: str):
    payload = {
        "jsonrpc": "2.0",
        "method": "starknet_blockNumber",
        "params": [],
        "id": 1
    }
    response = fetch_response(url=rpc_url, payload=payload)
    if response:
        current_block = response['result']
        logging.info(f"Current block number: {current_block}")
        return current_block

def fetch_blocks_data(rpc_url: str,from_block: int, to_block: int) -> list:
    start = time.time()
    logging.info(f"Extracting block details of block {from_block} to {to_block}...")
    payload = [
        {
            "jsonrpc": "2.0",
            "method": "starknet_getBlockWithTxHashes",
            "params": [{"block_number": block_num}],
            "id": block_num
        }
        for block_num in range(from_block, to_block + 1)
    ]
    block_detail = []
    response = fetch_response(url=rpc_url, payload=payload)
    if response:
        for data in response:
            block_data = data['result']
            block_detail.append(
                {
                    'block_number': block_data['block_number'],
                    'block_hash': block_data['block_hash'],
                    'block_timestamp': block_data['timestamp']
                }
            )
        time_process = (time.time() - start)/60
        logging.info(f'Success extract details block {from_block} to {to_block} in {time_process:.2f} minutes')
    else:
        logging.error('Failed to extract block details')
            
    return block_detail

def fetch_events_data(
        rpc_url: str,
        contract_address: str, 
        from_block: str,
        to_block: str, 
        chunk_size: int=5000,
        event_key=None
    ):
    start = time.time()
    logging.info(f"Crawling events data from block {from_block} to block {to_block} ...")
    payload = {
        "jsonrpc": "2.0",
        "method": "starknet_getEvents",
        "id": 1
    }
    params = [
            {
                "from_block": {
                    "block_number": from_block,
                },
                "to_block": {
                    "block_number": to_block,
                },
                "address": contract_address,
                "chunk_size": chunk_size,
                "keys": [[
                    "0x157717768aca88da4ac4279765f09f4d0151823d573537fbbeb950cdbd9a870"
                ]],
            }
    ]
    if event_key:
        params[0]['keys'] = [[event_key]]
    payload['params'] = params

    events_data = []
    response = fetch_response(rpc_url, payload)
    
    if response:
        result = response['result']
        for event in result['events']:
            events_data.append(event)
    while response and 'continuation_token' in result:
        params[0]['continuation_token'] = result['continuation_token']
        payload['params'] = params
        response = fetch_response(rpc_url, payload)
        if response:
            result = response['result']
            for event in result['events']:
                events_data.append(event)

    time_process = time.time()- start
    logging.info(f'Complete Crawl from block {from_block} to {to_block} in {time_process:.2f} minutes with {len(events_data)} events')
    return events_data
