import logging
from datetime import datetime


from starknetetl.utils.send_request import fetch_response


def parse_event(events, block_timstamp_dict):
    def format_address(address: str) -> str:
        """
        Formats the address by ensuring it has a proper length (66 characters for Starknet addresses).
        """
        if not address:
            return ''
        if len(address) >= 66:
            return address
        address_parts = address.split('x')
        return f"{address_parts[0]}x{'0' * (66 - len(address))}{address_parts[1]}"
    
    start = datetime.now()
    logging.info(f'Formating event data ...')
    txs_data = []
    for event in events:
        token0 = format_address(event['data'][1])
        token1 = format_address(event['data'][2])
        amount0 = event['data'][12]
        amount1 = event['data'][14]
        tx_exist = next((tx for tx in txs_data if tx['transaction_hash'] == event['transaction_hash']), None)
        if tx_exist:
            tx_exist['swap_steps'].append({
                'token0': token0,
                'token1': token1,
                'amount0': amount0,
                'amount1': amount1
            })
        else:
            txs_data.append({
                'block_number': event['block_number'],
                'block_timestamp': block_timstamp_dict[event['block_number']],
                'transaction_hash': event['transaction_hash'],
                'swap_steps': [
                    {
                        "token0": token0, 
                        "token1": token1, 
                        "amount0": amount0, 
                        "amount1": amount1
                    } 
                ]
            })
    logging.info(f'Complete parsing event data in {datetime.now()- start}')
    return txs_data
    