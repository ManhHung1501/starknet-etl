import requests

def get_coingeko_id():
    url = "https://api.coingecko.com/api/v3/coins/list"
    params = {
        "include_platform": "true"
    }
    result = []
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        for coin in data:
            if 'starknet' in coin.get('platforms', {}):
                result.append({
                    'id': coin['id'],
                    'token': coin['platforms']['starknet']
                })
        return result
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
    

def get_token_price(token_ids:list):
    token_ids = ','.join(token_ids)
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": token_ids,
        "vs_currencies": "usd"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
    
def get_price(data, token_id):
    if token_id in data:
        return float(data[token_id]['usd'])
    return None