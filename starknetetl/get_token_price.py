import requests, time

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
    

# def get_token_price(token_ids:list):
#     token_ids = ','.join(token_ids)
#     url = "https://api.coingecko.com/api/v3/simple/price"
#     params = {
#         "ids": token_ids,
#         "vs_currencies": "usd"
#     }

#     response = requests.get(url, params=params)
#     if response.status_code == 200:
#         data = response.json()
#         return data
#     else:
#         raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# def get_price(data, token_id):
#     if token_id in data:
#         return float(data[token_id]['usd'])
#     return None

def get_token_price(token_address: list):
    price_result = {}
    batch_size = 30
    for batch in range(0, len(token_address), batch_size):
        batch_token = token_address[batch:batch + batch_size]
        token_addresses = ','.join(batch_token)
        url = f"https://api.geckoterminal.com/api/v2/simple/networks/starknet-alpha/token_price/{token_addresses}"
        headers = {"accept": "application/json"}
        
        for attempt in range(3):
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                print(data)
                for token, price in data["data"]["attributes"]["token_prices"].items():
                    if price:
                        price_result[token] = float(price)
                    else:
                        price_result[token] = 0
                        print(f'Can not get price for {token}')
                break  # Exit retry loop on success
            else:
                print(f"Failed to fetch data {attempt + 1}/3: {response.status_code}, {response.text}")
                time.sleep(10)
                if attempt == 2:  # Log error after last attempt
                    print(f"Exhausted retries for batch: {batch_token}")
    return price_result
    
def get_price(data, token_address):
    if token_address in data:
        return data[token_address]
    return 0