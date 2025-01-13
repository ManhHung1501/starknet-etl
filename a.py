import requests
import json

def get_btc_price():
    url = "https://api.coingecko.com/api/v3/coins/list"
    params = {
        "include_platform": "true"
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        with open('coins.json', 'w') as f:
            f.write(json.dumps(data, indent=2))
        return data
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# Fetch and print the Bitcoin price
try:
    btc_price = get_btc_price()
    # print(f"Current Bitcoin price in USD: ${btc_price}")
except Exception as e:
    print(e)
