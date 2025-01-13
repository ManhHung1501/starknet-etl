import requests, time, logging

def fetch_response(url: str, payload, max_retries: int = 5):
    headers = {"Content-Type": "application/json"}
    for attempt in range(max_retries):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200 and 'error' not in response.json():
                return response.json()
            else:
                logging.error(f"Request Error in Attempt {attempt + 1}/{max_retries}: {response.text}, Retrying ...")
                time.sleep(5)
        except Exception as e:
            logging.error(f"An error occur in Attempt {attempt + 1}/{max_retries}: {e}")
            time.sleep(5)
    return None