import logging, time, random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

def crawl_token_detail(driver, token:str, retries: int=3 )->dict:
    for attempt in range(retries):
        try:
            driver.get(f'https://starkscan.co/token/{token}')
            token_detail = {
                'token': token,
                'name': 'No Name',
                'symbol': 'No Symbol',
                'decimals': 0,
            }
            time.sleep(random.uniform(1, 2))

            title_elements = driver.find_elements(By.CSS_SELECTOR, 'dl div dt')
            for element in title_elements:
                if element.text.strip().lower() == 'name':
                    dd_element = element.find_element(By.XPATH, "following-sibling::dd")
                    token_detail['name'] = dd_element.text.strip()
                elif element.text.strip().lower() == 'decimals':
                    dd_element = element.find_element(By.XPATH, "following-sibling::dd")
                    token_detail['decimals'] = int(dd_element.text.strip())
                elif element.text.strip().lower() == 'symbol':
                    dd_element = element.find_element(By.XPATH, "following-sibling::dd")
                    token_detail['symbol'] = dd_element.text.strip()
                    
            if token_detail['name'] == 'No Name':
                print(f'Failed to get name for {token}')
            if token_detail['symbol'] == 'No Symbol':
                print(f'Failed to get symbol for {token}')
            if token_detail['decimals'] == 0:
                print(f'Failed to get decimals for {token}')
            return token_detail
        except Exception:
            print(f'Get token {token} error attempt {attempt+1}/{retries}')
    