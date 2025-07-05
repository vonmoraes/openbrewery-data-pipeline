import os
import requests
import json
from datetime import datetime
import logging  
import time
import math

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__) 

bronze_path = "data/bronze/breweries/"

def get_breweries_total_metadata():
    '''
    Fetches the total number of breweries available in the API metadata.

    Returns:
        total_breweries (int): Total number of breweries available from openbrewerydb.
    '''
    response = requests.get("https://api.openbrewerydb.org/v1/breweries/meta")
    total_breweries = response.json()["total"]
    return total_breweries 

def request_from_api(url_base,per_page,total_pages,retry):
    '''
    Request from api using paginated url_base
    
    Args:
        url_base (str) : URL for API request.
        url_base (int) : Number of results returning each request.
        total_pages (int) : Maximum number of pages. 
        retry (int) : Maximum number of retries in case of a failed request.
    Returns:
        all_data (json) : Json containg all request response. 
    '''
    all_data = []
    for page in range(1, total_pages + 1):
        for attempt in range(1, retry + 1):
            try:
                response = requests.get(url_base, params={"per_page": per_page, "page": page})
                if response.status_code != 200:
                    logger.error(f"[Extract] - API request failed at page {page} with status code:{response.status_code}")
                    break
            except Exception as e:
                logger.error(f"[Extract] - Failed to get an response from API in {attempt} / {retry}")
                if attempt == retry + 1:
                    logger.exception(f"[Extract] - Failed to get an response from API maximum attempts reached. With the following exception : {e}")
                    raise e
        data = response.json()
        if not data:
            logger.info(f"[Extract] - No more data, fineshed at page:  {page}.")
            break
        all_data.extend(data)
        logger.info(f"[Extract] - Fineshed extracting {len(data)} records from page:  {page}/{total_pages}.")
        time.sleep(0.1)
    return all_data

def extract_breweries():
    '''
    Extract brewery data from the API and save it to a JSON file.
    '''
    
    url_base = "https://api.openbrewerydb.org/v1/breweries"
    per_page = 200 # Limite da API
    total_breweries = get_breweries_total_metadata()
    total_pages = math.ceil(total_breweries / per_page)

    logger.info(f"[Extract] - Starting data extraction from api.openbrewerydb...")
    logger.info(f"[Extract] - Starting data extraction of {total_pages} pages")
    all_data = request_from_api(url_base, per_page, total_pages, retry=3)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    os.makedirs(bronze_path, exist_ok=True)
    file_path = f"{bronze_path}breweries_raw_{timestamp}.json"

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2)
            logger.info(f"[Extract] - Successfully extracted {len(all_data)} records saved on path: {file_path}.")
    except Exception as e:
        logger.exception(f"[Extract] - Failed to save extracted data on : {file_path}.")
    
if __name__ == '__main__':
    extract_breweries()