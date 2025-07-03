# src/extract.py
import os
import requests
import json
from datetime import datetime
import logging  

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__) 

# TODO: Refatorar o metodo pra deixar ele mais clean
# TODO: Teste unitario

def extract_breweries():
    url_base = "https://api.openbrewerydb.org/v1/breweries"
    per_page = 200 # Limite da API
    page = 1
    all_data = []

    logger.info(f"[Extract] - Starting data extraction from api.openbrewerydb...")

    try:
        while True:
            response = requests.get(url_base, params={"per_page": per_page, "page": page})
            if response.status_code != 200:
                logger.error(f"[Extract] - API request failed at page {page} with status code:{response.status_code} ")
                raise Exception(f"Error - API request failed with status code:  {response.status_code}")

            data = response.json()
            if not data:
                logger.info(f"[Extract] - No more data, fineshed at page:  {page - 1}.")
                break

            all_data.extend(data)
            
            logger.info(f"[Extract] - Fineshed extracting {len(data)} records from page:  {page}.")
            page += 1
    except Exception as e:
        logger.exception(f"[Extract] - Failed to get an response from API with the following exceptio: {e}.")
    
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    os.makedirs("data/bronze", exist_ok=True)
    file_path = f"data/bronze/breweries_raw_{timestamp}.json"

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, indent=2)
            logger.info(f"[Extract] - Successfully extracted {len(all_data)} records saved on path: {file_path}.")
    except Exception as e:
        logger.exception(f"[Extract] - Failed to save extracted data on : {file_path}.")
    
if __name__ == '__main__':
    extract_breweries()