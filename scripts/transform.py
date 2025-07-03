# srcipts/transform.py
import os
import requests
import json
import glob
from datetime import datetime
import logging  

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__) 

def get_latest_file(folder_path, pattern="*"):
    full_pattern = os.path.join(folder_path, pattern)
    files = list(filter(os.path.isfile, glob.glob(full_pattern)))
    if not files:
        return None
    files.sort(key=os.path.getmtime)
    latest_file = files[-1]
    return latest_file

def transform_breweries():
    bronze_path = "data/bronze"
    silver_path = "data/silver"
    os.makedirs(silver_path, exist_ok=True)
    latest_file = get_latest_file(bronze_path, "breweries_raw_*.json") 
    print(latest_file)
    
if __name__ == '__main__':
    transform_breweries()