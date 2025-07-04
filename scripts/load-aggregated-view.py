# ler os parques da silver
# criar uma query nos mesmos e adiciona na gold 
import os
import glob
import logging
import pandas as pd
import numpy as np
from datetime import datetime
import unicodedata
import re
import shutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__) 

silver_path = "data/silver/"
gold_path = "data/gold/"

'''
- Brings all .parquet files from folder subfolders. 
'''
def get_all_parquet_files(folder_path, pattern="**"):
    full_pattern = os.path.join(folder_path, pattern, "*.parquet")
    parquet_files = glob.glob(full_pattern, recursive=True)
    if not parquet_files:
        logger.exception(f"[Load] - Exception: No Parquet files found in {folder_path}.")
        raise Exception(f"NotFoundParquetFiles")
    return parquet_files


def breweries_by_state_and_brewery_type_aggregated_silver_to_gold():
    
    logger.info(f"[Load] - Starting loading data aggregated by 'brewery state and brewery type' from {silver_path} to {gold_path} ...")
    parquet_files = get_all_parquet_files(silver_path)
    logger.info(f"[Load] - Found {len(parquet_files)} parquet files.")
    


if __name__ == '__main__':
    breweries_by_state_and_brewery_type_aggregated_silver_to_gold()