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

bronze_path = "data/bronze/breweries/"
silver_path = "data/silver/"

'''
- Brings the latest file from folder path. 
'''
def get_latest_file(folder_path, pattern="*"):
    full_pattern = os.path.join(folder_path, pattern)
    files = list(filter(os.path.isfile, glob.glob(full_pattern)))
    if not files:
        raise Exception(f"NotFoundFile") 
    files.sort(key=os.path.getmtime)
    latest_file = files[-1]
    return latest_file

'''
- Cleans and standardizes column names in a DataFrame.

Parameters:
-----------
df : pd.DataFrame
 Input DataFrame with raw column names.

Returns:
--------
pd.DataFrame
 DataFrame with cleaned column names.
'''
def standard_columns_names(df: pd.DataFrame) -> pd.DataFrame:
    def remove_nfkd(txt):
        nfkd = unicodedata.normalize('NFKD', txt)
        return ''.join([c for c in nfkd if not unicodedata.combining(c)])
    new_columns_names = []
    for col in df.columns:
        col = col.strip()
        col = remove_nfkd(col)
        col = col.lower()
        col = re.sub(r'\s+', '', col)
        col = re.sub(r'[^\w]', '', col)
        new_columns_names.append(col)
    df.columns = new_columns_names
    return df

'''
- Cleans and processes the breweries DataFrame.

Parameters:
-----------
raw_df : pd.DataFrame
 Input raw DataFrame.

Returns:
--------
pd.DataFrame
 Cleaned and processed DataFrame.
'''
def clean_breweries_df(raw_df:pd.DataFrame) -> pd.DataFrame:
    treated_df = raw_df.copy()
    treated_df = treated_df.drop_duplicates(subset=["id"])
    # treated_df = treated_df.dropna(subset=["name", "state"])
    treated_df['phone'] = treated_df['phone'].str.replace(r"\D", "", regex=True)
    treated_df['website_url'] = treated_df['website_url'].str.replace("@gmail", "")
    treated_df[treated_df.columns] = treated_df.apply(lambda x: x.str.strip() if x.dtype == object else x)
    
    treated_df = treated_df.astype({
        'id': 'string',
        'name': 'string',
        'brewery_type': 'string',
        'address_1': 'string',
        'address_2': 'string',
        'address_3': 'string',
        'city': 'string',
        'state_province': 'string',
        'postal_code': 'string',
        'country': 'string',
        'longitude': 'float64',
        'latitude': 'float64',
        'phone': 'string',
        'website_url': 'string',
        'state': 'string',
        'street': 'string',
        'created_at': 'datetime64[ns]'
    })
    treated_df = standard_columns_names(treated_df)
    treated_df = treated_df.replace({pd.NA,"<NA>", np.nan }, None)  # padronizar a falta de dados
    return treated_df

'''
- Transforms raw brewery data from the Bronze layer to the Silver layer.
'''
def transform_breweries_bronze_to_silver():
    # bronze_path = "data/bronze/breweries"
    # silver_path = "data/silver/"
    os.makedirs(silver_path, exist_ok=True)

    logger.info(f"[Transform] - Starting data transformation from {bronze_path} files...")

    try:
        latest_file = get_latest_file(bronze_path, "breweries_raw_*.json")
        logger.info(f"[Transform] - Reading data from: {latest_file}")
    except Exception as e:
        logger.exception(f"[Transform] - No data found in data/bronze.")
        raise e
        
    raw_breweries_df = pd.read_json(latest_file)
    raw_breweries_df['created_at'] = datetime.now().strftime("%Y%m%dT%H%M%S")
    
    breweries_df = clean_breweries_df(raw_breweries_df)
    logging.info(f"[Transform] - Cleaned data: {len(raw_breweries_df)} → {len(breweries_df)}.")
    
    try:
        shutil.rmtree(silver_path)
        breweries_df.to_parquet(
            silver_path,
            partition_cols=['country', 'state'],
            engine = 'pyarrow',
            index = False
        )
        logger.info(f"[Transform] - Parquet partition by country and state saved in {silver_path}.")
    except Exception as e:
        logger.exception(f"[Transform] - Exception raised when saving .parquet files in {silver_path}.")
        raise e
    
    
if __name__ == '__main__':
    transform_breweries_bronze_to_silver()