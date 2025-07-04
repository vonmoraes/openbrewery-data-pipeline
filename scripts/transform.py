import os
import glob
import logging
import pandas as pd
import numpy as np
import pyarrow as pa
from datetime import datetime
import unicodedata
import re
import shutil

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__) 

# parquet lidar com nome de colunas e padronizacao
def limpar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    def remover_acentos(txt):
        nfkd = unicodedata.normalize('NFKD', txt)
        return ''.join([c for c in nfkd if not unicodedata.combining(c)])

    colunas_limpa = []
    for col in df.columns:
        col = col.strip()  # remove espaços nas extremidades
        col = remover_acentos(col)  # remove acentos
        col = col.lower()  # minusculo
        col = re.sub(r'\s+', '', col)  # espaços (um ou mais) viram underline
        col = re.sub(r'[^\w]', '', col)  # remove caracteres especiais que não são letras, números ou 
        colunas_limpa.append(col)

    df.columns = colunas_limpa
    return df

def get_latest_file(folder_path, pattern="*"):
    full_pattern = os.path.join(folder_path, pattern)
    files = list(filter(os.path.isfile, glob.glob(full_pattern)))
    if not files:
        raise Exception(f"NotFoundFile") 
    files.sort(key=os.path.getmtime)
    latest_file = files[-1]
  
    return latest_file

def clean_breweries_df(raw_df:pd.DataFrame) -> pd.DataFrame:
    treated_df = raw_df.copy()
    treated_df = treated_df.drop_duplicates(subset=["id"])
    treated_df = treated_df.dropna(subset=["name", "state"])
    treated_df['phone'] = treated_df['phone'].str.replace(r"\D", "", regex=True)
    treated_df['website_url'] = treated_df['website_url'].str.replace("@gmail", "")
    treated_df['website_url'] = treated_df['website_url'].str.strip()
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
        'createdAt': 'datetime64[ns]'
    })
    treated_df = limpar_colunas(treated_df)
    treated_df = treated_df.replace({pd.NA,"<NA>", np.nan }, None)  # padronizar a falta de dados
    return treated_df

def transform_breweries():
    bronze_path = "data/bronze/breweries"
    silver_path = "data/silver/"
    os.makedirs(silver_path, exist_ok=True)

    try:
        latest_file = get_latest_file(bronze_path, "breweries_raw_*.json")
        logger.info(f"[Transform] - Reading data from: {latest_file}")
    except Exception as e:
        logger.exception(f"[Transform] - No data found in data/bronze.")
        raise e
        
    raw_breweries_df = pd.read_json(latest_file)
    raw_breweries_df['createdAt'] = datetime.now().strftime("%Y%m%dT%H%M%S")
    breweries_df = clean_breweries_df(raw_breweries_df)
    logging.info(f"[Transform] - Cleaned data: {len(raw_breweries_df)} → {len(breweries_df)}.")
    
    shutil.rmtree(silver_path)
    breweries_df.to_parquet(
        silver_path,
        partition_cols=['country'],
        engine = 'pyarrow',
        index = False
    )
    
    print(breweries_df.dtypes)
    print(breweries_df.head(10))
    
    
if __name__ == '__main__':
    transform_breweries()