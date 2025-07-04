import os
import glob
import logging
import pandas as pd
import pyarrow.dataset as ds

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


''''
- Loads all parquet files
Parameters:
-----------
parquet_files
 Input DataFrame with raw column names.

Returns:
--------
pd.DataFrame
 DataFrame with cleaned column names.
    
'''
def load_parquet_files_to_dataframe(folder_path):
    try:
        logger.info("[Load] - Reading dataset with partition info using pyarrow.dataset...")
        dataset = ds.dataset(folder_path, format="parquet", partitioning="hive")
        df = dataset.to_table().to_pandas()
        return df
    except Exception as e:
        logger.exception(f"[Load] - Exception reading parquet dataset with partition columns: {e}")
        return pd.DataFrame()

def breweries_by_state_and_brewery_type_aggregated_silver_to_gold():
    os.makedirs(gold_path, exist_ok=True)
    
    logger.info(f"[Load] - Starting loading data aggregated by 'brewery coutry and brewery type' from {silver_path} to {gold_path} ...")
    parquet_files = get_all_parquet_files(silver_path)
    logger.info(f"[Load] - Found {len(parquet_files)} parquet files.")
    raw_breweries_df = load_parquet_files_to_dataframe(parquet_files)
    logger.info(f"[Load] - Grouped all {len(raw_breweries_df)} data by coutry.")
    
    breweries_grouped_df = (
        raw_breweries_df
        .groupby(["country", "brewery_type"])
        .size()
        .reset_index(name="brewery_count")
    )
    
    breweries_grouped_df = breweries_grouped_df.sort_values(by=["country", "brewery_count"], ascending=[True, False])
    try:
        breweries_grouped_df.to_parquet(
            f"{gold_path}breweries_grouped_by_country.parquet",
            index = False
        )
        logger.info(f"[Load] - Parquet partition by country and state saved in {gold_path}.")
    except Exception as e:
        logger.exception(f"[Load] - Exception raised when saving .parquet files in {gold_path}.")
        raise e

if __name__ == '__main__':
    breweries_by_state_and_brewery_type_aggregated_silver_to_gold()