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

def get_all_parquet_files(folder_path, pattern="**"):
    '''
    Brings all .parquet files from folder subfolders.

    Args:
        folder_path (str): Path to the folder where Parquet files will be searched.
        pattern (str): Pattern to filter files in subfolders. Defaults to "**".

    Returns:
        parquet_files (list): List of paths to all Parquet files found.

    Raises:
        Exception: If no Parquet files are found in the specified folder.
    '''
    full_pattern = os.path.join(folder_path, pattern, "*.parquet")
    parquet_files = glob.glob(full_pattern, recursive=True)
    if not parquet_files:
        logger.exception(f"[Load] - Exception: No Parquet files found in {folder_path}.")
        raise Exception(f"NotFoundParquetFiles")
    return parquet_files

def load_parquet_files_to_dataframe(folder_path):
    '''
    Loads all Parquet files from a folder into a single DataFrame.

    Args:
        folder_path (str): Path to the folder containing Parquet files.

    Returns:
        df (pd.DataFrame): DataFrame containing data from all Parquet files.

    Raises:
        Exception: If there is an error while reading the Parquet dataset.
    '''
    try:
        logger.info("[Load] - Reading dataset with partition info.")
        dataset = ds.dataset(folder_path, format="parquet", partitioning="hive")
        df = dataset.to_table().to_pandas()
        return df
    except Exception as e:
        logger.exception(f"[Load] - Exception reading parquet dataset with partition columns: {e}")
        return pd.DataFrame()

def breweries_by_country_and_brewery_type_aggregated_silver_to_gold():
    '''
    Aggregates brewery data by country and brewery type, then saves it to the Gold layer.

    Raises:
        Exception: If there is an error during aggregation or saving the output file.
    '''
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
    breweries_by_country_and_brewery_type_aggregated_silver_to_gold()