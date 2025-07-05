import os
import glob
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.transform import transform_breweries_bronze_to_silver, silver_path

def test_transform_creates_parquet():
    
    transform_breweries_bronze_to_silver()
    assert os.path.exists(silver_path)

    full_pattern = os.path.join(silver_path, "**", "*.parquet")
    parquet_files = glob.glob(full_pattern, recursive=True)

    assert len(parquet_files) > 0
