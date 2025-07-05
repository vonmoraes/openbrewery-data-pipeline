import os
import pandas as pd
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.transform import transform_breweries_bronze_to_silver
from scripts.extract import extract_breweries
from scripts.load_aggregated_view import breweries_by_country_and_brewery_type_aggregated_silver_to_gold, gold_path

@pytest.fixture(autouse=True)
def prepare_data():
    extract_breweries()
    transform_breweries_bronze_to_silver()

def test_load_creates_aggregated_parquet():
    breweries_by_country_and_brewery_type_aggregated_silver_to_gold()
    output_file = os.path.join(gold_path, "breweries_grouped_by_country.parquet")
    assert os.path.exists(output_file), "Not created parquet file."

    df = pd.read_parquet(output_file)
    assert "brewery_count" in df.columns, "Not created brewery count column."
    assert len(df) > 0, "Empty dataframe."