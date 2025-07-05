import os
import glob
import json
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.extract import extract_breweries, bronze_path


def test_extract_creates_json_file():
   
    extract_breweries()

    arquivos = glob.glob(f"{bronze_path}breweries_raw_*.json")
    assert len(arquivos) > 0, "None file was created."

    with open(arquivos[0], "r", encoding="utf-8") as f:
        dados = json.load(f)
        assert isinstance(dados, list), "Invalid data from api"
        assert len(dados) > 0, "None records was found from api."