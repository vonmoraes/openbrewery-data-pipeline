import os
import glob
import json
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.extract import extract_breweries

def test_extract_creates_json_file():
    for f in glob.glob("data/bronze/breweries_raw_*.json"):
        os.remove(f)

    extract_breweries()

    arquivos = glob.glob("data/bronze/breweries_raw_*.json")
    assert len(arquivos) > 0, "None file was created."

    with open(arquivos[0], "r", encoding="utf-8") as f:
        dados = json.load(f)
        assert isinstance(dados, list), "Invalid data from api"
        assert len(dados) > 0, "None records was found from api."