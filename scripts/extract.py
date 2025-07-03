# src/extract.py
import os
import requests
import json
from datetime import datetime

import logging  

logger = logging.getLogger("airflow.task")

# TODO: Melhorar os textos do console
# TODO: Adicionar logging para nao deixar as mensagens so no console

def extract_breweries():

    url_base = "https://api.openbrewerydb.org/v1/breweries"
    per_page = 200 # Limite da API
    page = 1
    all_data = []


    # TODO: adicionar tratamento de excecao ao chamar a API

    while True:
        response = requests.get(url_base, params={"per_page": per_page, "page": page})
        if response.status_code != 200:
            raise Exception(f"Error - API request failed with status code:  {response.status_code}")

        data = response.json()
        if not data:
            break

        all_data.extend(data)
        
        print(f"Page {page} - {len(data)} Records")
        page += 1

    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S") # NOTE: Adicionado os segundos pra ficar mais facil a criacao de novos arquivos, verificar a necessidade de deixar ou excluir
    os.makedirs("data/bronze", exist_ok=True)
    file_path = f"data/bronze/breweries_raw_{timestamp}.json"


    # TODO: adicionar tratamento de excecao ao salvar arquivo

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2)

    print(f"Completed with total: {len(all_data)} records saved on path: {file_path}")
    


# tetes
if __name__ == '__main__':
    extract_breweries()