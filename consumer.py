import sseclient
import json
import duckdb
import os
import pandas as pd
import requests
import time

# Pasta onde os dados serao salvos
DATA_LAKE_PATH = 'data/analytics'
os.makedirs(DATA_LAKE_PATH, exist_ok=True)

con = duckdb.connect()

def process_and_save(event_data):
    try:
        dados_json = json.loads(event_data)
        df = pd.DataFrame([dados_json])
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        
        con.register('staging_table', df)

        # Particionamento por Ano, Mes e Dia (Requisito Serasa)
        query_filtrada = """
            SELECT *, 
                   year(pickup_datetime) as ano, 
                   month(pickup_datetime) as mes, 
                   day(pickup_datetime) as dia
            FROM staging_table
            WHERE passenger_count > 0 
        """

        con.execute(f"""
            COPY ({query_filtrada}) 
            TO '{DATA_LAKE_PATH}' 
            (FORMAT PARQUET, PARTITION_BY (ano, mes, dia), OVERWRITE_OR_IGNORE 1);
        """)
        
        print(f"✅ Sucesso! Gravado no Data Lake: {df['pickup_datetime'].iloc[0]}")
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")

def start_consumer():
    url = 'http://producer:5000/stream'
    print(f"🚀 Iniciando Consumidor... Conectando em: {url}")
    
    while True:
        try:
            response = requests.get(url, stream=True, timeout=5)
            if response.status_code == 200:
                client = sseclient.SSEClient(response)
                for msg in client.events():
                    if msg.data:
                        process_and_save(msg.data)
            else:
                print(f"⏳ Aguardando rota /stream (Status {response.status_code})...")
        except Exception:
            print("⚠️ Sem sinal do Producer. Tentando novamente em 5s...")
        time.sleep(5)

if __name__ == "__main__":
    start_consumer()