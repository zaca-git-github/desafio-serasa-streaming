import sseclient
import json
import duckdb
import os
import pandas as pd
import requests
import time

# Define o caminho raiz onde os dados particionados serão criados
DATA_LAKE_PATH = 'data/analytics'
os.makedirs(DATA_LAKE_PATH, exist_ok=True) # Cria a pasta se ela não existir

# Inicia o motor DuckDB em memória para processamento SQL ultra-rápido
con = duckdb.connect()

def process_and_save(event_data):
    try:
        # Transforma a mensagem de texto recebida do stream em um dicionário Python
        dados_json = json.loads(event_data)
        
        # Cria um DataFrame temporário com o novo registro
        df = pd.DataFrame([dados_json])
        
        # Converte o texto da data para o formato real de Data/Hora (essencial para o SQL)
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        
        # Registra o DataFrame como uma tabela virtual para o DuckDB enxergar
        con.register('staging_table', df)

        # Query SQL: Extrai Ano/Mês/Dia e limpa dados (filtra passageiros > 0)
        query_filtrada = """
            SELECT *, 
                   year(pickup_datetime) as ano, 
                   month(pickup_datetime) as mes, 
                   day(pickup_datetime) as dia
            FROM staging_table
            WHERE passenger_count > 0 
        """

        # Comando 'COPY': Salva em Parquet e cria as pastas ano/mes/dia automaticamente (Hive Partitioning)
        con.execute(f"""
            COPY ({query_filtrada}) 
            TO '{DATA_LAKE_PATH}' 
            (FORMAT PARQUET, PARTITION_BY (ano, mes, dia), OVERWRITE_OR_IGNORE 1);
        """)
        
        print(f"✅ Sucesso! Gravado no Data Lake: {df['pickup_datetime'].iloc[0]}")
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")

def start_consumer():
    # URL do serviço 'producer' dentro da rede do Docker
    url = 'http://producer:5000/stream'
    print(f"🚀 Iniciando Consumidor... Conectando em: {url}")
    
    while True: # Loop infinito para manter o serviço sempre ligado
        try:
            # Faz a conexão de streaming com o Producer
            response = requests.get(url, stream=True, timeout=5)
            if response.status_code == 200:
                client = sseclient.SSEClient(response)
                # Para cada nova mensagem que chegar no stream, processa e salva
                for msg in client.events():
                    if msg.data:
                        process_and_save(msg.data)
            else:
                print(f"⏳ Aguardando rota /stream (Status {response.status_code})...")
        except Exception:
            # Se o Producer estiver fora do ar, espera 5s e tenta de novo (Resiliência)
            print("⚠️ Sem sinal do Producer. Tentando novamente em 5s...")
        time.sleep(5)

if __name__ == "__main__":
    start_consumer()