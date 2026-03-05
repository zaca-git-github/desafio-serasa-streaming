import requests
import json
import duckdb
import pandas as pd
import time # Adicione isso

url = "http://producer:5000/start"

def consume_with_duckdb():
    # ESPERA O PRODUCER SUBIR
    print("Aguardando o Producer iniciar (5s)...")
    time.sleep(5)
    
    data_list = []
    print("Iniciando consumo com DuckDB e FILTROS...")
    
    try:
        response = requests.get(url, stream=True)
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8').replace('data: ', '')
                data = json.loads(decoded_line)
                data_list.append(data)
                print(f"Coletado: {data['key']}")

                if len(data_list) >= 4:
                    df = pd.DataFrame(data_list)
                    con = duckdb.connect()
                    
                    # FILTRO: Apenas corridas com passageiros (conforme o edital pediu)
                    query = "SELECT * FROM df WHERE passenger_count > 0"
                    
                    con.execute(f"CREATE TABLE taxi_filtrado AS {query}")
                    con.execute("COPY taxi_filtrado TO 'data/resultado_final.parquet' (FORMAT PARQUET)")
                    
                    print("\n--- SUCESSO: DADOS FILTRADOS E SALVOS EM PARQUET! ---")
                    break
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    consume_with_duckdb()