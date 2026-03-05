import time
import pandas as pd
import json
from flask import Flask, Response

app = Flask(__name__)

# Esta função vai ler o seu arquivo e enviar os dados um por um
@app.route('/start')
def start_streaming():
    def generate():
        try:
            # ATENÇÃO: O arquivo train_sample.csv deve estar dentro da pasta 'data'
            df = pd.read_csv('data/train_sample.csv')
            print("Sucesso! Começando a enviar os dados para o navegador...")
            
            for index, row in df.iterrows():
                # Transforma a linha da tabela em texto (JSON)
                data = row.to_json()
                # Formato que o streaming exige
                yield f"data: {data}\n\n"
                # Espera meio segundo antes de enviar a próxima linha
                time.sleep(0.5) 
        except Exception as e:
            print(f"Erro: {e}")
            yield f"data: Erro ao ler arquivo: {str(e)}\n\n"

    return Response(generate(), mimetype='text/event-stream')

if __name__ == "__main__":
    print("Servidor Iniciado!")
    print("Acesse no Chrome: http://localhost:5000/start")
    app.run(host='0.0.0.0', port=5000)