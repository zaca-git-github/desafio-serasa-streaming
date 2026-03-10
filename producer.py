from flask import Flask, Response
import pandas as pd
import time
import json

app = Flask(__name__)

# Lê o arquivo CSV de exemplo que serve como nossa fonte de dados
DATASET_PATH = 'train_sample.csv'

@app.route('/stream')
def stream():
    def generate():
        # Lê o CSV usando Pandas
        df = pd.read_csv(DATASET_PATH)
        # Transforma cada linha do CSV em um dicionário e envia como JSON pelo Stream
        for _, row in df.iterrows():
            json_data = row.to_json()
            # Formato padrão SSE: 'data: {conteúdo}\n\n'
            yield f"data: {json_data}\n\n"
            time.sleep(0.5) # Simula um delay de meio segundo entre cada corrida de táxi

    # Retorna a resposta com o tipo MIME correto para streaming (event-stream)
    return Response(generate(), mimetype='text/event-stream')

# Rota de controle para você iniciar o processo pelo navegador
@app.route('/start')
def start():
    return "🚀 Stream iniciado! O Consumer já pode ler os dados em /stream."

if __name__ == '__main__':
    # Roda o servidor na porta 5000, acessível por outros containers
    app.run(host='0.0.0.0', port=5000)