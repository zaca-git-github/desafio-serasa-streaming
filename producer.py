from flask import Flask, Response
import time
import pandas as pd
import json
import os

app = Flask(__name__)

@app.route('/stream')
def stream():
    def event_stream():
        # Caminho corrigido para a pasta correta
        csv_path = 'data/train_sample.csv' 
        
        if not os.path.exists(csv_path):
            print(f"❌ Erro: Arquivo nao encontrado em {csv_path}")
            yield f"data: {json.dumps({'error': 'file_not_found'})}\n\n"
            return

        df = pd.read_csv(csv_path)
        for index, row in df.iterrows():
            json_data = row.to_json()
            yield f"data: {json_data}\n\n"
            time.sleep(0.5)
            
    return Response(event_stream(), mimetype="text/event-stream")

@app.route('/start')
def start():
    return "🚀 Servidor de Streaming ativo! O consumidor ja pode ler de /stream."

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)