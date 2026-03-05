# Usa uma imagem leve do Python
FROM python:3.10-slim

# Define a pasta de trabalho dentro do container
WORKDIR /app

# Instala as bibliotecas necessárias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia todos os arquivos do seu projeto para o container
COPY . .

# Comando padrão
CMD ["python", "producer.py"]