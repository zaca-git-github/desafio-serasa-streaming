# Projeto_1 - Pipeline de Streaming Analítico

Este projeto implementa uma solução completa de engenharia de dados para processamento de eventos em tempo real, utilizando tecnologias modernas de alta performance.

######################################################################################

##  Tecnologias e Requisitos Atendidos

Tecnologias e Requisitos Atendidos

Streaming (Flask-SSE): Produção de eventos em tempo real baseados no dataset de táxis.

Consumo e Filtros (DuckDB): Utilização do motor DuckDB para consumir o stream e aplicar filtros analíticos via SQL (ex: passenger_count > 0) antes da persistência.

Armazenamento (Parquet): Consolidação dos dados filtrados em formato colunar Parquet para otimização de consultas batch.

Datalake Analítico: Estrutura preparada para consultas batch de alto desempenho.

Containerização (Docker): Pipeline totalmente orquestrado via Docker Compose.

######################################################################################

##  Como Executar

1. Certifique-se de ter o Docker instalado.
2. Na raiz do projeto, execute:
   ```bash
   docker compose up --build
