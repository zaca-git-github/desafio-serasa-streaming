# Pipeline de Streaming de Dados - Desafio Serasa

Este projeto implementa um pipeline de engenharia de dados completo, focado em alta performance e escalabilidade.

##  Tecnologias Utilizadas
- **Producer (Flask-SSE):** Geração de eventos de streaming baseados no dataset de táxis.
- **Consumer (DuckDB):** Consumo dos eventos com motor SQL OLAP para filtragem em tempo real.
- **Armazenamento (Parquet):** Persistência dos dados consolidados em formato colunar.
- **Orquestração (Docker Compose):** Ambiente totalmente containerizado e isolado.

##  Filtros e Consolidação
A aplicação realiza a filtragem dos eventos consumidos (ex: `passenger_count > 0`) utilizando o **DuckDB**, garantindo que apenas dados relevantes sejam persistidos no Datalake. Esta estrutura permite consultas batch subsequentes de forma extremamente eficiente.

##  Como Executar
Basta ter o Docker instalado e rodar:
```bash
docker compose up --build