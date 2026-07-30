# Painel Streamlit de negócio (Carlos) — consome iceberg.gold/ml via Trino.
# Build context = ./streamlit (o próprio projeto), mesmo padrão do dbt/Dockerfile
# — o docker-compose do overlay monta ./streamlit por cima em runtime, então
# mudança de código aplica sem rebuild (só requirements.txt novo exige rebuild).
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
