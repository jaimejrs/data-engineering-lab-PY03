# Apache Superset — camada de visualização que fala nativamente com o Trino
# (sqlalchemy-trino), sem driver ODBC nem conector customizado — troca pelo
# Power BI decidida depois de bugs reais de acentuação no driver ODBC
# disponível pra Trino (ver stacks/apache-superset.md).
FROM apache/superset:latest

USER root
# A imagem base usa `uv` (não pip) pra gerenciar o virtualenv em /app/.venv,
# que nem tem um `pip` instalado nele — um `pip install` comum vai pro pip do
# sistema, fora do venv que o Superset realmente usa em runtime. Precisa
# apontar o `uv` explicitamente pro python do venv.
RUN uv pip install --python /app/.venv/bin/python trino sqlalchemy-trino psycopg2-binary

COPY superset_config.py /app/pythonpath/superset_config.py

USER superset
