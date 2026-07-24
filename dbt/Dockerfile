# Imagem do dbt (adapter Trino), isolada do Airflow para evitar conflito de
# dependências dbt-core x airflow. Build context = ./dbt (o próprio projeto).
#
# Egress: build com `network: host` (host só tem IPv6, ver docker/airflow/README.md).
FROM python:3.11-slim

# Egress: o servidor não alcança github.com. --only-binary evita builds de sdist
# (que baixam wheels do GitHub); tudo vem de wheels do PyPI (IPv6 OK).
RUN pip install --no-cache-dir --only-binary=:all: "dbt-trino==1.8.4"

# Perfil embutido no projeto; --profiles-dir aponta para /dbt.
ENV DBT_PROFILES_DIR=/dbt
WORKDIR /dbt

# Projeto embutido (usado pelo DockerOperator da DAG 3). O serviço `dbt` do
# compose monta ./dbt por cima em runtime, então execuções manuais pegam a versão
# viva do projeto.
COPY . /dbt

ENTRYPOINT ["dbt"]
