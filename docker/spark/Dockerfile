# Imagem do cluster Spark (master + worker) com suporte a Iceberg + HDFS + HMS.
#
# Base: distribuição oficial do Spark 3.5.x (Scala 2.12, Java 17), que já traz os
# jars do client Hive 2.3.9 (necessários para o Iceberg HiveCatalog) e o client
# Hadoop 3.3 (compatível com o servidor HDFS 3.2.1 dos serviços bde2020/hadoop).
#
# Egress: o host só tem saída IPv6 (ver documentacao/workaround-egress-ipv4-api.md).
# O docker-compose builda esta imagem com `network: host`, então o curl abaixo usa a
# saída IPv6 do host. Maven Central (Fastly) atende por IPv6. Fallback offline: se o
# curl falhar, pré-baixe os dois jars para docker/spark/jars/ e troque o curl por COPY.
FROM apache/spark:3.5.3

USER root

ARG ICEBERG_VERSION=1.6.1
ARG POSTGRES_JDBC_VERSION=42.7.4
ENV MAVEN=https://repo1.maven.org/maven2

# Jars: Iceberg runtime (fat jar, inclui suporte a HiveCatalog) + driver JDBC do
# Postgres (usado pelo gold_job para escrever no DW, e útil ao HMS).
RUN curl -fSL \
      "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
    curl -fSL \
      "${MAVEN}/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      -o /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Dependências Python do driver (o gold_job roda a lógica pandas do dw_loader no
# driver e escreve no Postgres DW). Versões alinhadas ao requirements.txt do projeto.
RUN pip install --no-cache-dir \
      pandas==2.2.2 \
      "sqlalchemy>=1.4.36,<2.0" \
      psycopg2-binary==2.9.9 \
      python-dotenv==1.0.1

USER spark
