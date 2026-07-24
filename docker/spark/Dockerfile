# Imagem do cluster Spark (master + worker) com suporte a Iceberg + HDFS + HMS.
#
# Base: distribuição oficial do Spark 3.5.x (Scala 2.12, Java 17), que já traz os
# jars do client Hive 2.3.9 (necessários para o Iceberg HiveCatalog) e o client
# Hadoop 3.3 (compatível com HDFS 3.x).
#
# Jars via `ADD <url>`: o BuildKit baixa no build (a imagem base NÃO tem curl).
# Egress: o build usa `network: host` (host só tem saída IPv6; Maven atende IPv6 —
# ver documentacao/workaround-egress-ipv4-api.md).
FROM apache/spark:3.5.3

USER root

# Iceberg runtime (fat jar, inclui suporte a HiveCatalog) + driver JDBC do Postgres.
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/postgresql-42.7.4.jar
# ADD grava 0600/root; os jars precisam ser legíveis pelo usuário `spark` em runtime.
RUN chmod 644 /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark/jars/postgresql-42.7.4.jar

# silver_job é PySpark puro (sem pandas). O gold_job (Spark->pandas->Postgres) é
# legado — a Gold agora é dbt-trino sobre Iceberg (ver documentacao/gold-dbt-trino.md).
# Se for reativar o gold_job, instale pandas compatível com o Python da imagem
# (3.8 -> pandas<=2.0.3), sqlalchemy<2, psycopg2-binary, python-dotenv.

USER spark
