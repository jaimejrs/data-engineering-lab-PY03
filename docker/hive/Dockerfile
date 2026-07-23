# Hive Metastore standalone — catálogo do Iceberg, backing store no Postgres.
#
# É a peça mais sensível a ambiente do stack (ver documentacao/lakehouse-spark-iceberg.md):
# depende do DB `metastore` existir no serviço `postgres` e do schema estar
# inicializado. O entrypoint faz o initSchema de forma idempotente.
#
# Egress: build com `network: host` (host só tem IPv6) para o curl do driver JDBC.
FROM apache/hive:4.0.0

USER root

ARG POSTGRES_JDBC_VERSION=42.7.4
# Driver JDBC do Postgres no classpath do Hive (metastore -> Postgres).
RUN curl -fSL \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
      -o /opt/hive/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY entrypoint.sh /entrypoint-metastore.sh
RUN chmod +x /entrypoint-metastore.sh

ENTRYPOINT ["/entrypoint-metastore.sh"]
