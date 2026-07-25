# Hive Metastore standalone — catálogo do Iceberg, backing store no Postgres.
#
# É a peça mais sensível a ambiente do stack (ver documentacao/lakehouse-spark-iceberg.md):
# depende do DB `metastore` existir no serviço `postgres` e do schema estar
# inicializado. O entrypoint faz o initSchema de forma idempotente.
#
# Egress: build com `network: host` (host só tem IPv6). Jar via `ADD` porque a
# imagem apache/hive NÃO tem curl.
FROM apache/hive:4.0.0

USER root

# Driver JDBC do Postgres no classpath do Hive (metastore -> Postgres).
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/hive/lib/postgresql-42.7.4.jar
RUN chmod 644 /opt/hive/lib/postgresql-42.7.4.jar

# Hostname do HDFS/Postgres e credencial do metastore parametrizados no build
# (compose passa ${HDFS_HOST}/${METASTORE_DB_USER}/${METASTORE_DB_PASSWORD});
# token substituído no template -> um só arquivo p/ namenode|hadoop e p/
# qualquer usuário/senha (default 'hive'/'hive' só pro stack local descartável
# — ver docker/postgres-init/01-metastore.sh e .env.example).
ARG HDFS_HOST=namenode
ARG METASTORE_DB_HOST=postgres
ARG METASTORE_DB_USER=hive
ARG METASTORE_DB_PASSWORD=hive
COPY hive-site.xml.template /tmp/hive-site.xml.template
RUN sed -e "s/__HDFS_HOST__/${HDFS_HOST}/g" -e "s/__METASTORE_DB_HOST__/${METASTORE_DB_HOST}/g" \
      -e "s/__METASTORE_DB_USER__/${METASTORE_DB_USER}/g" -e "s/__METASTORE_DB_PASSWORD__/${METASTORE_DB_PASSWORD}/g" \
      /tmp/hive-site.xml.template > /opt/hive/conf/hive-site.xml
COPY entrypoint.sh /entrypoint-metastore.sh
RUN chmod +x /entrypoint-metastore.sh

ENTRYPOINT ["/entrypoint-metastore.sh"]
