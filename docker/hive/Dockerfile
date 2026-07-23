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

COPY hive-site.xml /opt/hive/conf/hive-site.xml
COPY entrypoint.sh /entrypoint-metastore.sh
RUN chmod +x /entrypoint-metastore.sh

ENTRYPOINT ["/entrypoint-metastore.sh"]
