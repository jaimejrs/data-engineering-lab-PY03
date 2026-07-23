# Trino single-node — query engine do lakehouse (connector Iceberg -> mesmo HMS/HDFS).
# A imagem base já vem com config single-node (coordinator + worker); só
# adicionamos os catálogos e a config de HDFS.
FROM trinodb/trino:455

COPY catalog/iceberg.properties /etc/trino/catalog/iceberg.properties
COPY catalog/postgres.properties /etc/trino/catalog/postgres.properties
COPY hadoop/core-site.xml /etc/trino/hadoop/core-site.xml
