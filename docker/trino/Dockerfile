# Trino single-node — query engine do lakehouse (connector Iceberg -> mesmo HMS/HDFS).
# A imagem base já vem com config single-node (coordinator + worker); só
# adicionamos os catálogos e a config de HDFS.
FROM trinodb/trino:455

# Hostname do HDFS parametrizado no build (compose passa ${HDFS_HOST}).
ARG HDFS_HOST=namenode
COPY catalog/iceberg.properties /etc/trino/catalog/iceberg.properties
COPY hadoop/core-site.xml.template /tmp/core-site.xml.template
RUN mkdir -p /etc/trino/hadoop \
    && sed "s/__HDFS_HOST__/${HDFS_HOST}/g" /tmp/core-site.xml.template > /etc/trino/hadoop/core-site.xml
