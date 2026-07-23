"""Fábrica da SparkSession configurada para o lakehouse (Iceberg + Hive Metastore + HDFS).

Fonte única de verdade da configuração Spark<->Iceberg<->HMS<->HDFS. Tudo
parametrizável por variável de ambiente para não hardcodar hostnames do
docker-compose, mas com defaults que funcionam dentro da rede do compose.

Os jars do Iceberg e do driver JDBC do Postgres são embutidos na imagem do
Spark (docker/spark/Dockerfile) — nada é baixado em runtime (`--packages`),
porque o host tem egress restrito (ver documentacao/workaround-egress-ipv4-api.md).
"""

import os

from pyspark.sql import SparkSession

# Nome do catálogo Iceberg exposto no Spark SQL (ex: lakehouse.silver.empenhos).
CATALOG = os.environ.get("ICEBERG_CATALOG", "lakehouse")
# Namespace (database no HMS) onde ficam as tabelas da Silver.
NAMESPACE = os.environ.get("ICEBERG_NAMESPACE", "silver")

HMS_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse")
HDFS_DEFAULT_FS = os.environ.get("HDFS_DEFAULT_FS", "hdfs://namenode:9000")

ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"


def build_session(app_name: str) -> SparkSession:
    """Cria (ou reusa) a SparkSession com o catálogo Iceberg `CATALOG` sobre o HMS/HDFS.

    As configs de extensão e de catálogo precisam ser definidas *antes* de a
    sessão ser criada — por isso vão no builder, não via `spark.conf.set` depois.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", ICEBERG_EXTENSIONS)
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hive")
        .config(f"spark.sql.catalog.{CATALOG}.uri", HMS_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        # Trata TIMESTAMP sem timezone (comum em dados legados) sem estourar erro.
        .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
        .config("spark.hadoop.fs.defaultFS", HDFS_DEFAULT_FS)
        .enableHiveSupport()
        .getOrCreate()
    )


def table_fqn(source: str) -> str:
    """Nome totalmente qualificado da tabela Iceberg de uma fonte (ex: lakehouse.silver.empenhos)."""
    return f"{CATALOG}.{NAMESPACE}.{source}"
