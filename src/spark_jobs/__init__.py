"""Jobs PySpark do pipeline lakehouse (Silver Iceberg + carga Gold).

Executados no cluster Spark standalone (ver docker-compose.yml), submetidos
pelas DAGs 2 e 3 via SparkSubmitOperator. Toda a configuração Iceberg/Hive
Metastore/HDFS fica em `spark_session.build_session`.
"""
