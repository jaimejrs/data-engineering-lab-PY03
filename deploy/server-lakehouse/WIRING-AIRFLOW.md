# Wiring das DAGs 2/3 no Airflow do servidor — procedimento (aplicar em janela combinada)

> ⚠️ Este passo **recria os containers do Airflow compartilhado** (usados pelo time).
> Não é aditivo. Fazer em janela combinada, com o rollback abaixo à mão. O restante
> do lakehouse (HMS/Spark/Trino/dbt) já roda de forma aditiva sem isto — a Silver/Gold
> podem seguir sendo executadas manualmente (ver `docs/02-rotina-manutencao.md`).

## O que muda e por quê
Hoje a DAG 1 (Bronze) roda no Airflow; Silver/Gold são manuais. Para orquestrá-las,
o Airflow precisa de: **imagem com Java + pyspark + providers** (`apache-airflow-providers-apache-spark`,
`-docker`), **socket do Docker** (DockerOperator do dbt) e algumas **envs** apontando
para a topologia do servidor. As DAGs do repo já são portáveis por env
(`SPARK_DRIVER_HOST`, `DBT_DOCKER_NETWORK`).

## Passos

```bash
# 0. Backup (rollback): imagem atual + compose do time
docker tag datalab-airflow:local datalab-airflow:pre-lakehouse-backup
cp /home/dataadm/docker-compose.yml /home/dataadm/docker-compose.yml.bak

# 1. Rebuild da imagem do Airflow com os providers (contexto = repo docker/airflow,
#    enviado via scp). network: host por causa do egress IPv6.
docker build --network=host -t datalab-airflow:local /home/dataadm/lakehouse-airflow-build/docker/airflow

# 2. Teste a imagem ANTES de trocar os containers (não deve dar erro de import):
docker run --rm datalab-airflow:local airflow version
docker run --rm -v /home/dataadm/airflow/dags:/opt/airflow/dags datalab-airflow:local \
  airflow dags list-import-errors    # esperar: nenhum erro

# 3. Sincronizar código novo para o Airflow do servidor:
#    - DAGs: dags/dag_silver_transform.py, dags/dag_gold_load.py, dags/common.py
#    - src:  src/spark_jobs/* e src/transformers/rules.py (o silver_job roda como
#            driver client no scheduler)
#    (via scp para /home/dataadm/airflow/{dags,src})

# 4. Ajustar o compose do time (/home/dataadm/docker-compose.yml): no serviço
#    airflow-scheduler (e webserver), adicionar as envs e o socket:
#      environment:
#        - AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077
#        - SPARK_DRIVER_HOST=datalab_airflow_scheduler   # alias do scheduler na rede
#        - DBT_DOCKER_NETWORK=dataadm_default
#        - HIVE_METASTORE_URI=thrift://hive-metastore:9083
#        - ICEBERG_WAREHOUSE=hdfs://hadoop:9000/warehouse
#        - HDFS_DEFAULT_FS=hdfs://hadoop:9000
#        - HADOOP_USER_NAME=root
#      volumes:
#        - /var/run/docker.sock:/var/run/docker.sock   # só no scheduler
#    Obs.: a imagem já traz spark-submit (pyspark) + jars em /opt/spark-extra-jars.

# 5. Recriar só os serviços do Airflow:
cd /home/dataadm && docker compose up -d airflow-webserver airflow-scheduler

# 6. Validar no Airflow: DAG 1 (bronze) sem erro de import; DAG 2 (silver_transform)
#    e DAG 3 (gold_load) aparecem e disparam por Dataset (bronze -> silver -> gold).
```

## Rollback (se algo quebrar)
```bash
cp /home/dataadm/docker-compose.yml.bak /home/dataadm/docker-compose.yml
docker tag datalab-airflow:pre-lakehouse-backup datalab-airflow:local
cd /home/dataadm && docker compose up -d airflow-webserver airflow-scheduler
```

## Notas
- **Permissão do socket:** o usuário `airflow` (uid 50000) pode não ter acesso a
  `/var/run/docker.sock` (dono `root:docker`). Se o DockerOperator falhar por
  permissão, adicionar `group_add: ["<gid do grupo docker>"]` ao serviço, ou rodar o
  dbt manualmente (`docker compose run --rm dbt build`).
- **Spark no cluster:** com o wiring, a Silver passa a rodar via `SparkSubmitOperator`
  no cluster standalone (client mode), não mais em `local[*]`.
