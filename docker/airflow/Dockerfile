FROM apache/airflow:2.9.1

# Java (JRE 17) para o spark-submit rodar o driver Spark em client mode.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Jars extras que o driver/executores precisam no client mode (passados via
# spark.jars nas DAGs): Iceberg runtime + driver JDBC do Postgres. Via `ADD`
# (robusto, não depende de curl na imagem). Build usa `network: host` (host IPv6).
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark-extra-jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark-extra-jars/postgresql-42.7.4.jar
RUN chmod -R 644 /opt/spark-extra-jars/*.jar

USER airflow
COPY requirements.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt

# O wheel do pyspark traz spark-submit + os jars do Spark, mas fora do PATH.
# Symlink estável (independe da versão do Python) + PATH/SPARK_HOME para o
# SparkSubmitOperator achar o binário.
RUN PYSPARK_DIR="$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" \
    && ln -s "$PYSPARK_DIR" /home/airflow/spark
ENV SPARK_HOME=/home/airflow/spark
ENV PATH="/home/airflow/spark/bin:${PATH}"
