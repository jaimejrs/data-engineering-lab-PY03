"""Testes do job PySpark da Silver (normalização + idempotência do MERGE INTO).

Item 8 de docs/06-analise-critica.md: "sem teste de idempotência / testes dos
jobs Spark e dbt". Roda uma SparkSession local de verdade (`local[1]`), com
Iceberg em catálogo `hadoop` (diretório temporário, sem Hive Metastore/HDFS) —
mesma engine de produção, sem a infraestrutura externa. Requer `pyspark`
instalado (não faz parte da suíte leve padrão — ver `.github/workflows/ci.yml`,
job `spark-tests`) e Java na máquina.

Rodando local no Windows (não é preciso no CI, que é Linux):
  - Precisa de `winutils.exe`/`hadoop.dll` (ex: github.com/cdarlint/winutils) +
    `HADOOP_HOME` apontando pra pasta que os contém, senão o Spark nem inicia
    (`HADOOP_HOME and hadoop.home.dir are unset`).
  - Precisa de `PYSPARK_PYTHON`/`PYSPARK_DRIVER_PYTHON` apontando pro
    `python.exe` do venv — sem isso, o worker do Spark tenta rodar o `python`
    genérico do PATH, que no Windows costuma ser o stub da Microsoft Store
    ("Python não foi encontrado..."), e todo `.collect()`/`.count()` falha com
    `Python worker failed to connect back`.
"""

import os

import pytest

pyspark = pytest.importorskip("pyspark")

# Precisa ser definido ANTES de importar src.spark_jobs.spark_session — os
# nomes de catálogo/tipo são lidos do ambiente na hora do import do módulo.
os.environ.setdefault("ICEBERG_CATALOG", "local_test")
os.environ.setdefault("ICEBERG_CATALOG_TYPE", "hadoop")
os.environ.setdefault("ICEBERG_NAMESPACE", "silver")
os.environ.setdefault("HDFS_DEFAULT_FS", "file:///")
# Em produção o jar do Iceberg vem embutido na imagem do Spark (ver
# spark_session.py); aqui, sem essa imagem, baixa via Maven/Ivy — precisa de
# internet (ok em CI/dev; não seria ok no servidor, que tem egress restrito).
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 pyspark-shell",
)

from pyspark.sql import Row  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src.spark_jobs import silver_job, spark_session  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    spark_session.WAREHOUSE = str(warehouse)  # sobrescreve o default HDFS pelo tmp_path do teste
    session = spark_session.build_session("test_silver_job")
    yield session
    session.stop()


class TestNormalize:
    def test_normalizes_api_date_ddmmyyyy_to_iso(self, spark):
        df = spark.createDataFrame([Row(id="1", data_assinatura="15/07/2026")])
        out = silver_job.normalize(df, "contratos")
        assert out.collect()[0]["data_assinatura"] == "2026-07-15"

    def test_normalizes_api_date_iso_with_timezone(self, spark):
        df = spark.createDataFrame([Row(id="1", data_assinatura="2026-07-21T00:00:00.000-03:00")])
        out = silver_job.normalize(df, "contratos")
        assert out.collect()[0]["data_assinatura"] == "2026-07-21"

    def test_derives_cnpj_cpf_and_tipo_credor_pf(self, spark):
        df = spark.createDataFrame([Row(id="1", plain_cpf_cnpj_financiador="123.456.789-01")])
        out = silver_job.normalize(df, "contratos")
        row = out.collect()[0]
        assert row["cnpj_cpf_normalizado"] == "12345678901"
        assert row["tipo_credor"] == "PF"

    def test_derives_tipo_credor_invalido_for_bad_length(self, spark):
        df = spark.createDataFrame([Row(id="1", plain_cpf_cnpj_financiador="123")])
        out = silver_job.normalize(df, "contratos")
        assert out.collect()[0]["tipo_credor"] == "INVALIDO"

    def test_does_not_touch_non_date_sources(self, spark):
        # empenhos usa dataemissao (POSTGRES_DATE_FIELDS), não data_assinatura —
        # normalize() só deve mexer nos campos de data da fonte certa.
        df = spark.createDataFrame([Row(id="1", dataemissao="2026-07-15 00:00:00.000", ano=2026)])
        out = silver_job.normalize(df, "empenhos")
        assert out.collect()[0]["dataemissao"] == "2026-07-15"


class TestAddPartitions:
    def test_derives_ano_mes_for_contratos_from_data_assinatura(self, spark):
        df = spark.createDataFrame([Row(id="1", data_assinatura="2026-07-15")])
        out = silver_job.add_partitions(df, "contratos")
        row = out.collect()[0]
        assert row["ano"] == 2026
        assert row["mes"] == 7

    def test_uses_real_ano_column_for_empenhos(self, spark):
        df = spark.createDataFrame([Row(id="1", dataemissao="2026-07-15", ano=2022)])
        out = silver_job.add_partitions(df, "empenhos")
        row = out.collect()[0]
        assert row["ano"] == 2022  # ano real da origem, não derivado da data
        assert row["mes"] == 7


class TestDedupBatch:
    def test_removes_duplicates_by_business_key(self, spark):
        df = spark.createDataFrame([Row(id="1", ano=2026), Row(id="1", ano=2026), Row(id="2", ano=2026)])
        out = silver_job.dedup_batch(df, "empenhos")
        assert out.count() == 2

    def test_tie_break_is_deterministic_across_repeated_calls(self, spark):
        """Item 2 de docs/06-analise-critica.md: quando a MESMA data_extracao
        traz duas versões DIFERENTES do mesmo registro (raro), o resultado do
        dedup não pode depender do plano de execução do Spark — rodar várias
        vezes sobre o mesmo lote sempre tem que sobrar a mesma linha."""
        df = spark.createDataFrame([Row(id="1", ano=2026, valor=100.0), Row(id="1", ano=2026, valor=200.0)])
        resultados = {silver_job.dedup_batch(df, "empenhos").collect()[0]["valor"] for _ in range(5)}
        assert len(resultados) == 1  # sempre a mesma linha vencedora, nunca varia entre execuções

    def test_tie_break_keeps_one_row_per_key_with_differing_non_key_values(self, spark):
        df = spark.createDataFrame(
            [Row(id="1", ano=2026, valor=100.0), Row(id="1", ano=2026, valor=200.0), Row(id="2", ano=2026, valor=1.0)]
        )
        out = silver_job.dedup_batch(df, "empenhos")
        rows = out.collect()
        assert len(rows) == 2
        vencedora = [r for r in rows if r["id"] == "1"][0]
        assert vencedora["valor"] in (100.0, 200.0)


class TestMergeIdempotency:
    """O teste central do item 8: reprocessar o MESMO lote não deve duplicar
    linhas na tabela Iceberg — é a promessa central do `MERGE INTO` (ver
    docstring de `silver_job.py` e docs/04-por-que-foi-evolucao.md)."""

    def test_reprocessing_same_batch_keeps_row_count_stable(self, spark):
        source = "unidade_gestora"  # dedup key (codigo, ano) — mais simples pro teste
        fqn = spark_session.table_fqn(source)
        if spark.catalog.tableExists(fqn):
            spark.sql(f"DROP TABLE {fqn}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {spark_session.CATALOG}.{spark_session.NAMESPACE}")

        df = spark.createDataFrame(
            [Row(codigo="1", ano=2026, titulo="ORGAO A"), Row(codigo="2", ano=2026, titulo="ORGAO B")]
        )

        silver_job.write_source(spark, source, df, "2026-07-20")
        primeira_carga = spark.table(fqn).count()

        # Reprocessa o MESMO lote, MESMA data_extracao (cenário real de retry/backfill sobreposto).
        silver_job.write_source(spark, source, df, "2026-07-20")
        segunda_carga = spark.table(fqn).count()

        assert primeira_carga == 2
        assert segunda_carga == primeira_carga  # idempotente: não duplicou

    def test_merge_updates_existing_row_instead_of_duplicating(self, spark):
        source = "unidade_gestora"
        fqn = spark_session.table_fqn(source)
        if spark.catalog.tableExists(fqn):
            spark.sql(f"DROP TABLE {fqn}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {spark_session.CATALOG}.{spark_session.NAMESPACE}")

        v1 = spark.createDataFrame([Row(codigo="1", ano=2026, titulo="NOME ANTIGO")])
        silver_job.write_source(spark, source, v1, "2026-07-10")

        v2 = spark.createDataFrame([Row(codigo="1", ano=2026, titulo="NOME NOVO")])
        silver_job.write_source(spark, source, v2, "2026-07-20")

        rows = spark.table(fqn).collect()
        assert len(rows) == 1
        assert rows[0]["titulo"] == "NOME NOVO"

    def test_reprocessing_older_data_extracao_does_not_overwrite_newer_row(self, spark):
        """Item 2 de docs/06-analise-critica.md: o gap que o `_data_extracao`
        + guarda no MERGE resolve. Sem a guarda, este teste falharia (o
        reprocessamento do lote antigo, rodado por último, sobrescreveria o
        nome já atualizado pelo lote mais novo)."""
        source = "unidade_gestora"
        fqn = spark_session.table_fqn(source)
        if spark.catalog.tableExists(fqn):
            spark.sql(f"DROP TABLE {fqn}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {spark_session.CATALOG}.{spark_session.NAMESPACE}")

        antigo = spark.createDataFrame([Row(codigo="1", ano=2026, titulo="NOME ANTIGO")])
        novo = spark.createDataFrame([Row(codigo="1", ano=2026, titulo="NOME NOVO")])

        # Ordem cronológica normal: processa a data_extracao mais nova primeiro...
        silver_job.write_source(spark, source, novo, "2026-07-20")
        # ...depois um backfill/retry reprocessa uma data_extracao MAIS ANTIGA
        # (cenário real: reprocessamento fora de ordem, lookback sobreposto, etc.)
        silver_job.write_source(spark, source, antigo, "2026-07-10")

        rows = spark.table(fqn).collect()
        assert len(rows) == 1
        assert rows[0]["titulo"] == "NOME NOVO"  # não foi sobrescrito pelo lote antigo
        assert str(rows[0]["_data_extracao"]) == "2026-07-20"  # marcador não regrediu

    def test_legacy_row_without_data_extracao_marker_is_still_updated(self, spark):
        """Linhas gravadas antes desta coluna existir (`_data_extracao` NULL
        após a evolução de schema) precisam continuar aceitando UPDATE
        normalmente — não podemos travar dado histórico achando que ele
        nunca deveria ser tocado de novo."""
        source = "unidade_gestora"
        fqn = spark_session.table_fqn(source)
        if spark.catalog.tableExists(fqn):
            spark.sql(f"DROP TABLE {fqn}")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {spark_session.CATALOG}.{spark_session.NAMESPACE}")

        # Cria a tabela "à mão", sem passar por write_source — simula um
        # registro legado sem a coluna _data_extracao preenchida.
        legado = spark.createDataFrame([Row(codigo="1", ano=2026, titulo="NOME LEGADO")])
        legado.writeTo(fqn).using("iceberg").partitionedBy(F.col("ano")).create()
        spark.sql(f"ALTER TABLE {fqn} ADD COLUMN `_data_extracao` date")
        assert spark.table(fqn).collect()[0]["_data_extracao"] is None

        atualizado = spark.createDataFrame([Row(codigo="1", ano=2026, titulo="NOME ATUALIZADO")])
        silver_job.write_source(spark, source, atualizado, "2026-07-15")

        rows = spark.table(fqn).collect()
        assert len(rows) == 1
        assert rows[0]["titulo"] == "NOME ATUALIZADO"
