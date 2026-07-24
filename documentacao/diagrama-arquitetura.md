# Diagrama de Arquitetura — Pipeline Ceará Transparente

Última atualização: 22/07/2026. Status por componente conforme o [`checklist`](../docs/checklist.md)
interno da equipe (não versionado) — aqui mantemos apenas o retrato público da arquitetura.
Evolução para **lakehouse** (Silver em Iceberg, Spark, Hive Metastore) documentada em
[`lakehouse-spark-iceberg.md`](lakehouse-spark-iceberg.md).

## Visão geral (Medallion: Bronze → Silver → Gold → ML/IA)

```mermaid
flowchart LR
    subgraph Fontes
        API["API REST\nCeará Transparente\n(contratos)"]
        PG["PostgreSQL de origem\n(empenhos, ordem_bancaria_\norcamentaria, unidade_gestora)"]
    end

    subgraph Bronze["Bronze — HDFS (bruto)"]
        B_CONTRATOS[("contratos/")]
        B_EMPENHOS[("empenhos/")]
        B_OB[("ordem_bancaria_orcamentaria/")]
        B_UG[("unidade_gestora/")]
    end

    subgraph Silver["Silver — Iceberg (HDFS + Hive Metastore)"]
        S["lakehouse.silver.*\nMERGE INTO · snapshots · particionado ano/mês"]
    end

    subgraph Gold["Gold — Iceberg (HDFS) via dbt-trino"]
        DIM["dim_credor · dim_orgao\ndim_modalidade · dim_tempo"]
        FATO["fato_contrato · fato_empenho"]
    end

    subgraph MLIA["ML / IA"]
        M1["Isolation Forest\n(anomalias)"]
        M2["XGBoost/Prophet\n(previsão trimestral)"]
        LLM["LLM\n(relatório narrativo)"]
    end

    API -->|extract_api| B_CONTRATOS
    PG -->|extract_postgres| B_EMPENHOS
    PG -->|extract_postgres| B_OB
    PG -->|extract_postgres| B_UG

    B_CONTRATOS --> S
    B_EMPENHOS --> S
    B_OB --> S
    B_UG --> S

    S --> DIM --> FATO
    FATO --> M1 --> FATO
    FATO --> M2
    M1 --> LLM
    M2 --> LLM

    classDef done fill:#1a7f37,color:#fff,stroke:none;
    classDef partial fill:#9a6700,color:#fff,stroke:none;
    classDef pending fill:#57606a,color:#fff,stroke:none;

    class B_EMPENHOS,B_OB,B_UG,B_CONTRATOS done
    class S,DIM,FATO,M1,M2,LLM pending
```

**Legenda de status:** 🟢 verde = concluído e validado dentro do Airflow · 🟠 laranja = concluído via
workaround manual (fora do Airflow) · ⚪ cinza = não iniciado.

## Orquestração — DAGs do Airflow

```mermaid
flowchart TB
    subgraph DAG1["DAG 1 — bronze_extract (funcional ponta a ponta — ver nota sobre extract_api)"]
        d1a[extract_postgres] --> d1c[validate]
        d1b[extract_api] --> d1c
        d1c --> d1d[advance_watermark]
    end
    subgraph DAG2["DAG 2 — Silver (SparkSubmitOperator -> silver_job.py)"]
        d2a[transform: Bronze -> Iceberg MERGE]
    end
    subgraph DAG3["DAG 3 — Gold (DockerOperator -> dbt build)"]
        d3a[dbt-trino: Silver Iceberg -> Gold Iceberg]
    end
    subgraph DAG4["DAG ML (não implementada)"]
        d4a[anomaly_detection]
        d4b[payment_forecast]
    end

    DAG1 -.depende de tarefas 10/13/14.-> DAG2 -.depende de tarefas 15/16.-> DAG3 --> DAG4
```

## Infraestrutura (`docker-compose.yml`)

```mermaid
flowchart LR
    subgraph Docker["docker-compose.yml"]
        PG_META[("postgres\n(metadados Airflow +\nDB metastore do Hive)")]
        PG_DW[("postgres_dw :5434\n(Gold/DW)")]
        NN["namenode\n(HDFS, WebHDFS :9870)"]
        DN["datanode"]
        AF_INIT["airflow-init"]
        AF_WEB["airflow-webserver :8080"]
        AF_SCH["airflow-scheduler\n(spark-submit / DockerOperator)"]
        SM["spark-master :7077"]
        SW["spark-worker (executores)"]
        HMS["hive-metastore :9083"]
        TR["trino :8085\n(connector Iceberg)"]
        DBT["dbt-trino\n(container sob demanda)"]
        JUP["jupyter :8888"]
    end
    SRC_PG["PostgreSQL de origem\n(externo — infra do curso)"]
    SRC_API["API Ceará Transparente\n(externa)"]

    AF_SCH -->|extract_postgres| SRC_PG
    AF_SCH -->|extract_api| SRC_API
    AF_SCH -->|grava Bronze JSON| NN
    AF_SCH -->|DAG2: submit Silver| SM --> SW
    AF_SCH -->|DAG3: dispara dbt| DBT --> TR
    SW -->|dados Iceberg Silver| NN
    TR -->|Gold Iceberg| NN
    SM -->|catálogo| HMS
    TR -->|catálogo| HMS
    HMS -->|warehouse + metadados| PG_META
    HMS -->|warehouse| NN
    TR -.->|federação opcional| PG_DW
    NN --- DN
    AF_WEB --> PG_META
    AF_SCH --> PG_META
    AF_INIT --> PG_META
    JUP -.-> NN
```

> **Nota sobre o Postgres de origem:** `SOURCE_POSTGRES_URL` aponta para um banco
> fornecido pela infraestrutura do curso (fora deste `docker-compose.yml`, hoje
> acessado via relay `pg-source-relay.service` no servidor do Datalab). O compose
> não tenta recriar esse banco — apenas o consome como fonte externa.
>
> **Nota sobre `extract_api`:** o host não tem saída IPv4 (só IPv6), mas a API do
> Ceará Transparente é IPv4-only. `extra_hosts` neste compose aponta o hostname da
> API para um relay TCP via Tailscale — ver
> [`workaround-egress-ipv4-api.md`](workaround-egress-ipv4-api.md) para o mecanismo
> completo e a limitação (depende de uma máquina do time estar ligada).

## Status resumido (22/07/2026)

| Camada/Componente | Status |
|---|---|
| Bronze — `empenhos`, `ordem_bancaria_orcamentaria`, `unidade_gestora` | ✅ Validado no HDFS real |
| Bronze — `contratos` (API) | ✅ Validado dentro do Airflow (19/07 — via relay, ver nota acima) |
| DAG 1 (`bronze_extract`) no Airflow | ✅ 4/4 tasks com sucesso ponta a ponta (`extract_postgres`, `extract_api`, `validate`, `advance_watermark`) — primeira execução 100% dentro do Airflow |
| Silver — **Iceberg via Spark** (`silver_job.py`, `MERGE INTO`) | 🟠 Implementada; validação ponta a ponta no cluster pendente (ver [`lakehouse-spark-iceberg.md`](lakehouse-spark-iceberg.md)) |
| Gold — **Iceberg via dbt-trino** (`dbt/`, dims/fatos + testes) | 🟠 Implementada; validação ponta a ponta pendente (ver [`gold-dbt-trino.md`](gold-dbt-trino.md)) |
| Cluster Spark + Hive Metastore + Trino + tabelas Iceberg (HDFS) | 🟠 Adicionados ao `docker-compose.yml`; wiring de versões/classpath a validar com o stack no ar |
| ML/IA | ⚪ Não iniciada |
| `docker-compose.yml` reproduzível | ✅ Postgres (+DB metastore), Hadoop NN/DN, Airflow custom, Jupyter, **spark-master/worker**, **hive-metastore**, **trino**, **dbt** |
