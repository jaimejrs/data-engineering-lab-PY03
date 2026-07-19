# Diagrama de Arquitetura — Pipeline Ceará Transparente

Última atualização: 19/07/2026. Status por componente conforme o [`checklist`](../docs/checklist.md)
interno da equipe (não versionado) — aqui mantemos apenas o retrato público da arquitetura.

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

    subgraph Silver["Silver — HDFS Parquet (limpo, dedup, tipado)"]
        S["particionado ano/mês"]
    end

    subgraph Gold["Gold — PostgreSQL DW (dimensional)"]
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
    subgraph DAG2["DAG 2 — Silver (não implementada)"]
        d2a[bronze_to_silver]
    end
    subgraph DAG3["DAG 3 — Gold (não implementada)"]
        d3a[dw_loader]
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
        PG_META[("postgres\n(metadados Airflow +\nfutura Gold/DW)")]
        NN["namenode\n(HDFS, WebHDFS :9870)"]
        DN["datanode"]
        AF_INIT["airflow-init"]
        AF_WEB["airflow-webserver :8080"]
        AF_SCH["airflow-scheduler"]
        JUP["jupyter :8888"]
    end
    SRC_PG["PostgreSQL de origem\n(externo — infra do curso)"]
    SRC_API["API Ceará Transparente\n(externa)"]

    AF_SCH -->|extract_postgres| SRC_PG
    AF_SCH -->|extract_api| SRC_API
    AF_SCH -->|grava Bronze/Silver| NN
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

## Status resumido (19/07/2026)

| Camada/Componente | Status |
|---|---|
| Bronze — `empenhos`, `ordem_bancaria_orcamentaria`, `unidade_gestora` | ✅ Validado no HDFS real |
| Bronze — `contratos` (API) | ✅ Validado dentro do Airflow (19/07 — via relay, ver nota acima) |
| DAG 1 (`bronze_extract`) no Airflow | ✅ 4/4 tasks com sucesso ponta a ponta (`extract_postgres`, `extract_api`, `validate`, `advance_watermark`) — primeira execução 100% dentro do Airflow |
| Silver | ⚪ Não iniciada |
| Gold (DW) | ⚪ Não iniciada — nenhuma tabela dimensional/fato existe ainda |
| ML/IA | ⚪ Não iniciada |
| `docker-compose.yml` reproduzível | ✅ Adicionado (Postgres, Hadoop NameNode/DataNode, Airflow com imagem custom, Jupyter) |
