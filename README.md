# Ingestão & Camada Bronze — Ceará Transparente

Escopo do Membro 3 (Nara) — extração da API REST e do PostgreSQL de origem para a camada Bronze (arquitetura medalhão)

`Fase 1 — Ingestão` · Última atualização: 16/07/2026 · Responsável: Nara (Membro 3)

> Versão estilizada em HTML deste mesmo documento: [`README.html`](README.html) (abra localmente no navegador — o GitHub não renderiza `.html` como página).

## Visão geral

Este módulo extrai dados de duas fontes e grava na camada Bronze (local em disco, para desenvolvimento, ou HDFS via WebHDFS, para o pipeline compartilhado):

- **API REST do Ceará Transparente** — contratos públicos (`contratos/contratos`), com paginação.
- **PostgreSQL de origem** — tabelas `empenhos`, `ordem_bancaria_orcamentaria` (filtradas por data) e `unidade_gestora` (tabela de referência, completa a cada execução).

## Estrutura de diretórios

```
projeto-final/
├── data/bronze/              # saída local (dev) — ignorado pelo git
├── notebooks/
│   └── exploracao_ingestao.ipynb
├── src/
│   ├── extractors/
│   │   ├── api_extractor.py       ← extração paginada da API de contratos
│   │   ├── postgres_extractor.py  ← extração das tabelas de origem
│   │   └── storage.py             ← escrita na Bronze (local ou HDFS)
│   ├── transformers/         # Silver/Gold — Membro 2 (ainda não implementado)
│   └── loaders/               # carga no DW — Membro 2 (ainda não implementado)
├── dags/                      # Airflow — Membro 1 (ainda não implementado)
├── sql/                       # DDL do DW — Membro 2 (ainda não implementado)
├── models/                    # ML — Membro 4 (ainda não implementado)
├── tests/
│   └── test_extractors.py
├── .env / .env.example
└── requirements.txt
```

## Configuração

Copie `.env.example` para `.env` e ajuste os valores. Variáveis relevantes para este módulo:

| Variável | Descrição | Padrão |
|---|---|---|
| `CEARA_TRANSPARENTE_API_URL` | Endpoint base da API de contratos | URL oficial da API |
| `CEARA_API_TIMEOUT_SECONDS` | Timeout por requisição | 30 |
| `CEARA_API_SLEEP_SECONDS` | Espera entre páginas (rate limit) | 1.0 |
| `CEARA_API_MAX_RETRIES` | Tentativas em caso de falha/429 | 3 |
| `SOURCE_POSTGRES_URL` | String de conexão do Postgres de origem | — |
| `POSTGRES_EXTRACT_CHUNK_SIZE` | Máx. de linhas por arquivo JSON gravado | 20000 |
| `BRONZE_STORAGE_BACKEND` | `local` (disco) ou `hdfs` (WebHDFS) | local |
| `BRONZE_BASE_PATH` | Caminho base — relativo se `local`, absoluto (`/bronze`) se `hdfs` | ./data/bronze |
| `HDFS_WEBHDFS_URL` | URL do NameNode (WebHDFS) | — |
| `HDFS_USER` | Usuário HDFS (via `user.name`) | — |

> **Nunca commitar o `.env`** — já está no `.gitignore`. Só o `.env.example` (sem credenciais reais) deve ir para o repositório.

## Como rodar

```bash
# Contratos da API (datas em ISO — a conversão para o formato da API é automática)
python -m src.extractors.api_extractor --inicio 2026-06-01 --fim 2026-06-03

# Tabelas do PostgreSQL (empenhos, ordem_bancaria_orcamentaria, unidade_gestora)
python -m src.extractors.postgres_extractor --inicio 2026-06-01 --fim 2026-06-04

# Testes
python -m pytest tests/ -v
```

## Particularidades importantes (não estão no enunciado oficial)

### API de contratos

- **Formato de data real da API é `DD/MM/YYYY`**, não ISO. Os argumentos `--inicio`/`--fim` do script continuam em ISO (`YYYY-MM-DD`) por consistência com o extractor do Postgres — a conversão pro formato da API é feita internamente. Enviar ISO direto faz a API responder `HTTP 200` com texto puro de erro em vez de JSON.
- **A chave de paginação é `"sumary"`** (erro de digitação real da API, falta o 2º "m"), não `"summary"` como o enunciado sugere. O código já trata isso com fallback: `payload.get("sumary") or payload.get("summary")`.
- Se a resposta não trouxer `total_pages` de nenhuma das duas formas, a extração **aborta com erro** em vez de arriscar um loop infinito.
- `sleep` entre páginas e retry com backoff em respostas `429`/falha de rede, configuráveis via `.env`.

### PostgreSQL de origem

- Nenhuma tabela tem **PRIMARY KEY** declarada no banco real, mesmo as que o enunciado descreve com PK lógica (ex: `empenhos (PK: id, ano)`). Não assumir unicidade de `id` sem deduplicação a jusante.
- Colunas de data são `TEXT` (ex: `'2026-06-02 00:00:00.000'`), não `DATE`/`TIMESTAMP`. A comparação lexicográfica com `'YYYY-MM-DD'` funciona porque o prefixo é ISO 8601. A coluna real usada para filtro incremental é `dataemissao` (não `data_empenho`/`data_pagamento` como um rascunho antigo do enunciado sugeria).
- Cada tabela é gravada em **blocos de até `POSTGRES_EXTRACT_CHUNK_SIZE` linhas** (`chunk_0001.json`, `chunk_0002.json`, ...) em vez de um arquivo único — necessário porque o histórico completo de `empenhos`/`ordem_bancaria_orcamentaria` tem centenas de milhares a milhões de linhas, e um arquivo único ficaria grande demais para escrever de uma vez via WebHDFS.

### Backend HDFS — atenção ao rodar fora da rede do Datalab

> O WebHDFS grava em duas etapas: o NameNode responde com um redirecionamento apontando para o **hostname interno do DataNode** (`hadoop`, porta `9864`) — nome que não resolve fora da rede Docker do Datalab. Se for rodar a extração com `BRONZE_STORAGE_BACKEND=hdfs` de uma máquina Windows fora do servidor (via VPN), é necessário adicionar ao `hosts` (`C:\Windows\System32\drivers\etc\hosts`):
>
> ```
> 100.69.31.14 hadoop
> ```
>
> Atenção a uma possível entrada conflitante `127.0.0.1 hadoop` criada pelo Docker Desktop — ela precisa estar comentada/removida, senão a escrita falha com `ConnectionRefusedError`/`MaxRetryError` mesmo com a permissão do HDFS correta.

## Chaves de junção — Contratos (API) × PostgreSQL

Validadas cruzando os contratos já extraídos contra o banco real (amostra de 740 contratos, 01–03/06/2026). Relevante para o Membro 2 montar `fato_contrato` na Fase 2.

| Campo API | Campo Postgres | Confiabilidade | Observação |
|---|---|---|---|
| `cod_gestora` | `empenhos.codigoug` / `unidade_gestora.codigo` | ✅ 100% match | Join confiável. `unidade_gestora` é versionada por `ano` — juntar sempre por `(codigo, ano)`. |
| `plain_cpf_cnpj_financiador` | `empenhos.codigocredor` | ⚠️ 96% match | Relação N:N (um credor pode ter vários contratos/empenhos) — não é join 1:1. |
| `num_spu` | `empenhos.codprocesso` | ❌ ~7,5% match | Mesmo formato de processo administrativo, mas baixa cobertura na amostra. Usar só como enriquecimento best-effort. |
| `num_contrato` / `plain_num_contrato` | `empenhos.codcontrato` | ❌ Sem correspondência | Domínios diferentes (provável código interno SIAFEM). Não usar sem achar um de-para real. |

> A própria API de contratos já retorna `calculated_valor_empenhado` e `calculated_valor_pago` por contrato, junto de `valor_contrato`/`valor_atualizado_concedente` — útil para métricas de execução financeira (% pago, % empenhado, detecção de pagamento acima do valor) sem depender do join fraco com `empenhos`/`ordem_bancaria_orcamentaria`.

## Status das tarefas (Fase 1 — Ingestão)

| Tarefa | Status | Nota |
|---|---|---|
| Extrair `empenhos` do PostgreSQL para o HDFS | ✅ Concluída | Validado ponta a ponta no HDFS real (5.313 registros na amostra de teste) |
| Extrair `ordem_bancaria_orcamentaria` do PostgreSQL para o HDFS | ✅ Concluída | Validado ponta a ponta no HDFS real (7.164 registros na amostra de teste) |
| Extração paginada da API de contratos | ✅ Concluída | Testada com 740 contratos reais extraídos |
| Inspecionar `total_pages` e controlar sleep/rate limit | ✅ Concluída | Implementado no mesmo módulo da extração da API |
| Extração incremental automática (watermark) | ⏳ Pendente | Hoje o período é passado manualmente via `--inicio`/`--fim`; falta persistir a última data processada (ex: Variable do Airflow, a ser criada junto da DAG do Membro 1) |

---
Ceará Transparente — Pipeline de Dados e IA · Documentação de ingestão (Membro 3)
