# Testes dbt — catálogo completo

Última atualização: 01/08/2026.

Lista cada um dos **62 testes** que rodam a cada `dbt build` (Gold), organizados
pelas três origens onde são declarados. Complementa
[`gold-dbt-trino.md`](gold-dbt-trino.md) (arquitetura da camada) e
[`dicionario-dados.md`](dicionario-dados.md) (colunas e chaves de cada tabela).

Correção de histórico: uma versão anterior desta contagem citava "47
verificações" — desatualizado desde que `fato_ordem_bancaria` e os testes de
`ml`/`audit` foram adicionados. O número certo, hoje, é 62.

## Como rodar / conferir

```bash
cd dbt && dbt build     # roda modelos + os 62 testes (requer Trino no ar)
cd dbt && dbt test      # roda só os testes, sem reconstruir os modelos
```

Testes com severidade `error` (padrão) derrubam o `dbt build` se falharem —
o job `dbt-integration` do CI roda um subconjunto real contra um
Trino+Iceberg+Hive Metastore efêmero a cada push (ver
[`github-actions-cicd.md`](../stacks/github-actions-cicd.md), interno).
Testes com severidade `warn` (4 dos 10 testes singulares, listados abaixo)
nunca derrubam o build — servem para monitorar um cenário que pode ser
legítimo, não necessariamente um erro.

## 1. Testes genéricos nos modelos da Gold (`dbt/models/marts/schema.yml`) — 32

| Modelo | Coluna | Teste(s) |
|---|---|---|
| `dim_credor` | `sk_credor` | `not_null`, `unique` |
| `dim_credor` | `cnpj_cpf` | `not_null` |
| `dim_credor` | `tipo` | `accepted_values` (`PF`, `PJ`, `INVALIDO`) |
| `dim_orgao` | `sk_orgao` | `not_null`, `unique` |
| `dim_orgao` | `codigo` | `not_null` |
| `dim_orgao` | `ano` | `not_null` |
| `dim_modalidade` | `sk_modalidade` | `not_null`, `unique` |
| `dim_modalidade` | `descricao_modalidade` | `not_null` |
| `dim_tempo` | `sk_tempo` | `not_null`, `unique` |
| `dim_tempo` | `data` | `not_null` |
| `fato_contrato` | `id_contrato_origem` | `not_null` |
| `fato_contrato` | `ano` | `not_null` |
| `fato_contrato` | `sk_credor` | `relationships` → `dim_credor.sk_credor` |
| `fato_contrato` | `sk_orgao` | `relationships` → `dim_orgao.sk_orgao` |
| `fato_contrato` | `sk_modalidade` | `relationships` → `dim_modalidade.sk_modalidade` |
| `fato_contrato` | `sk_tempo` | `relationships` → `dim_tempo.sk_tempo` |
| `fato_empenho` | `id_empenho_origem` | `not_null` |
| `fato_empenho` | `ano` | `not_null` |
| `fato_empenho` | `sk_orgao` | `relationships` → `dim_orgao.sk_orgao` |
| `fato_empenho` | `sk_tempo` | `relationships` → `dim_tempo.sk_tempo` |
| `fato_ordem_bancaria` | `id_ob_origem` | `not_null` |
| `fato_ordem_bancaria` | `ano` | `not_null` |
| `fato_ordem_bancaria` | `sk_orgao` | `relationships` → `dim_orgao.sk_orgao` |
| `fato_ordem_bancaria` | `sk_tempo` | `relationships` → `dim_tempo.sk_tempo` |
| `gold_reconciliacao` | `tabela` | `not_null`, `unique` |
| `bronze_silver_reconciliacao` | `fonte` | `not_null`, `unique` |

Subtotal: 32 (soma de todas as linhas acima, contando cada teste listado numa célula separadamente).

## 2. Testes genéricos nas fontes (`dbt/models/sources.yml`) — 20

| Fonte (schema.tabela) | Coluna | Teste(s) |
|---|---|---|
| `silver.empenhos` | `id` | `not_null` |
| `silver.empenhos` | `ano` | `not_null` |
| `silver.ordem_bancaria_orcamentaria` | `id` | `not_null` |
| `silver.ordem_bancaria_orcamentaria` | `ano` | `not_null` |
| `silver.unidade_gestora` | `codigo` | `not_null` |
| `silver.unidade_gestora` | `ano` | `not_null` |
| `silver.contratos` | `id` | `not_null`, `unique` |
| `ml_scores.score_anomalia_contrato` | `id_contrato_origem` | `not_null` |
| `ml_scores.score_anomalia_contrato` | `ano` | `not_null` |
| `ml_scores.previsao_pagamento_orgao` | `codigo_orgao` | `not_null` |
| `ml_scores.previsao_pagamento_orgao` | `ano_previsto` | `not_null` |
| `ml_scores.previsao_pagamento_orgao` | `trimestre_previsto` | `not_null` |
| `ml_scores.relatorio_narrativo` | `gerado_em` | `not_null` |
| `audit.bronze_ingestao` | `fonte` | `not_null` |
| `audit.bronze_ingestao` | `data_extracao` | `not_null` |
| `audit.infra_metricas_containers` | `coletado_em` | `not_null` |
| `audit.infra_metricas_disco` | `coletado_em` | `not_null` |
| `audit.sessoes_ssh` | `timestamp_evento` | `not_null` |
| `audit.comandos_executados` | `timestamp_evento` | `not_null` |

Subtotal: 20.

## 3. Testes singulares (`dbt/tests/*.sql`) — 10

Regras de negócio que um teste genérico (`not_null`/`unique`/`relationships`/
`accepted_values`) não expressa — cada um é uma consulta SQL própria; o teste
falha se a consulta retornar alguma linha.

| Arquivo | Severidade | O que verifica |
|---|---|---|
| `assert_silver_chaves_unicas.sql` | `error` | Chave de negócio composta única nas 3 fontes Silver incrementais (`empenhos`, `ordem_bancaria_orcamentaria` por `id, ano`) — nenhuma tem PK real na origem; a garantia vem só do `MERGE INTO` do Spark. |
| `assert_fatos_chave_unica.sql` | `error` | Chave de negócio única nos 3 fatos da Gold (`fato_contrato`, `fato_empenho`, `fato_ordem_bancaria`). |
| `assert_bronze_ingestao_chave_unica.sql` | `error` | Chave `(fonte, data_extracao)` única em `audit.bronze_ingestao`. |
| `assert_ml_chaves_unicas.sql` | `error` | Chave `(id_contrato_origem, ano)` única em `score_anomalia_contrato`. |
| `assert_previsao_chave_unica.sql` | `error` | Chave `(codigo_orgao, ano_previsto, trimestre_previsto)` única em `previsao_pagamento_orgao`. |
| `assert_valores_nao_negativos.sql` | `error` | Nenhum valor monetário negativo (`valor_contrato`, `valor_pago`, `valor_empenhado`, etc.) nos fatos. |
| `assert_reconciliacao_bronze_silver.sql` | `warn` | A Silver deduplicada nunca deveria ter mais linhas do que o total já validado na Bronze (o `MERGE INTO` só remove duplicata, nunca cria linha do nada). |
| `assert_cobertura_gold_minima.sql` | `warn` | `pct_descartadas`/`pct_sem_orgao`/`pct_sem_tempo` acima de 1% na reconciliação Silver→Gold (limiar 10x acima do baseline observado, ~0,1%). |
| `assert_empenho_negativo_monitorado.sql` | `warn` | `valor_empenhado` negativo em `fato_contrato` — pode ser estorno/anulação contábil legítima, não necessariamente erro. |
| `assert_pagamento_dentro_do_contratado.sql` | `warn` | Contrato pago/empenhado a mais do que o dobro do `valor_contrato`, **excluindo** os casos já explicados por `valor_aditivo`/`valor_ajuste` (tolerância 1%). |

Subtotal: 10 (6 `error`, 4 `warn`).

## Total: 32 + 20 + 10 = 62
