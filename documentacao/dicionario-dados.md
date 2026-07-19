# Dicionário de Dados

Última atualização: 18/07/2026.

Duas seções, com status diferente:

- **Bronze** — schema real, confirmado contra a fonte (arquivos gravados no HDFS/local hoje).
- **Gold (DW)** — **proposta** baseada na seção 4.2 do `Trabalho Final.pdf`. As tabelas
  ainda **não existem** (dependem das tarefas 15/16 do Carlos — ver
  [`checklist`](../docs/checklist.md) interno). Marcadas como `[planejado]`.

---

## Camada Bronze (HDFS, formato JSON — bruto, sem transformação)

### `contratos/` — API REST Ceará Transparente

Particionado por `data_extracao=YYYY-MM-DD/page_NNNN.json`. Um objeto JSON por registro,
sem alteração de tipos (tudo como a API retorna).

| Campo | Origem | Observação |
|---|---|---|
| `id`, `isn_sic`, `num_spu`, `num_contrato`, `cod_concedente`, `cod_gestora` | API | Identificação do contrato |
| `isn_parte_origem`, `isn_parte_destino`, `cpf_cnpj_financiador`, `descricao_nome_credor` | API | Partes envolvidas |
| `valor_contrato`, `valor_original_concedente`, `valor_atualizado_concedente`, `calculated_valor_pago`, `calculated_valor_empenhado` | API | Valores financeiros |
| `data_assinatura`, `data_inicio`, `data_termino`, `data_publicacao_portal`, `data_rescisao` | API | Datas — formato `DD/MM/YYYY` na fonte |
| `descricao_modalidade`, `descricao_tipo`, `tipo_objeto`, `descricao_objeto` | API | Classificação |
| `descricao_situacao`, `infringement_status`, `accountability_status`, `emergency` | API | Status |

**Colunas mínimas exigidas pelo `bronze_validator`:** `id`, `num_contrato`, `valor_contrato`, `data_assinatura`, `cod_gestora`.

### `empenhos/` — PostgreSQL de origem

Particionado por `ano=YYYY/` (histórico completo) e `data_extracao=YYYY-MM-DD/chunk_NNNN.json`
(incremental, watermark `bronze_last_data_extracao`). Sem PK declarada no banco de origem
— **não assumir unicidade de `id` sem deduplicação a jusante** (Silver).

| Campo | Tipo na fonte | Observação |
|---|---|---|
| `id`, `ano` | — | Chave lógica (não é PK real no banco) |
| `dataemissao` | `TEXT` (`'YYYY-MM-DD HH:MM:SS.mmm'`) | Coluna usada para filtro incremental — comparação lexicográfica funciona por ser prefixo ISO 8601 |
| `codigoug` | — | Junta com `unidade_gestora.codigo` (join por `codigo, ano`) |
| `codigocredor` | — | Junta com `contratos.plain_cpf_cnpj_financiador` (N:N, ~96% match) |
| `codprocesso` | — | Junta com `contratos.num_spu` (baixa cobertura, ~7,5% match — usar só como enriquecimento best-effort) |
| `codcontrato` | — | Sem correspondência confiável com `contratos.num_contrato` |

**Colunas mínimas exigidas:** `id`, `ano`, `dataemissao`.

### `ordem_bancaria_orcamentaria/` — PostgreSQL de origem

Mesma estrutura de particionamento e mesmas particularidades de `empenhos` (sem PK,
`dataemissao` como `TEXT`, filtro incremental pela mesma coluna). Registra o pagamento
efetivo ao credor (dados bancários).

**Colunas mínimas exigidas:** `id`, `ano`, `dataemissao`.

### `unidade_gestora/` — PostgreSQL de origem

Tabela de referência — extraída por completo a cada execução (sem filtro de data).
Versionada por `ano`: **sempre juntar por `(codigo, ano)`**, nunca só por `codigo`.

**Colunas mínimas exigidas:** `codigo`, `ano`.

---

## Camada Gold — Data Warehouse `[planejado — pendente tarefas 15/16]`

Modelo dimensional (estrela) proposto em `Trabalho Final.pdf` §4.2 (DAG 3 — Carga Gold).
Nenhuma dessas tabelas existe hoje no PostgreSQL DW; layout final e tipos exatos ficam
a cargo de quem implementar as tarefas 15 (modelagem) e 16 (DDL).

### `dim_credor` (SCD2)

| Coluna proposta | Tipo | Descrição |
|---|---|---|
| `sk_credor` | `BIGSERIAL` (PK) | Surrogate key |
| `cnpj_cpf` | `VARCHAR` | Chave de negócio |
| `nome` | `VARCHAR` | Razão social |
| `tipo` | `VARCHAR` | PJ / PF |
| `historico_infringement` | `BOOLEAN`/`INT` | Histórico de infração (`infringement_status > 0`) |
| `valido_de`, `valido_ate`, `versao_atual` | `TIMESTAMP`/`BOOLEAN` | Controle SCD2 — rastreia mudança de razão social |

### `dim_orgao`

| Coluna proposta | Tipo | Descrição |
|---|---|---|
| `sk_orgao` | `BIGSERIAL` (PK) | Surrogate key |
| `codigo`, `ano` | — | Chave de negócio (`unidade_gestora`) |
| `nome`, `cnpj`, `tipo_administracao`, `esfera` | — | Atributos descritivos |

### `dim_modalidade`

| Coluna proposta | Tipo | Descrição |
|---|---|---|
| `sk_modalidade` | `BIGSERIAL` (PK) | Surrogate key |
| `descricao_modalidade` | `VARCHAR` | Pregão eletrônico, dispensa, inexigibilidade etc. |

### `dim_tempo`

| Coluna proposta | Tipo | Descrição |
|---|---|---|
| `sk_tempo` | `BIGSERIAL` (PK) | Surrogate key |
| `data`, `ano`, `trimestre`, `mes`, `dia_semana` | `DATE`/`INT` | Atributos de data completa |

### `fato_contrato` (particionada por ano)

| Coluna proposta | Tipo | Descrição |
|---|---|---|
| `sk_credor`, `sk_orgao`, `sk_modalidade`, `sk_tempo` | FK | Chaves das dimensões |
| `valor_contrato`, `valor_pago`, `valor_empenhado` | `NUMERIC` | Valores financeiros |
| `status` | `VARCHAR` | `descricao_situacao` |
| `flag_emergency` | `BOOLEAN` | Contrato de emergência |
| `score_anomalia` | `NUMERIC(0-1)` | Preenchido pelo Modelo 1 (Isolation Forest) — Fase 3 |

### `fato_empenho`

| Coluna proposta | Tipo | Descrição |
|---|---|---|
| `sk_orgao`, `sk_tempo` | FK | Chaves das dimensões |
| `cod_contrato` | — | Liga com `fato_contrato` |
| `valor`, `modalidade` | — | Valor do empenho e classificação |
