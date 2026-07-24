# Dicionário de Dados

Última atualização: 19/07/2026.

Três seções, com status diferente:

- **Bronze** — schema real, confirmado contra a fonte (arquivos gravados no HDFS/local hoje).
- **Mapeamento Bronze → Gold e regras de normalização** — **proposta** (tarefa 10, adiantada por
  Jaime em 19/07 para destravar a Fase 2 — ver [`checklist`](../docs/checklist.md) interno).
  Marcada como `[proposto — a validar]`: são regras iniciais grounded no schema real já
  documentado abaixo, mas a decisão final é do Carlos/Fernanda (donos da tarefa 10) antes de
  virar código nas tarefas 13/14/15.
- **Gold (DW)** — **implementado e carregado com dado real** em 19/07/2026 (tarefas 15/16,
  reatribuídas a Jaime na redistribuição do dia — ver [`checklist`](../docs/checklist.md)
  interno). DDL em [`sql/ddl_dw.sql`](../sql/ddl_dw.sql), rodando em Postgres dedicado
  (`datalab_postgres_dw`, porta 5434). Contagens reais da carga completa: `dim_credor` 10.612,
  `dim_orgao` 5.011, `dim_modalidade` 21, `dim_tempo` 1.449, `fato_contrato` 215.402,
  `fato_empenho` 1.376.379.

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

| Campo | Tipo na fonte | Observação |
|---|---|---|
| `codigo`, `ano` | `text`, `integer` | Chave de negócio (versionada por ano) |
| `titulo` | `text` | Nome do órgão |
| `sigla` | `text` | Sigla |
| `cnpj` | `text` | CNPJ do órgão |
| `tipoadministracao`, `tipoug` | `text` | Classificação administrativa |
| `codigopoder`, `nomepoder` | `text` | Poder (Executivo/Legislativo/Judiciário) |
| `codigouf`, `nomemunicipio` | `text` | Localização |

**Colunas mínimas exigidas:** `codigo`, `ano`.

### `empenhos` / `ordem_bancaria_orcamentaria` — colunas confirmadas adicionais

Confirmado contra `information_schema.columns` do Postgres de origem em 19/07/2026 (destravou
as tarefas 15/16/17): `valor` (`numeric`) é o campo de valor do empenho, `modalidade` (`text`) a
classificação — nenhum dos dois tinha sido confirmado 1:1 até então.

---

## Mapeamento Bronze → Gold e regras de normalização `[proposto — a validar]`

Cobre o que faltava pra tarefa 10 destravar as tarefas 13 (conversão de datas/normalização
CNPJ-CPF) e 15 (modelagem do DW): schema real já estava documentado acima, mas não havia
regra de transformação explícita nem de-para campo a campo.

### Datas — formato-alvo `DATE` (ISO 8601, sem hora)

| Fonte | Formato original | Regra de conversão |
|---|---|---|
| API (`data_assinatura`, `data_inicio`, `data_termino`, `data_publicacao_portal`, `data_rescisao`) | `DD/MM/YYYY` (string) | `datetime.strptime(v, "%d/%m/%Y").date()` — mesma conversão que `api_extractor` já faz internamente para filtrar por `--inicio`/`--fim` (ver `README.md`), só que persistida na Silver em vez de descartada após o filtro |
| Postgres (`dataemissao`) | `TEXT` `'YYYY-MM-DD HH:MM:SS.mmm'` | Cortar nos 10 primeiros caracteres (`v[:10]`) e converter pra `DATE` — já é prefixo ISO 8601, não precisa parse de formato, só cast de tipo |
| Campos vazios/`None` (ex: `data_rescisao` em contrato não rescindido) | — | Manter `NULL` — não é dado ausente por erro, é ausência legítima (contrato ativo) |

`dim_tempo` é alimentada pelo conjunto de datas distintas resultante (não por uma fonte só).

### CNPJ/CPF — formato-alvo: apenas dígitos (sem máscara), `VARCHAR`

| Campo origem | Observação | Regra |
|---|---|---|
| `contratos.plain_cpf_cnpj_financiador` | API já retorna sem máscara | Usar direto como candidato principal |
| `contratos.cpf_cnpj_financiador` | API retorna com máscara (`000.000.000-00` / `00.000.000/0000-00`) | Fallback só se `plain_...` vier vazio: `re.sub(r"\D", "", v)` |
| `empenhos.codigocredor` | Já é o campo de junção usado hoje (~96% match com o financiador do contrato) | Mesma normalização (só dígitos) antes de comparar/juntar |
| Resultado com 11 dígitos | CPF | `dim_credor.tipo = "PF"` |
| Resultado com 14 dígitos | CNPJ | `dim_credor.tipo = "PJ"` |
| Resultado com outro tamanho após normalização | Dado inconsistente na fonte | **Não descartar a linha** — gravar `cnpj_cpf` normalizado mesmo assim e marcar `tipo = "INVALIDO"` para não perder o fato financeiro; validação de dígito verificador fica como melhoria futura, não bloqueante pra Fase 2 |

### Deduplicação — `empenhos` / `ordem_bancaria_orcamentaria` / `contratos`

Nenhuma das duas primeiras tem PK real no banco de origem (ver seção Bronze acima). Chave
lógica: **`(id, ano)`** para `empenhos`/`ordem_bancaria_orcamentaria`, **`id`** para
`contratos` (esse tem id único de verdade na API). `silver_transformer.py` dedupa dentro de
uma mesma execução (`data_extracao`).

**Achado real (19/07/2026, carga do DW):** dedup *entre* execuções diferentes da Silver não
existia — quando duas execuções da DAG 2 processam janelas incrementais sobrepostas (ex: um
backfill manual reprocessando um período já coberto por outra execução), o mesmo registro
aparece duas vezes no histórico consolidado da Silver. Isso quebrou a carga do DW
(`psycopg2.errors.CardinalityViolation: ON CONFLICT DO UPDATE command cannot affect row a
second time`) até o `dw_loader.py` passar a dedupar de novo por `(id, ano)`/`id` antes de
montar os fatos.

**Resolvido na arquitetura lakehouse (Spark + Iceberg):** a Silver passou a ser tabela
Iceberg gravada por `src/spark_jobs/silver_job.py` com **`MERGE INTO`** pela mesma chave
lógica (`(id, ano)` / `id` / `(codigo, ano)`) — o dedup agora acontece **entre execuções**,
na própria Silver, não só na borda do DW. O caminho pandas legado (`silver_transformer.py`,
backend `local`/dev) mantém a limitação anterior (dedup só dentro de uma `data_extracao`).
Ver [`lakehouse-spark-iceberg.md`](lakehouse-spark-iceberg.md).
Também descoberto na mesma carga: ~120 mil datas de `contratos` (campos além de
`data_assinatura`, ex. `data_termino`/`data_rescisao`) vêm em `ISO 8601` com timezone
(`2026-02-22T00:00:00.000-03:00`) em vez de `DD/MM/YYYY` — `silver_transformer.py` já trata
sem quebrar (mantém valor original com aviso), mas essas datas específicas não ficam
normalizadas.

### De-para por tabela Gold

Ver coluna **"Origem (Bronze)"** adicionada em cada tabela da seção Gold abaixo.

---

## Camada Gold — Data Warehouse `[implementado — schema real, sql/ddl_dw.sql]`

Modelo dimensional (estrela), schema `dw` no Postgres `datalab_postgres_dw` (porta 5434,
container dedicado). DDL completo em [`sql/ddl_dw.sql`](../sql/ddl_dw.sql); carga via
`src/loaders/dw_loader.py` (`python -m src.loaders.dw_loader`), idempotente (upsert por chave
de negócio). Contagens da carga completa (19/07/2026) entre parênteses.

> **Evolução para lakehouse (Gold em Iceberg via dbt-trino):** a Gold passou a ser
> construída de forma declarativa por dbt-trino e materializada como **tabelas
> Iceberg** `iceberg.gold.*` no HDFS (mesmo esquema estrela), servida pelo Trino —
> ver [`gold-dbt-trino.md`](gold-dbt-trino.md). Duas diferenças de modelagem em
> relação ao schema Postgres descrito abaixo: (1) as **surrogate keys viram hash
> `md5`** da chave natural (Iceberg não tem `BIGSERIAL`), e (2) as **constraints
> (PK/FK/unique) viram testes dbt** (`schema.yml`). A regra de negócio de cada
> dim/fato é a mesma de `dw_loader.py`, que fica como referência/legado (o Postgres
> DW vira espelho opcional). As colunas abaixo seguem válidas como contrato lógico.

### `dim_credor` (SCD2) — 10.612 linhas

| Coluna | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_credor` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `cnpj_cpf` | `VARCHAR(14)` | Chave de negócio | `contratos.plain_cpf_cnpj_financiador` (fallback: `cpf_cnpj_financiador` normalizado) |
| `nome` | `VARCHAR(255)` | Razão social | `contratos.descricao_nome_credor` |
| `tipo` | `VARCHAR(10)` | `PF`/`PJ`/`INVALIDO` | derivado do tamanho do `cnpj_cpf` normalizado |
| `historico_infringement` | `BOOLEAN` | Histórico de infração (`infringement_status > 0`) | `contratos.infringement_status` |
| `valido_de`, `valido_ate`, `versao_atual` | `TIMESTAMP`/`TIMESTAMP`/`BOOLEAN` | Controle SCD2 | gerado |

**Limitação conhecida:** o loader hoje só insere uma versão nova quando não existe nenhuma
"atual" pro mesmo `cnpj_cpf` — não detecta troca de razão social pra fechar a versão antiga
(`valido_ate`) e abrir uma nova automaticamente. Melhoria futura, não bloqueante.

### `dim_orgao` — 5.011 linhas

| Coluna | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_orgao` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `codigo`, `ano` | `VARCHAR`, `INT` | Chave de negócio | `unidade_gestora.codigo`, `.ano` |
| `nome`, `sigla`, `cnpj` | `VARCHAR` | Identificação | `unidade_gestora.titulo`, `.sigla`, `.cnpj` |
| `tipo_administracao`, `tipo_ug` | `VARCHAR` | Classificação | `unidade_gestora.tipoadministracao`, `.tipoug` |
| `codigo_poder`, `nome_poder` | `VARCHAR` | Poder (subst. o `esfera` da proposta original — sem correspondência real na fonte) | `unidade_gestora.codigopoder`, `.nomepoder` |
| `codigo_uf`, `nome_municipio` | `VARCHAR` | Localização | `unidade_gestora.codigouf`, `.nomemunicipio` |

### `dim_modalidade` — 21 linhas

| Coluna | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_modalidade` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `descricao_modalidade` | `VARCHAR` UNIQUE | Pregão eletrônico, dispensa, inexigibilidade etc. | `contratos.descricao_modalidade` |

### `dim_tempo` — 1.449 linhas

| Coluna | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_tempo` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `data` | `DATE` UNIQUE | — | conjunto de datas normalizadas de `contratos.data_assinatura` + `empenhos.dataemissao` |
| `ano`, `trimestre`, `mes`, `dia_semana` | `INT` | Derivados de `data` | calculado |

### `fato_contrato` — 215.402 linhas, particionada por `ano` (RANGE, 2022–2026 + `DEFAULT`)

| Coluna | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_fato_contrato`, `ano` | `BIGSERIAL`, `INT` | PK composta (exigência do Postgres p/ tabela particionada) | gerado / ano de `data_assinatura` |
| `id_contrato_origem` | `VARCHAR` | Chave de negócio p/ recarga idempotente (`UNIQUE` com `ano`) | `contratos.id` |
| `sk_credor`, `sk_orgao`, `sk_modalidade`, `sk_tempo` | `BIGINT` FK | Chaves das dimensões | lookup pelas chaves de negócio |
| `valor_contrato`, `valor_pago`, `valor_empenhado` | `NUMERIC(15,2)` | Valores financeiros | `contratos.valor_contrato`, `calculated_valor_pago`, `calculated_valor_empenhado` — API já calcula essas duas últimas, preferidas em vez de recalcular via join fraco com `empenhos`/`ordem_bancaria_orcamentaria` |
| `status` | `VARCHAR` | `descricao_situacao` | `contratos.descricao_situacao` |
| `flag_emergency` | `BOOLEAN` | Contrato de emergência | `contratos.emergency` |
| `score_anomalia` | `NUMERIC(5,4)` | `NULL` até a Fase 3 | gravado pela tarefa 24 |

Cobertura real do join: 211/215.402 sem `sk_orgao` (0,1%), 11/215.402 sem `sk_credor` — resto
100% resolvido.

### `fato_empenho` — 1.376.379 linhas (sem particionamento — não exigido pelo checklist)

| Coluna | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_fato_empenho` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `id_empenho_origem`, `ano` | `BIGINT`, `INT` | Chave de negócio (`UNIQUE` composta — `empenhos` não tem PK real, `id` sozinho se repete entre anos) | `empenhos.id`, `.ano` |
| `sk_orgao`, `sk_tempo` | `BIGINT` FK | Chaves das dimensões | lookup por `(codigoug, ano)` → `dim_orgao`, `dataemissao` → `dim_tempo` |
| `cod_contrato` | `VARCHAR` | Liga com `fato_contrato` (best-effort) | `empenhos.codprocesso` — **atenção:** só ~7,5% de match real contra `contratos.num_spu` (ver README); sem FK obrigatória |
| `valor` | `NUMERIC(15,2)` | Valor do empenho | `empenhos.valor` (confirmado real, `numeric`) |
| `modalidade` | `VARCHAR` | Classificação | `empenhos.modalidade` (confirmado real, `text`) |

Cobertura real do join: 0 registros sem `sk_orgao` — 100% resolvido.
