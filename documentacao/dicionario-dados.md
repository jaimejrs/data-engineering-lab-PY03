# Dicionário de Dados

Última atualização: 19/07/2026.

Três seções, com status diferente:

- **Bronze** — schema real, confirmado contra a fonte (arquivos gravados no HDFS/local hoje).
- **Mapeamento Bronze → Gold e regras de normalização** — **proposta** (tarefa 10, adiantada por
  Jaime em 19/07 para destravar a Fase 2 — ver [`checklist`](../docs/checklist.md) interno).
  Marcada como `[proposto — a validar]`: são regras iniciais grounded no schema real já
  documentado abaixo, mas a decisão final é do Carlos/Fernanda (donos da tarefa 10) antes de
  virar código nas tarefas 13/14/15.
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

### Deduplicação — `empenhos` / `ordem_bancaria_orcamentaria`

Nenhuma das duas tem PK real no banco de origem (ver seção Bronze acima). Chave lógica
proposta para dedup na Silver: **`(id, ano)`**. Como a extração é incremental por
`dataemissao` e cada `data_extracao` é uma partição própria, duplicata só pode ocorrer entre
`data_extracao`s diferentes (ex: reprocessamento manual de uma janela já extraída), nunca
dentro da mesma partição. Regra: ao consolidar Bronze → Silver, manter a ocorrência da
**`data_extracao` mais recente** para cada `(id, ano)` (mais recente = dado mais atualizado
na fonte, não apenas "mais nova gravação").

### De-para por tabela Gold

Ver coluna **"Origem (Bronze)"** adicionada em cada tabela da seção Gold abaixo.

---

## Camada Gold — Data Warehouse `[planejado — pendente tarefas 15/16]`

Modelo dimensional (estrela) proposto em `Trabalho Final.pdf` §4.2 (DAG 3 — Carga Gold).
Nenhuma dessas tabelas existe hoje no PostgreSQL DW; layout final e tipos exatos ficam
a cargo de quem implementar as tarefas 15 (modelagem) e 16 (DDL).

### `dim_credor` (SCD2)

| Coluna proposta | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_credor` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `cnpj_cpf` | `VARCHAR` | Chave de negócio | `contratos.plain_cpf_cnpj_financiador` (fallback: `cpf_cnpj_financiador` normalizado — ver regra de CNPJ/CPF acima) |
| `nome` | `VARCHAR` | Razão social | `contratos.descricao_nome_credor` |
| `tipo` | `VARCHAR` | PJ / PF | derivado do tamanho do `cnpj_cpf` normalizado (11 = PF, 14 = PJ, ver regra acima) |
| `historico_infringement` | `BOOLEAN`/`INT` | Histórico de infração (`infringement_status > 0`) | `contratos.infringement_status` |
| `valido_de`, `valido_ate`, `versao_atual` | `TIMESTAMP`/`BOOLEAN` | Controle SCD2 — rastreia mudança de razão social | gerado (comparação de `nome` entre execuções) |

### `dim_orgao`

| Coluna proposta | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_orgao` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `codigo`, `ano` | — | Chave de negócio (`unidade_gestora`) | `unidade_gestora.codigo`, `unidade_gestora.ano` |
| `nome`, `cnpj`, `tipo_administracao`, `esfera` | — | Atributos descritivos | `unidade_gestora.*` (campos ainda não confirmados 1:1 — `unidade_gestora` não foi detalhada campo a campo nesta seção Bronze; conferir schema real antes de codar a 15) |

### `dim_modalidade`

| Coluna proposta | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_modalidade` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `descricao_modalidade` | `VARCHAR` | Pregão eletrônico, dispensa, inexigibilidade etc. | `contratos.descricao_modalidade` |

### `dim_tempo`

| Coluna proposta | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_tempo` | `BIGSERIAL` (PK) | Surrogate key | gerado |
| `data`, `ano`, `trimestre`, `mes`, `dia_semana` | `DATE`/`INT` | Atributos de data completa | conjunto de datas normalizadas (ver regra de datas acima) vindas de `contratos.data_assinatura` e `empenhos`/`ordem_bancaria_orcamentaria.dataemissao` |

### `fato_contrato` (particionada por ano)

| Coluna proposta | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_credor`, `sk_orgao`, `sk_modalidade`, `sk_tempo` | FK | Chaves das dimensões | lookup pelas chaves de negócio acima |
| `valor_contrato`, `valor_pago`, `valor_empenhado` | `NUMERIC` | Valores financeiros | `contratos.valor_contrato`, `calculated_valor_pago`, `calculated_valor_empenhado` — API já calcula essas duas últimas, preferir a elas em vez de recalcular via join com `empenhos`/`ordem_bancaria_orcamentaria` (ver seção "Chaves de junção" do `README.md` — join fraco/N:N) |
| `status` | `VARCHAR` | `descricao_situacao` | `contratos.descricao_situacao` |
| `flag_emergency` | `BOOLEAN` | Contrato de emergência | `contratos.emergency` |
| `score_anomalia` | `NUMERIC(0-1)` | Preenchido pelo Modelo 1 (Isolation Forest) — Fase 3 | — (não vem da Bronze, gravado depois pela tarefa 24) |

### `fato_empenho`

| Coluna proposta | Tipo | Descrição | Origem (Bronze) |
|---|---|---|---|
| `sk_orgao`, `sk_tempo` | FK | Chaves das dimensões | lookup por `empenhos.codigoug`+`ano` (→ `dim_orgao`) e `dataemissao` normalizada (→ `dim_tempo`) |
| `cod_contrato` | — | Liga com `fato_contrato` | `empenhos.codprocesso` — **atenção:** README registra só ~7,5% de match real contra `contratos.num_spu` na amostra validada; usar como enriquecimento best-effort, não como FK obrigatória |
| `valor`, `modalidade` | — | Valor do empenho e classificação | `empenhos.*` (campo de valor específico ainda não confirmado nesta seção Bronze — conferir schema real antes de codar a 16) |
