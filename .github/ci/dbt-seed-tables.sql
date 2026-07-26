-- Passo 2/2 do seed do CI (job dbt-integration) — roda depois do
-- dbt-seed-schemas.sql e de um novo `chmod -R 777 /warehouse` (ver
-- .github/workflows/ci.yml). Sem isso, dbt build falha na primeira consulta
-- com "table not found", já que os modelos leem de source() e ninguém
-- populou a Silver nesse ambiente descartável.
--
-- Uma linha por fonte, com chaves que se conectam entre si (mesmo
-- codigo/ano/data) — o objetivo NÃO é validar volume/qualidade de produção
-- (isso continua sendo feito manualmente contra o Trino real, ver
-- docs/03-pendencias-e-melhorias.md), é garantir que o SQL dos modelos e dos
-- testes realmente EXECUTA (erro de sintaxe, coluna errada, join quebrado —
-- coisa que `dbt parse` não pega, porque nunca manda a consulta pro banco).

CREATE TABLE iceberg.silver.unidade_gestora (
    codigo varchar,
    ano integer,
    titulo varchar,
    sigla varchar,
    cnpj varchar,
    tipoadministracao varchar,
    tipoug varchar,
    codigopoder varchar,
    nomepoder varchar,
    codigouf varchar,
    nomemunicipio varchar
);
INSERT INTO iceberg.silver.unidade_gestora VALUES
    ('1001', 2024, 'SECRETARIA DE TESTE', 'SEC', '11111111000191', 'D', 'U', 'E', 'EXECUTIVO', 'CE', 'FORTALEZA');

CREATE TABLE iceberg.silver.contratos (
    id bigint,
    num_spu varchar,
    cnpj_cpf_normalizado varchar,
    tipo_credor varchar,
    descricao_nome_credor varchar,
    infringement_status bigint,
    descricao_modalidade varchar,
    data_assinatura date,
    cod_gestora varchar,
    valor_contrato decimal(15,2),
    calculated_valor_pago decimal(15,2),
    calculated_valor_empenhado decimal(15,2),
    calculated_valor_aditivo decimal(15,2),
    calculated_valor_ajuste decimal(15,2),
    descricao_situacao varchar,
    emergency boolean
);
INSERT INTO iceberg.silver.contratos VALUES
    (1, '001/2024', '12345678901', 'PF', 'FULANO DE TAL', 0, 'PREGAO',
     DATE '2024-01-15', '1001', 1000.00, 500.00, 800.00, 0.00, 0.00,
     'EM EXECUCAO - NORMAL', false);

CREATE TABLE iceberg.silver.empenhos (
    id bigint,
    ano integer,
    dataemissao date,
    codigoug varchar,
    codprocesso varchar,
    valor double,
    modalidade varchar
);
INSERT INTO iceberg.silver.empenhos VALUES
    (1, 2024, DATE '2024-01-20', '1001', '001/2024', 500.0, 'PREGAO');

CREATE TABLE iceberg.silver.ordem_bancaria_orcamentaria (
    id bigint,
    ano integer,
    codigo varchar,
    codigoug varchar,
    dataemissao date,
    datacancelamento varchar,
    valor double,
    codnatureza varchar,
    statusdocumento varchar,
    codprocesso varchar
);
INSERT INTO iceberg.silver.ordem_bancaria_orcamentaria VALUES
    (1, 2024, 'OB001', '1001', DATE '2024-01-25', NULL, 500.0, '339039', 'PAGO', '001/2024');

-- Tabelas escritas pelos scripts Python (models/, deploy/server-lakehouse/),
-- não pelo dbt — em produção já existem antes do 1º dbt build; aqui
-- precisam existir (vazias, tudo bem) só pra fonte declarada em sources.yml
-- não quebrar com "table not found". score_anomalia_contrato e
-- bronze_ingestao já são criadas pelo próprio on-run-start hook do dbt
-- (dbt_project.yml) — as outras 6 não têm hook (não são LEFT JOINed por
-- nenhum modelo, só testadas na fonte), então entram aqui.
CREATE TABLE iceberg.ml.previsao_pagamento_orgao (
    codigo_orgao varchar, nome_orgao varchar, ano_previsto integer, trimestre_previsto integer,
    valor_previsto_p10 double, valor_previsto_p50 double, valor_previsto_p90 double,
    model_version varchar, scored_at timestamp
);
CREATE TABLE iceberg.ml.relatorio_narrativo (
    gerado_em timestamp, llm_model varchar, num_contratos_anomalos bigint,
    num_orgaos_previstos bigint, conteudo_markdown varchar
);
CREATE TABLE iceberg.audit.infra_metricas_containers (
    container varchar, cpu_percent double, mem_usage_bytes bigint,
    mem_limit_bytes bigint, mem_percent double, coletado_em timestamp
);
CREATE TABLE iceberg.audit.infra_metricas_disco (
    ponto_montagem varchar, total_bytes bigint, usado_bytes bigint, pct_usado double, coletado_em timestamp
);
CREATE TABLE iceberg.audit.sessoes_ssh (
    evento varchar, usuario varchar, origem_ip varchar, sessao_pid varchar,
    timestamp_evento timestamp, coletado_em timestamp
);
CREATE TABLE iceberg.audit.comandos_executados (
    usuario varchar, executado_como varchar, comando varchar, executavel varchar,
    sessao_id varchar, tty varchar, sucesso boolean, timestamp_evento timestamp, coletado_em timestamp
);
