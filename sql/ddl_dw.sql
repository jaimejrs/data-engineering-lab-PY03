-- DDL do Data Warehouse (Gold) — modelo dimensional em estrela.
--
-- Tarefas 15/16 do checklist. Campos confirmados contra:
--  - schema real do Postgres de origem (information_schema.columns, 19/07/2026)
--    para unidade_gestora/empenhos/ordem_bancaria_orcamentaria;
--  - schema real da API/Bronze/Silver (documentacao/dicionario-dados.md) para contratos.
--
-- Idempotente (IF NOT EXISTS) -- seguro rodar de novo sem apagar dado existente.

CREATE SCHEMA IF NOT EXISTS dw;

-- dim_credor: SCD2 simplificado. O loader hoje so insere versao nova quando
-- nao existe ainda uma linha "atual" pro mesmo cnpj_cpf -- deteccao de mudanca
-- de nome (reabertura de versao) fica como melhoria futura, ver dw_loader.py.
CREATE TABLE IF NOT EXISTS dw.dim_credor (
    sk_credor               BIGSERIAL PRIMARY KEY,
    cnpj_cpf                VARCHAR(14) NOT NULL,
    nome                    VARCHAR(255),
    tipo                    VARCHAR(10) NOT NULL CHECK (tipo IN ('PF', 'PJ', 'INVALIDO')),
    historico_infringement  BOOLEAN NOT NULL DEFAULT FALSE,
    valido_de               TIMESTAMP NOT NULL DEFAULT now(),
    valido_ate              TIMESTAMP,
    versao_atual            BOOLEAN NOT NULL DEFAULT TRUE
);

-- Unicidade so entre as versoes "atuais" -- permite manter historico (valido_ate
-- preenchido, versao_atual=false) sem violar a constraint quando uma nova
-- versao for aberta no futuro.
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_credor_atual
    ON dw.dim_credor (cnpj_cpf) WHERE versao_atual;

-- dim_orgao: campos reais de unidade_gestora (nao existe "esfera" na fonte --
-- nomepoder/codigopoder cobre esse conceito de forma mais fiel).
CREATE TABLE IF NOT EXISTS dw.dim_orgao (
    sk_orgao            BIGSERIAL PRIMARY KEY,
    codigo              VARCHAR NOT NULL,
    ano                 INT NOT NULL,
    nome                VARCHAR,
    sigla               VARCHAR,
    cnpj                VARCHAR,
    tipo_administracao  VARCHAR,
    tipo_ug             VARCHAR,
    codigo_poder        VARCHAR,
    nome_poder          VARCHAR,
    codigo_uf           VARCHAR,
    nome_municipio      VARCHAR,
    UNIQUE (codigo, ano)
);

CREATE TABLE IF NOT EXISTS dw.dim_modalidade (
    sk_modalidade       BIGSERIAL PRIMARY KEY,
    descricao_modalidade VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    sk_tempo    BIGSERIAL PRIMARY KEY,
    data        DATE NOT NULL UNIQUE,
    ano         INT NOT NULL,
    trimestre   INT NOT NULL,
    mes         INT NOT NULL,
    dia_semana  INT NOT NULL
);

-- fato_contrato: particionada por ano (RANGE). PK precisa incluir a coluna de
-- particionamento (limitacao do Postgres pra tabelas particionadas).
-- id_contrato_origem + ano formam a chave de negocio, usada pelo loader pra
-- recarga idempotente via ON CONFLICT.
CREATE TABLE IF NOT EXISTS dw.fato_contrato (
    sk_fato_contrato    BIGSERIAL,
    ano                 INT NOT NULL,
    id_contrato_origem  VARCHAR NOT NULL,
    sk_credor           BIGINT REFERENCES dw.dim_credor (sk_credor),
    sk_orgao            BIGINT REFERENCES dw.dim_orgao (sk_orgao),
    sk_modalidade       BIGINT REFERENCES dw.dim_modalidade (sk_modalidade),
    sk_tempo            BIGINT REFERENCES dw.dim_tempo (sk_tempo),
    valor_contrato      NUMERIC(15, 2),
    valor_pago          NUMERIC(15, 2),
    valor_empenhado     NUMERIC(15, 2),
    status              VARCHAR,
    flag_emergency      BOOLEAN NOT NULL DEFAULT FALSE,
    score_anomalia      NUMERIC(5, 4),  -- preenchido pela tarefa 24 (Fase 3), NULL ate la
    PRIMARY KEY (sk_fato_contrato, ano),
    UNIQUE (id_contrato_origem, ano)
) PARTITION BY RANGE (ano);

CREATE TABLE IF NOT EXISTS dw.fato_contrato_2022 PARTITION OF dw.fato_contrato FOR VALUES FROM (2022) TO (2023);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2023 PARTITION OF dw.fato_contrato FOR VALUES FROM (2023) TO (2024);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2024 PARTITION OF dw.fato_contrato FOR VALUES FROM (2024) TO (2025);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2025 PARTITION OF dw.fato_contrato FOR VALUES FROM (2025) TO (2026);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_2026 PARTITION OF dw.fato_contrato FOR VALUES FROM (2026) TO (2027);
CREATE TABLE IF NOT EXISTS dw.fato_contrato_default PARTITION OF dw.fato_contrato DEFAULT;

-- fato_empenho: sem exigencia de particionamento no checklist (so
-- fato_contrato pede). cod_contrato liga com fato_contrato.id_contrato_origem
-- via codprocesso/num_spu -- README documenta so ~7,5% de match real na
-- amostra validada, por isso e' so uma coluna solta (enriquecimento
-- best-effort), sem FK obrigatoria.
--
-- Chave de negocio e' (id_empenho_origem, ano), nao id_empenho_origem sozinho:
-- a fonte (empenhos) nao tem PK real, e o "id" se repete em anos diferentes
-- (documentado em README.md/dicionario-dados.md). UNIQUE so em
-- id_empenho_origem colapsaria registros distintos de anos diferentes.
CREATE TABLE IF NOT EXISTS dw.fato_empenho (
    sk_fato_empenho     BIGSERIAL PRIMARY KEY,
    id_empenho_origem   BIGINT NOT NULL,
    ano                 INT NOT NULL,
    sk_orgao            BIGINT REFERENCES dw.dim_orgao (sk_orgao),
    sk_tempo            BIGINT REFERENCES dw.dim_tempo (sk_tempo),
    cod_contrato        VARCHAR,
    valor               NUMERIC(15, 2),
    modalidade          VARCHAR,
    UNIQUE (id_empenho_origem, ano)
);
