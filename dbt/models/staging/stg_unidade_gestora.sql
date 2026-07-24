-- unidade_gestora é versionada por ano — join sempre por (codigo, ano).
select
    codigo,
    ano,
    titulo,
    sigla,
    cnpj,
    tipoadministracao,
    tipoug,
    codigopoder,
    nomepoder,
    codigouf,
    nomemunicipio
from {{ source('silver', 'unidade_gestora') }}
