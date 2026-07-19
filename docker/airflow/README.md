# Imagem customizada do Airflow

Estende `apache/airflow:2.9.1` instalando as dependências que as DAGs
(`src/extractors`, `src/validators`) precisam em runtime — ver `requirements.txt`
nesta pasta.

## Por que isso existe

Antes desta imagem, as dependências (`hdfs`, `psycopg2-binary`, `pandas` etc.)
eram instaladas manualmente dentro do container já em execução. Isso não
sobrevive a uma recriação do container (`docker compose up -d` troca a imagem
por uma limpa) — foi exatamente o que causou
`ModuleNotFoundError: No module named 'hdfs'` na DAG `bronze_extract` em
produção: o pacote nunca tinha sido instalado na imagem, só em um container que
já não existe mais.

## `network: host` no build

Se você builda esta imagem em um host cujos containers não têm rota de saída
para a internet (ex: bloqueio de IPv4 nas redes bridge do Docker — problema já
documentado em `docs/relatorio-orquestracao-dag1.md` para a task `extract_api`),
o `RUN pip install` dentro do build falha com
`Temporary failure in name resolution`, mesmo que o host tenha internet.
`docker-compose.yml` já define `network: host` para o build destes serviços,
o que faz o container de build usar a interface de rede do host diretamente e
contorna esse bloqueio — sem alterar nenhuma configuração do host ou dos
containers em execução.
