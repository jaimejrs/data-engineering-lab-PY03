# Egress IPv4 do servidor — procedimento aplicado (24/07/2026)

> Runbook de uma mudança já feita, mantido pra referência e pro caso de precisar
> refazer/reverter. Ver `documentacao/workaround-egress-ipv4-api.md` (causa raiz
> original) e `docs/03-pendencias-e-melhorias.md`.

## O que mudou

O `datalab-server` só tinha rota de saída IPv6 (`enp12s0` sem `dhcp4` no
netplan). Isso forçava dois workarounds: `extract_api` dependia de um relay TCP
numa máquina pessoal (`jotav15-1`), e o servidor não alcançava GitHub/PyPI.

**Fix na raiz:** habilitar `dhcp4: true` na interface cabeada
(`/etc/netplan/00-installer-config.yaml`) deu IPv4 real ao host. Confirmado:
API do Ceará Transparente, GitHub e PyPI todos respondendo `200` direto, sem
o relay. Rotas do Docker (bridges) e todos os containers continuaram intactos.

Com o host resolvido, o passo seguinte é remover o `extra_hosts` que força
`api-dados-abertos.cearatransparente.ce.gov.br` a resolver pro IP do relay
dentro dos containers do Airflow — sem isso, o `extract_api` continuaria
passando pelo relay mesmo com o host já tendo IPv4 de verdade.

## 1. Backup (sempre antes de mexer no compose de produção)

```bash
cd /home/dataadm
cp docker-compose.yml docker-compose.yml.bak-pre-ipv4-fix
```

## 2. Remover o `extra_hosts` — via `sed`, não heredoc

**Não cole um heredoc gigante reescrevendo o arquivo inteiro** — em terminais
sem bracketed-paste bem configurado, colar um bloco muito longo pode corromper
o conteúdo no meio (já aconteceu numa tentativa anterior: o nome do backup saiu
truncado no meio do paste). Prefira um comando curto e cirúrgico:

```bash
sed -i '/extra_hosts:/,+1d' docker-compose.yml
```

Isso apaga a linha `extra_hosts:` e a linha seguinte (o
`- "api-dados-abertos...`) em cada uma das duas ocorrências do arquivo
(`datalab_airflow_webserver` e `datalab_airflow_scheduler`).

## 3. Validar antes de aplicar

```bash
docker compose config > /dev/null && echo "OK: sintaxe válida"
diff docker-compose.yml.bak-pre-ipv4-fix docker-compose.yml
```

O `diff` deve mostrar **só** essas 4 linhas removidas (2 por serviço) — nada
mais. Se aparecer qualquer outra diferença, pare e restaure o backup (passo 6)
antes de continuar.

## 4. Recriar os containers do Airflow

```bash
docker compose up -d datalab_airflow_webserver datalab_airflow_scheduler
sleep 15
docker ps --format '{{.Names}} :: {{.Status}}' | grep airflow
```

## 5. Validar

```bash
# Sem erro de import nas DAGs
docker exec datalab_airflow_scheduler airflow dags list-import-errors

# O hostname da API deve resolver pelo DNS real agora, não mais pro IP do relay (100.101.236.119)
docker exec datalab_airflow_scheduler getent hosts api-dados-abertos.cearatransparente.ce.gov.br

# Teste real: extract_api direto, sem relay
docker exec datalab_airflow_scheduler airflow tasks test bronze_extract extract_api 2026-07-23
```

> **Nota (25/07/2026):** `tasks test` aqui é de baixo risco (escreve JSON
> particionado por página no HDFS, uma reexecução só sobrescreve os mesmos
> arquivos) — mas para tasks que escrevem via Trino, `tasks test` sobre SSH
> não é seguro (processo pode sobreviver a um timeout do cliente e virar
> escritor concorrente). Ver `docs/02-rotina-manutencao.md`.

## 6. Rollback (se algo der errado em qualquer passo)

```bash
cp /home/dataadm/docker-compose.yml.bak-pre-ipv4-fix /home/dataadm/docker-compose.yml
docker compose up -d datalab_airflow_webserver datalab_airflow_scheduler
```

## Depois de confirmado estável

Só depois de ver a cadeia `@daily` completa rodar sem o relay por pelo menos
um ciclo:

1. Desligar o `api_relay.py` em `jotav15-1` (não é mais necessário).
2. Atualizar `documentacao/workaround-egress-ipv4-api.md` marcando a causa raiz
   como resolvida.
3. Atualizar `docs/03-pendencias-e-melhorias.md` (item de egress IPv4×IPv6).
