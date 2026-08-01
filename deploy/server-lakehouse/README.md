# Overlay lakehouse — deploy no servidor do curso (`datalab-server`)

Estes arquivos reproduzem o deploy **aditivo** do lakehouse (Hive Metastore +
Spark + Trino + dbt) feito no servidor compartilhado do curso, cuja topologia
**difere** do `docker-compose.yml` da raiz (que é o stack "reprodutível" autônomo).

## Por que um overlay separado

O servidor **não** roda o compose da raiz. Ele tem uma topologia bespoke:

| | Raiz do repo | Servidor (`datalab-server`) |
|---|---|---|
| HDFS | serviços `namenode`+`datanode` (`hdfs://namenode:9000`) | container único `aula_hadoop`, acessado como **`hdfs://hadoop:9000`** |
| Rede Docker | própria do compose | **`dataadm_default`** (externa, já existente) |
| Postgres metadados | `postgres` | `postgres` (container `datalab_postgres`) |
| Postgres DW | `postgres_dw` | container `datalab_postgres_dw` |
| Código Airflow | montado do repo | `/home/dataadm/airflow/{dags,src,sql}` (sync via `git pull`+`rsync`, ver abaixo) |

Por isso `docker-compose.yml` aqui usa `network: default → external: dataadm_default`
e todas as configs apontam para `hadoop`. `HADOOP_USER_NAME=root` é setado no Spark
e no Trino (o HDFS `aula_hadoop` roda como root/superusuário; sem isso, escrever no
`/warehouse` falha por permissão).

## Conteúdo

- `docker-compose.yml` — overlay: `hive-metastore`, `spark-master`, `spark-worker`,
  `trino`, `dbt` (profile `tools`), todos em `dataadm_default`.
- `conf/hive-site.xml` — HMS → Postgres (`metastore`) + warehouse `hdfs://hadoop:9000/warehouse`.
- `conf/trino-core-site.xml` — `fs.defaultFS=hdfs://hadoop:9000` para o connector Iceberg.
- `conf/trino-postgres.properties` — catálogo Postgres do Trino → `datalab_postgres_dw`.

Os Dockerfiles e o código (`docker/{spark,hive,trino}`, `src/`, `dbt/`) são os mesmos
da raiz do repo — no servidor eles vivem num diretório de staging
(`/home/dataadm/lakehouse/`) com estas configs adaptadas por cima.

**Deploy (25/07/2026 em diante):** `./sync-from-git.sh` (`git pull` num clone
read-only em `~/repo` + `rsync` pros diretórios live) — viabilizado pelo fix
de egress IPv4 (`FIX-EGRESS-IPV4.md`), que também resolveu o servidor não
alcançar o GitHub. Antes disso o código ia por `scp` arquivo a arquivo.

> **Nunca edite arquivos direto em `/home/dataadm/repo`** — esse diretório é
> sincronizado automaticamente (`git pull` do `sync-from-git.sh`/`auto-sync.py`);
> qualquer mudança manual lá é sobrescrita ou conflita com o próximo sync. Para
> testar algo pontual, copie para um diretório à parte (ex.:
> `cp -r ~/repo/dbt ~/dbt_test`).

**Automatizado (25/07/2026, à noite):** `auto-sync.py` roda via `cron`
(`*/15 * * * *`, usuário `dataadm`) e dispara o `sync-from-git.sh --apply`
sozinho — só quando há commit novo em `main` **e** a CI desse commit já
terminou verde (consulta a Checks API pública do GitHub, sem token). Nunca
reaplica o mesmo commit, nunca aplica com CI vermelho/pendente. Log em
`~/auto-sync.log` (do próprio script) e `~/auto-sync-cron.log` (saída bruta
do cron). Para checar: `tail -f ~/auto-sync.log`. Para rodar manualmente fora
do cron (debug): `python3 ~/repo/deploy/server-lakehouse/auto-sync.py`.

## Passos do deploy (resumo — runbook completo em docs/ interno)

```bash
# 1. Staging: ./sync-from-git.sh --apply sincroniza src/ e dbt/ do repo (via
#    git pull + rsync); docker/{spark,hive,trino} + estas configs adaptadas
#    continuam avulsas (não fazem parte do sync automático, mudam raramente).
# 2. DB do metastore no Postgres existente:
docker exec datalab_postgres psql -U dlab -d datalab \
  -c "CREATE USER hive WITH PASSWORD 'hive';" -c "CREATE DATABASE metastore OWNER hive;"
# 3. Warehouse no HDFS:
docker exec aula_hadoop hdfs dfs -mkdir -p /warehouse && docker exec aula_hadoop hdfs dfs -chmod 777 /warehouse
# 4. Subir infra:
cd /home/dataadm/lakehouse && docker compose build && docker compose up -d hive-metastore spark-master spark-worker trino
# 5. Silver (Spark, por data_extracao) e Gold (dbt):
docker exec -e HADOOP_USER_NAME=root lakehouse_spark_master /opt/spark/bin/spark-submit \
  --driver-memory 10g /opt/datalab/src/spark_jobs/silver_job.py --run-date <YYYY-MM-DD>
docker compose run --rm dbt build
```

Validado em 23/07/2026: Silver (empenhos 1.376.379 · contratos 215.402 · unidade_gestora 5.011),
Gold dbt-trino (fato_empenho 1.376.379 · fato_contrato 215.518), 22/22 testes dbt PASS.

## Coletores de observabilidade (cron)

Além do `auto-sync.py`, dois coletores rodam via `cron` (usuário `dataadm`),
gravando em `iceberg.audit` — mesmo padrão (script sem dependência externa,
grava via `docker exec ... trino --execute`):

```
*/5 * * * * /usr/bin/python3 /home/dataadm/repo/deploy/server-lakehouse/collect_infra_metrics.py lakehouse_trino >> /home/dataadm/collect-infra-metrics-cron.log 2>&1
*/5 * * * * /usr/bin/python3 /home/dataadm/repo/deploy/server-lakehouse/collect_access_audit.py lakehouse_trino >> /home/dataadm/collect-access-audit-cron.log 2>&1
```

`collect_access_audit.py` (sessão SSH + comando executado, "quem entrou,
quanto tempo ficou, o que rodou") depende de configuração de servidor que
**não é código versionável** — precisa ser feita manualmente uma vez em
qualquer novo servidor:

1. **Instalar auditd:** `sudo apt-get install -y auditd audispd-plugins`.
2. **Regra de auditoria de comando**, focada em sessão interativa real
   (exclui processo interno de container/cron via filtro de `auid`):
   ```bash
   echo "-a always,exit -F arch=b64 -S execve -F auid!=4294967295 -F auid!=0 -k cmd_exec" \
     | sudo tee /etc/audit/rules.d/50-cmd-exec.rules
   sudo augenrules --load && sudo systemctl restart auditd
   ```
3. **sudoers dedicado** (o coletor roda como `dataadm`, não root; `ausearch`
   precisa ler `/var/log/audit/audit.log`):
   ```bash
   echo "dataadm ALL=(root) NOPASSWD: /usr/sbin/ausearch" \
     | sudo tee /etc/sudoers.d/dataadm-ausearch
   sudo chmod 440 /etc/sudoers.d/dataadm-ausearch
   ```
4. **Achado real ao configurar (26/07/2026):** `ausearch` trava
   indefinidamente sem um terminal controlador — funciona na hora via SSH
   interativo, mas pendura pra sempre via cron ou qualquer chamada
   não-interativa. `collect_access_audit.py` já contorna isso internamente
   (`script -qc "..." /dev/null`, aloca um pseudo-terminal só pra rodar o
   `ausearch`) — não precisa de nada extra além dos 3 passos acima.

Validado em 26/07/2026: sessão SSH (`iceberg.audit.sessoes_ssh`) e comando
executado (`iceberg.audit.comandos_executados`) coletados corretamente,
excluindo ruído de processo interno de container/systemd/cron.
