# Workaround — saída IPv4 para a task `extract_api`

Última atualização: 19/07/2026 · Aplicado e validado em produção (ver `docs/checklist.md`
e `docs/relatorio-orquestracao-dag1.md` para o histórico anterior a esta correção).

## Causa raiz (confirmada, não é config de container)

O `datalab-server` **não tem nenhuma rota IPv4 padrão** — só saída IPv6. Confirmado via:

```
$ ip -4 route show
172.17.0.0/16 dev docker0 ...
172.18.0.0/16 dev br-dc99c5291f2d ...
172.19.0.0/16 dev br-e467b660d0aa ...   # só as sub-redes locais do Docker

$ ip -6 route show default
default via fe80::... dev enp12s0 ...   # só existe rota IPv6
```

A API do Ceará Transparente (`api-dados-abertos.cearatransparente.ce.gov.br`,
`189.90.161.108`) é **IPv4-only**. Não há como alcançá-la a partir deste host sem uma
ponte para uma rede que tenha IPv4 real — isso não é um problema de iptables/Docker,
é uma limitação do host em si (provavelmente do provedor de hospedagem).

Uma tentativa anterior de resolver isso configurando um **exit node do Tailscale**
quebrou o roteamento local para as redes Docker (`rota /15 do Tailscale sobrepondo a
rota da bridge local`, ver `relatorio-orquestracao-dag1.md`) e foi revertida.

## Solução aplicada: relay TCP via Tailscale (sem exit node)

Diferente da tentativa anterior, esta abordagem **não mexe na rota padrão do host** —
usa apenas conectividade ponto-a-ponto normal do Tailscale (a mesma que já permite
`ssh dataadm@100.69.31.14` funcionar), que já está confirmada como funcional para
tráfego encaminhado pelos containers:

```
$ ip route get 100.101.236.119 from 172.19.0.4 iif br-e467b660d0aa
100.101.236.119 from 172.19.0.4 dev tailscale0 table 52   # roteável, sem conflito
```

### Como funciona

1. **`jotav15-1`** (100.101.236.119 no Tailscale) — a máquina Windows do time que já
   tem IPv4 pleno e que já era usada manualmente para validar a extração da API — roda
   um relay TCP transparente (`api_relay.py`, script local, não versionado neste repo
   por ser específico da máquina) escutando em `100.101.236.119:443`, repassando bytes
   crus para `api-dados-abertos.cearatransparente.ce.gov.br:443`.
2. O relay **não termina TLS** — só repassa bytes. O container continua enviando o
   `ClientHello` com o hostname real (SNI), e o certificado retornado pelo servidor
   real continua válido para esse hostname. Do ponto de vista do Python/`requests`
   rodando na DAG, nada muda a não ser o IP para onde o hostname resolve.
3. Nos containers do Airflow, `docker-compose.yml` define `extra_hosts` apontando
   `api-dados-abertos.cearatransparente.ce.gov.br` para `100.101.236.119` — o mesmo
   truque de override de hostname que o README da Nara já documentava para `hadoop`.
4. Camada extra de segurança: o relay só aceita conexões vindas de `100.69.31.14`
   (o próprio `datalab-server`), rejeitando qualquer outra origem — mesmo que a regra
   de firewall do Windows seja mais permissiva.

### Validado

- Conexão TCP crua: host → relay ✅, container → relay ✅.
- Requisição HTTPS completa através do relay (`GET /transparencia/contratos/contratos`)
  retornou `200` com dados reais.
- DAG `bronze_extract` disparada após a mudança — `extract_api` concluída com sucesso
  dentro do Airflow pela primeira vez (ver run `manual__2026-07-19T...` no Airflow).

## ⚠️ Limitação importante — dependência de uma máquina pessoal

**A task `extract_api` só funciona dentro do Airflow enquanto o notebook `jotav15-1`
estiver ligado, conectado ao Tailscale e com o script `api_relay.py` rodando.** Se essa
máquina desligar ou perder conexão, `extract_api` volta a falhar com o mesmo erro de
antes (`Network is unreachable`) — não há perda de dados (a DAG não avança o watermark
se a validação falhar), só um atraso até a máquina voltar.

Isso é uma melhoria real (a extração deixa de depender de rodar o script manualmente
fora do Airflow), mas continua sendo um **workaround de desenvolvimento**, não uma
solução de produção. As opções para resolver definitivamente, em ordem de preferência:

1. **Conseguir saída IPv4 real para o `datalab-server`** junto ao provedor de
   hospedagem — resolve a causa raiz para qualquer necessidade futura de IPv4, não só
   esta API.
2. **Mover o relay para uma máquina que fique sempre ligada** (ex: um exit node
   dedicado, um VPS pequeno com IPv4, ou uma das outras máquinas do time no Tailscale
   que fique sempre online) — mesma técnica, sem depender de um notebook pessoal.
3. Reavaliar o exit node do Tailscale com escopo mais restrito (ex: `--exit-node` só
   para um range específico de destino, não `0.0.0.0/0`) — mais arriscado, requer
   testes cuidadosos por já ter quebrado o roteamento uma vez neste host.

### Como reiniciar o relay (se `jotav15-1` reiniciar)

No `jotav15-1`: rodar `api_relay.py` (escuta em `100.101.236.119:443`, repassa para a
API). Verificar que a interface Tailscale está com categoria de rede **Privada** (não
Pública) para que o Windows Firewall não bloqueie a conexão de entrada vinda de
`100.69.31.14`.
