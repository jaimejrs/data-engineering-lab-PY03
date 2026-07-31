# Construindo um lakehouse de dados públicos (parte 1): preparando o terreno

*Esta é a primeira de seis partes contando como transformamos duas fontes públicas de dados do Ceará, sem chave confiável e sem trilha de auditoria, em um lakehouse de verdade rodando em produção. Antes de falar de qualquer linha de dado, precisamos falar do chão onde tudo isso roda: um servidor Ubuntu, provisionado especificamente para este projeto.*

## Um servidor pensado desde o início para simular produção

A primeira decisão deste projeto não foi de ferramenta — foi de ambiente. Em vez de cada pessoa do time montar (e derrubar) o pipeline na própria máquina, decidimos provisionar um servidor Ubuntu único, compartilhado por todo o time, criado especificamente para este projeto. Duas motivações concretas levaram a essa escolha: trabalhar como equipe de verdade, todo mundo apontando para o mesmo ambiente vivo em vez de "na minha máquina funciona"; e simular um contexto mais próximo do real — acesso restrito, múltiplas pessoas compartilhando a mesma infraestrutura ao mesmo tempo, disciplina de deploy — em vez de um exercício acadêmico isolado, rodando sozinho no notebook de alguém.

O servidor foi crescendo em camadas, seguindo as próprias fases do projeto: a fase de ingestão (Bronze) já exigiu um sistema de arquivos distribuído (HDFS) e um orquestrador (Airflow) no ar; quando o projeto avançou para as camadas de transformação e modelagem (Silver e Gold), um novo conjunto de peças — catálogo de metadados, motor de processamento distribuído, motor de consulta e a ferramenta de construção da camada final — subiu ao lado do que já estava rodando, na mesma rede Docker, sem precisar recriar nada do que a fase anterior já tinha validado.

O resultado é que este projeto tem **duas descrições de infraestrutura** diferentes, e não por acidente:

- Uma pensada para qualquer pessoa reproduzir o projeto inteiro do zero, na própria máquina — importante para fins de portfólio e avaliação.
- Uma pensada para o servidor real do time, adaptada às particularidades desse ambiente específico (nome de rede, topologia dos containers) que foram sendo fixadas conforme cada fase do projeto entrava no ar.

Uma verificação automática compara as duas periodicamente, para avisar caso alguém atualize uma e esqueça de replicar a mudança na outra.

## Cada ferramenta na sua caixa

Todas as peças do projeto rodam como containers Docker — o banco de dados, o sistema de arquivos, o orquestrador, o motor de processamento, o catálogo, o motor de consulta. A ideia central do Docker é simples: cada ferramenta roda isolada, com exatamente as versões e configurações de que precisa, sem brigar com as vizinhas por uma versão diferente de alguma biblioteca. Quando uma peça precisa de mais memória, ou de uma versão nova, isso é resolvido isoladamente, sem risco de derrubar as outras.

No servidor real, cada container roda com um limite definido de quanto processador e memória pode consumir — para que um processamento pesado de uma ferramenta não deixe as outras lentas, já que várias pessoas do time compartilham a mesma máquina ao mesmo tempo. E nenhuma senha ou chave de acesso fica escrita dentro dessas descrições de infraestrutura: tudo vem de configuração de ambiente, nunca versionado.

## Uma rede privada, não a internet aberta

O servidor não é exposto à internet. O acesso — tanto SSH quanto às interfaces web de cada ferramenta (Airflow, Trino, HDFS, MLflow) — só existe dentro de uma rede privada própria do time, criada com **Tailscale**. Na prática, isso significa que só quem está autenticado nessa rede privada enxerga o servidor; para qualquer pessoa de fora, ele simplesmente não existe.

```bash
ssh dataadm@100.69.31.14
```

Esse comando só funciona para quem já está dentro da rede Tailscale do time. O servidor fica no ar 24 horas por dia, com uso concomitante de várias pessoas — o que trouxe uma pergunta natural: se várias pessoas compartilham o mesmo acesso à mesma infraestrutura de produção, como saber quem fez o quê?

## Uma auditoria de acesso própria

A resposta foi construir uma auditoria de acesso, gravada como dado — não como um arquivo de log solto que ninguém nunca vai olhar de novo. O sistema operacional já registra, através do `auditd`, cada comando executado por uma sessão interativa real (inclusive via `sudo`), com um filtro específico para não capturar ruído de processo interno de container ou de tarefa agendada. Um coletor próprio lê esse registro a cada 5 minutos e grava sessão SSH e comando executado como tabelas Iceberg, no mesmo lakehouse que o projeto já constrói para os dados públicos.

Isso não foi trivial de configurar: a ferramenta usada para consultar esse log de auditoria (`ausearch`) trava indefinidamente quando chamada fora de um terminal interativo — funciona na hora via SSH, mas pendura para sempre quando chamada por um `cron`. A solução foi alocar um pseudo-terminal só para essa chamada (via `script -qc "..." /dev/null`), o suficiente para o comando achar que está rodando interativamente e retornar normalmente. Dado sensível como IP e o comando completo tem retenção limitada (90 dias por padrão), a mesma rotina que cuida de compactação e expiração de versões antigas no Iceberg.

## Chegar em produção sem arriscar produção

Toda mudança de infraestrutura passa antes por um ambiente isolado, nunca direto no servidor real — o pipeline inteiro sobe do zero a cada execução de integração contínua, validando que tudo ainda funciona junto antes de qualquer coisa chegar no servidor de verdade.

Quando o código está pronto para ir ao ar, existem duas camadas independentes de entrega contínua, por design, não por acidente:

1. Um job de deploy que entra na rede privada do servidor via Tailscale e aplica o código por SSH, usando uma chave dedicada exclusivamente a essa tarefa — restrita, do lado do servidor, a rodar apenas o script de deploy, nada mais, mesmo que a chave vazasse.
2. Uma segunda camada, mais simples e independente da primeira, rodando via `cron` no próprio servidor a cada 15 minutos: ela consulta a API pública de checks do GitHub e só aplica o código mais recente se a integração contínua desse commit já tiver terminado com sucesso. Ela nunca reaplica o mesmo commit duas vezes, e nunca aplica um commit com testes vermelhos ou pendentes.

A segunda camada existe como rede de segurança para o caso de a primeira falhar — uma prova de que, mesmo em um projeto pequeno, vale a pena desenhar para o cenário em que alguma peça do caminho automatizado não funciona.

## Um problema real de rede, resolvido na raiz

Nem tudo num servidor provisionado do zero vem configurado do jeito que se espera de saída. Em um certo ponto, descobrimos que o servidor só tinha rota de saída via IPv6 — a interface de rede cabeada nunca teve IPv4 habilitado no arquivo de configuração de rede. Na prática, isso significava que o servidor não conseguia falar diretamente com a API pública de dados nem com o GitHub, e a extração de dados dependia de um relay rodando em uma máquina pessoal só para contornar isso.

O conserto não foi outro workaround por cima do workaround — foi corrigir a causa raiz: habilitar IPv4 na interface certa do próprio host. Depois de confirmado que a API, o GitHub e o gerenciador de pacotes Python respondiam diretamente, sem passar pelo relay, removemos o contorno interno que forçava o nome da API a resolver para o IP do relay dentro dos containers do Airflow. O relay pessoal, que existia apenas por causa dessa limitação, pôde ser desligado.

## O que vem a seguir

Com o servidor no ar, isolado numa rede privada, auditado e com dois caminhos independentes de entrega contínua, a próxima pergunta é: como um dado que nasce em uma API pública paginada e em um banco PostgreSQL de origem — sem chave primária declarada, com datas em texto solto — chega de forma confiável na primeira camada do lakehouse? Isso é assunto da parte 2, sobre a ingestão da camada Bronze.
