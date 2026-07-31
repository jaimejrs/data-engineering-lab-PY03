# Construindo um lakehouse de dados públicos (parte 2): fazendo o dado bruto chegar inteiro

*Na [parte 1](./01-servidor-e-infraestrutura.md) desta série, contamos como o servidor que hospeda este projeto foi montado: um Ubuntu compartilhado, isolado numa rede privada, com duas camadas de entrega contínua e uma auditoria própria de acesso. Com o chão pronto, esta parte é sobre o primeiro problema de dados de verdade: pegar duas fontes públicas que não foram desenhadas para virar uma base analítica, e fazer o dado bruto chegar ao lakehouse inteiro, sem perda e sem duplicidade.*

## Duas fontes, dois formatos, nenhuma pensada para isso

O projeto parte de duas fontes públicas do Ceará: uma **API REST** de contratos (paginada, só resposta em JSON) e um banco **PostgreSQL** de origem, com três tabelas (`empenhos`, `ordem_bancaria_orcamentaria`, `unidade_gestora`). Nenhuma das duas foi pensada como fonte de um pipeline analítico — a API não tem um jeito nativo de exportar tudo de uma vez, e nenhuma tabela do Postgres tem sequer uma chave primária declarada.

A camada **Bronze** é a primeira parada dos dois: um espelho fiel do que a fonte respondeu, sem nenhuma transformação, gravado como JSON simples. Essa é uma escolha deliberada, e vale explicar por quê.

## Por que gravar cru, em vez de já transformar

Numa arquitetura de dados mais tradicional (o modelo de *data warehouse* que dominou o mercado das décadas de 1990 e 2000, ligado a ferramentas como Informatica PowerCenter ou Microsoft SSIS), o padrão comum era **ETL**: extrair, já **transformar** de acordo com as regras de negócio, e só então carregar no destino final. O problema é que, se uma regra de transformação estiver errada — ou mudar meses depois —, o dado original já não existe mais em lugar nenhum para reprocessar; é preciso voltar à fonte, que pode ter mudado, ter rate limit, ou nem existir mais daquele jeito.

A arquitetura em camadas usada aqui (Bronze → Silver → Gold, um padrão que passou a se chamar **arquitetura medalhão** e foi popularizado pela comunidade em torno do Apache Spark/Delta Lake a partir de 2019) inverte essa ordem: primeiro **carrega** o dado exatamente como veio (**ELT**, não ETL), e só depois aplica as regras de transformação, numa camada separada. Se uma regra de normalização se mostrar errada, o reprocessamento parte do dado bruto guardado na Bronze — nunca é preciso bater na API ou no banco de origem de novo. Não é uma vantagem teórica: aconteceu de verdade neste projeto, quando percebemos que uma coluna estava sendo convertida com o tipo errado — o conserto foi só reprocessar a Bronze já guardada, sem tocar a fonte outra vez.

## A extração em si: por que código, não uma ferramenta visual

A extração das duas fontes (`src/extractors/api_extractor.py` e `src/extractors/postgres_extractor.py`) é escrita em Python puro — `requests` para a API, `pandas` + `SQLAlchemy` para o Postgres — em vez de uma ferramenta de ETL visual, do tipo "arraste e solte" (a mesma família do Informatica/SSIS citada acima, ou ferramentas mais atuais como o Talend). Para um pipeline deste tamanho, a escolha por código puro trouxe algumas vantagens concretas: o extractor inteiro é revisável por Pull Request, testável por `pytest` como qualquer outra parte do projeto, e versionado no mesmo Git de tudo o mais — nenhuma lógica de negócio fica escondida numa tela gráfica que só abre dentro de uma ferramenta paga.

Duas particularidades reais da API valem ser contadas, porque mostram por que vale a pena um extractor próprio em vez de confiar cegamente no formato documentado:

- A API espera as datas no formato `DD/MM/AAAA`, não em ISO — mandar ISO direto faz a API responder `HTTP 200` com um texto de erro em vez de JSON, um jeito enganoso de falhar silenciosamente se ninguém checar o corpo da resposta.
- A chave que informa quantas páginas existem no total vem com um erro de digitação real da própria API (`"sumary"`, faltando a segunda letra "m"). O código trata isso com um fallback (`sumary` OU `summary`) e, mais importante, **aborta a extração** se nenhuma das duas chaves vier na resposta — em vez de arriscar um laço que nunca teria fim.

## O detalhe que evita estourar a memória do banco de origem

Um problema real apareceu ao carregar o histórico completo das tabelas do Postgres (perto de 1,4 milhão de linhas em `empenhos` e outro tanto em `ordem_bancaria_orcamentaria`): pedir tudo de uma vez, mesmo com paginação do lado do código Python, ainda estourava memória — **do lado do servidor de banco**, não do cliente. A causa era sutil: sem um parâmetro específico da conexão (`stream_results=True`, que ativa um cursor nomeado no `psycopg2`), o Postgres tenta montar a resposta inteira da consulta antes de mandar a primeira linha, não importa como o código do lado do cliente pretenda consumir o resultado depois.

É o mesmo tipo de problema que motivou, historicamente, o surgimento de ferramentas de processamento distribuído como o próprio Hadoop/MapReduce nos anos 2000: processar em blocos, nunca tentar segurar tudo na memória de uma vez, porque o volume só cresce. Aqui a solução foi mais simples — não precisamos de um cluster de processamento para isso, só de pedir ao banco, corretamente, para transmitir os resultados em blocos reais, e gravar cada bloco (até 20 mil linhas) como um arquivo JSON separado na Bronze, em vez de um único arquivo gigante.

## Onde o dado bruto realmente mora: HDFS

O destino de tudo isso é o **HDFS** (Hadoop Distributed File System) — o mesmo sistema de arquivos distribuído que já rodava no servidor antes deste projeto começar, parte da infraestrutura do curso. Vale contextualizar por que uma ferramenta como essa existe: antes do Hadoop (que nasceu por volta de 2006, inspirado em artigos técnicos publicados pelo Google sobre como a própria empresa guardava e processava um volume de dado imenso), guardar e processar dado em grande escala normalmente exigia hardware especializado e caro — os chamados *storage arrays* corporativos. O Hadoop popularizou a ideia de distribuir esse volume entre várias máquinas comuns, mais baratas, replicando cada arquivo para não perder nada se uma delas falhar.

Vale uma ressalva honesta: o mercado hoje, para projetos novos, tende a preferir armazenamento de objetos na nuvem (Amazon S3, Google Cloud Storage e equivalentes) no lugar de operar um cluster HDFS próprio — a nuvem tira do time a responsabilidade de manter o cluster de armazenamento no ar, e escala sem planejamento de capacidade manual. Neste projeto, porém, o HDFS não foi uma escolha nova: ele já existia, rodando no servidor compartilhado, antes da nossa primeira linha de código. A decisão certa foi reaproveitá-lo, e não gerar dois pontos de armazenamento de verdade divergentes — mas o código de leitura/escrita da Bronze (`src/extractors/storage.py`) foi escrito com um backend alternável (`local` ou `hdfs`, por variável de ambiente), justamente para não travar o projeto nessa dependência para sempre.

Cada extração é gravada num caminho que já carrega a data do evento e a data em que a extração rodou — algo como `/bronze/contratos/ano=2026/mes=07/data_extracao=2026-07-25/`. Esse particionamento existe para que qualquer etapa seguinte do pipeline saiba exatamente qual lote de arquivos processar, sem precisar adivinhar nem reler tudo desde o início.

## Quem manda: Airflow e a extração incremental por Dataset

Nenhuma dessas extrações roda sozinha, avulsa, disparada manualmente. Quem orquestra é o **Apache Airflow** — mais uma peça que já existia parcialmente no servidor do curso antes deste projeto. O Airflow nasceu dentro do Airbnb em 2014, exatamente para resolver um problema comum antes dele: pipelines de dados costumavam ser um emaranhado de scripts disparados por `cron`, sem visibilidade real de quais tarefas dependiam de quais, sem reexecução automática em caso de falha parcial, e sem um jeito central de enxergar o que rodou, quando e com que resultado. Hoje o Airflow tem concorrentes diretos mais recentes no mercado (Dagster e Prefect, por exemplo, ambos nascidos de críticas específicas a decisões de design do próprio Airflow) — mas, de novo, a peça já estava ali, rodando, antes de escolhermos qualquer coisa.

A DAG `bronze_extract` roda diariamente e faz mais do que só "extrair tudo de novo": ela mantém um **watermark** (marca d'água) próprio por fonte — a maior data de evento já vista naquela fonte — e usa isso como ponto de partida da próxima execução, com uma margem de segurança de 7 dias para trás (`LOOKBACK_DAYS`), porque um lançamento pode entrar na origem com data retroativa depois que já avançamos o watermark. Reprocessar essa sobreposição não é um problema: a camada Silver (assunto da parte 3) foi construída para ser idempotente a isso.

Essa DAG só avança para a próxima etapa por meio de um recurso relativamente recente do próprio Airflow (introduzido na versão 2.4, em 2022): **Datasets**, que permite disparar a DAG seguinte assim que esta emite um sinal de "dado validado disponível", em vez de depender de um horário fixo que poderia rodar cedo demais, antes do dado estar pronto — outra melhoria direta sobre o modelo "legado" de agendamento só por horário.

## A última etapa antes de confiar no dado: validar

Antes de avançar o watermark — ou seja, antes de considerar aquela extração "boa" para as próximas camadas lerem — existe uma etapa de validação (`src/validators/bronze_validator.py`) que confere schema e completude do que acabou de ser gravado. Existem ferramentas de mercado dedicadas inteiramente a esse problema (Great Expectations e Soda são as mais conhecidas), mas, dado o escopo específico do projeto, optamos por um validador próprio, enxuto, sem trazer uma dependência nova só para checagens relativamente simples. Se a validação falhar, a DAG inteira falha — o watermark não avança, e a próxima execução tenta reprocessar a mesma janela, em vez de seguir em frente com um dado suspeito.

## O que vem a seguir

Com o dado bruto pousado na Bronze, particionado, validado e sem margem para regra de negócio nenhuma ainda, a pergunta da parte 3 é: como transformar meses de arquivos JSON soltos, sem chave primária confiável, em tabelas normalizadas e sem duplicidade — mesmo quando o mesmo período é reprocessado mais de uma vez? Essa é a camada Silver, construída sobre Apache Spark e Apache Iceberg.
