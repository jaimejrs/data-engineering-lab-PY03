# Construindo um lakehouse de dados públicos (parte 3): de arquivo solto a tabela de verdade

*Na [parte 2](./02-ingestao-bronze.md), o dado bruto das duas fontes chegou inteiro à camada Bronze, sem nenhuma transformação, guardado como JSON simples e particionado por data. Esta parte é sobre o que acontece a seguir: como meses de arquivos soltos, sem chave primária confiável e sujeitos a reprocessamento, viram tabelas normalizadas, sem duplicidade, mesmo quando o mesmo período é processado mais de uma vez. É aqui que o projeto deixa de ser um *data lake* e passa a ser, de fato, um *lakehouse*.*

## O problema que a camada Silver resolve

A Bronze guarda, para uma mesma tabela de origem, um arquivo novo a cada execução diária — nunca sobrescreve, nunca deduplica. Isso é uma vantagem para auditoria (o dado bruto original nunca desaparece), mas é inútil para consulta direta: ninguém quer varrer centenas de arquivos JSON, cada um representando um dia diferente de extração, só para saber o estado atual de um contrato. A camada **Silver** existe para resolver exatamente isso — normalizar o formato e consolidar tudo numa única visão atualizada, sem duplicar linha nenhuma mesmo quando a mesma janela de dados é reprocessada.

E reprocessamento não é hipótese: a extração incremental usa uma margem de segurança de 7 dias para trás (contada na parte 2), justamente porque um lançamento pode entrar na fonte com data retroativa. Isso significa que a mesma linha pode, de propósito, aparecer em mais de uma extração — e a Silver precisa lidar bem com isso.

## Como este projeto já tentou resolver isso antes — e por que não bastou

Vale contar a própria história deste projeto, porque ela é um caso real do problema que este artigo descreve. A primeira versão da Silver era **Parquet solto em HDFS**, escrito diretamente por pandas: um formato colunar eficiente para leitura, mas sem nenhuma noção de transação, sem controle de versão, sem qualquer garantia de que duas execuções não duplicariam informação. A deduplicação daquela versão só enxergava o lote que estava sendo processado naquele momento — se a mesma linha aparecesse de novo numa execução seguinte (o cenário exato do reprocessamento por lookback), ela virava uma segunda cópia solta no arquivo.

Isso é, historicamente, o problema central de qualquer **data lake** clássico (o termo popularizado a partir do início dos anos 2010, para descrever grandes repositórios de arquivo bruto/semi-estruturado, geralmente sobre Hadoop): armazenamento barato e flexível, mas sem as garantias transacionais que um banco de dados relacional sempre teve. A resposta do mercado a esse problema, a partir de 2018–2019, foi uma nova categoria de formato de tabela — Apache Iceberg (criado dentro do Netflix), Delta Lake (criado dentro do Databricks) e Apache Hudi (criado dentro do Uber) surgiram quase ao mesmo tempo, todos resolvendo essencialmente o mesmo problema: trazer garantias de transação (ACID), controle de versão e schema para cima de arquivos que já viviam num data lake. Esse conjunto de ideias é o que passou a ser chamado de **lakehouse** — e é exatamente a mudança que este projeto fez, trocando o Parquet solto por tabelas **Apache Iceberg**.

## Por que Iceberg, e não Delta Lake ou Hudi

Os três formatos citados acima resolvem o mesmo problema central, mas partiram de pontos de partida diferentes — e essa origem ainda importa na escolha. O Delta Lake nasceu fortemente acoplado ao Spark e ao ecossistema comercial do Databricks (embora hoje seja mais aberto do que era originalmente); o Hudi foi desenhado com forte foco em ingestão incremental de baixa latência. O Iceberg, por sua vez, foi desenhado desde o início para ser **agnóstico ao motor de processamento** — a especificação da tabela não pertence a nenhum motor específico, e tanto o Spark quanto o Trino (a ferramenta que consulta e constrói a camada Gold, assunto da próxima parte) leem e escrevem o mesmo formato através do mesmo catálogo.

Essa característica não foi um detalhe teórico aqui: é justamente o que faz o Spark (que escreve a Silver) e o Trino (que lê a Silver para construir a Gold, e também escreve as tabelas de resultado dos modelos de IA) enxergarem exatamente a mesma verdade sobre cada tabela, sem exportar, converter ou sincronizar nada manualmente entre os dois mundos.

## Quem processa: Apache Spark, e por que não pandas puro

O motor que lê a Bronze, normaliza e grava na Silver é o **Apache Spark**. A limitação mais óbvia do pandas — usado na primeira versão deste mesmo projeto — é que ele processa tudo numa única máquina, num único processo: para transformar mais de um milhão de linhas de empenhos por execução, isso deixa de escalar bem em algum momento.

O Spark nasceu em 2014 (originado como projeto acadêmico no AMPLab da Universidade da Califórnia em Berkeley, depois doado à Apache) como resposta direta a uma limitação do próprio **Hadoop MapReduce**, o motor de processamento distribuído que veio antes dele: o MapReduce escreve o resultado de cada etapa intermediária em disco antes de seguir para a próxima, o que o tornava seguro, porém lento para cargas de trabalho com muitas etapas encadeadas. O Spark mantém dado intermediário em memória entre as etapas sempre que possível, o que historicamente trouxe ganhos de dezenas de vezes em desempenho para esse tipo de carga — e foi um dos fatores que tornou o MapReduce, hoje, um motor raramente escolhido para projeto novo.

Vale registrar, com honestidade, que o mercado atual tem alternativas relevantes ao Spark para processamento distribuído — Apache Flink (mais focado em streaming contínuo) e Dask (mais leve, mais próximo do ecossistema Python/pandas) são exemplos. A escolha do Spark aqui não foi feita no vácuo: é o motor com o suporte mais maduro e mais direto ao Iceberg, e processar em lote diário (não em streaming contínuo) é exatamente o cenário para o qual ele foi desenhado.

## O `MERGE INTO`: o coração da camada Silver

A peça central do job de Silver (`src/spark_jobs/silver_job.py`) é um `MERGE INTO` — o comando que decide, linha a linha, se um registro do lote recém-chegado já existe na tabela (e deve atualizar) ou é novo (e deve ser inserido), usando uma chave de negócio definida por fonte (o identificador do contrato, ou o par identificador+ano para empenhos e ordens bancárias, já que a fonte original não garante identificador único sozinho).

Dois detalhes reais valem a pena contar, porque mostram problemas que só aparecem quando o dado é real e não um exemplo de tutorial:

- **Desempate determinístico.** Quando a mesma `data_extracao` traz duas versões do mesmo registro (a fonte, às vezes, responde de forma levemente diferente para a mesma linha em requisições diferentes), simplesmente remover duplicatas com a função padrão do Spark escolhe uma linha de forma não determinística — pode mudar a cada execução, dependendo do plano físico interno. A solução foi ordenar por um hash calculado sobre todas as colunas da linha, garantindo que a escolha seja sempre a mesma, execução após execução, sem inventar um conceito de "mais recente" que a fonte não fornece (não existe coluna de última atualização por linha).
- **Tipo inconsistente entre lotes.** O mesmo campo, vindo de uma API JSON, às vezes chega como texto e às vezes como verdadeiro/falso, dependendo do lote. Sem tratar isso, o `MERGE` falha inteiro por causa de uma única coluna inconsistente. A correção converte cada coluna do lote para o tipo já estabelecido na tabela antes de tentar o merge — best-effort, um valor realmente incompatível vira nulo em vez de derrubar a carga inteira.

O resultado prático, testado de propósito contra o ambiente real: reprocessar o mesmo dia duas vezes seguidas resulta exatamente na mesma contagem de linhas, sem duplicar nada — a garantia que a versão anterior, em Parquet solto, não conseguia dar.

## Quem sabe onde cada tabela está: Hive Metastore

Duas ferramentas diferentes trabalham sobre as mesmas tabelas Iceberg: o Spark escreve a Silver, e o Trino lê a Silver para construir a Gold. Para as duas enxergarem exatamente a mesma coisa, existe um catálogo compartilhado — o **Hive Metastore** — que não guarda o dado em si, só sabe, para cada tabela, onde os arquivos dela estão e qual é a versão mais recente.

Há uma curiosidade histórica genuína aqui: o Hive Metastore nasceu como parte do Apache Hive, criado dentro do Facebook por volta de 2010, o primeiro motor a trazer uma linguagem parecida com SQL para cima do Hadoop. O Hive-motor-de-consulta, hoje, é raramente a primeira escolha para projeto novo — motores mais modernos como o próprio Trino (assunto da parte 4) o superam em desempenho para consulta interativa. Mas o **Hive Metastore**, a peça de catálogo, sobreviveu ao motor que lhe deu nome: virou um padrão de fato para catalogar tabelas, falado nativamente por ferramentas muito mais novas — Spark e Trino incluídos — mesmo quando ninguém ali está de fato rodando o Hive original.

## Uma decisão de arquitetura que veio de um problema de rede, não de teoria

Nem toda decisão técnica deste projeto veio de uma comparação de mercado — algumas vieram de tentar uma abordagem, ela falhar na prática, e trocar por outra. A orquestração do job de Silver no Airflow (DAG `silver_transform`) inicialmente rodava em **modo cliente** do Spark (`SparkSubmitOperator`), com o processo "condutor" (*driver*) rodando dentro do próprio container do Airflow e os executores respondendo de containers separados do cluster Spark. Na prática, essa combinação trouxe problemas de rede entre os executores e o driver — e a imagem do Airflow, de qualquer forma, não é um bom ambiente para rodar Spark de verdade.

A solução foi trocar por um `DockerOperator`: o Airflow apenas dispara um container dedicado, já com o runtime Spark completo (Java 17, Spark 3.5.3, o jar do Iceberg embutido), rodando `spark-submit` em modo local dentro dele mesmo — sem depender de rede entre containers separados para a execução diária. O cluster Spark dedicado (`spark-master`/`spark-worker`) continua existindo, mas reservado para reprocessamentos grandes e pontuais do histórico completo, não para a rotina diária.

## O que vem a seguir

Com a Silver normalizada, deduplicada entre execuções e catalogada de um jeito que Spark e outras ferramentas enxergam da mesma forma, falta a última transformação: organizar esse dado num modelo pensado para consulta de negócio — dimensões, fatos, e testes automáticos que garantem que a modelagem não quebrou silenciosamente. Essa é a camada Gold, construída de forma declarativa com dbt sobre o Trino, assunto da parte 4.
