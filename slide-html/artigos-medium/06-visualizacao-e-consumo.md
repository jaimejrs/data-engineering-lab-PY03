# Construindo um lakehouse de dados públicos (parte 6 e final): a única porta que abrimos para fora

*Na [parte 5](./05-machine-learning-e-ia.md), os dois modelos de aprendizado de máquina e o componente de inteligência artificial generativa já rodavam automaticamente todos os dias, com resultado registrado e rastreável. Fecha esta série a pergunta mais simples de todas, e também a mais fácil de esquecer no meio do entusiasmo técnico: depois de tudo isso, como alguém que não escreve SQL efetivamente enxerga o que foi construído?*

## Duas audiências, duas ferramentas diferentes

Vale reconhecer, antes de qualquer escolha de ferramenta, que "visualizar o dado" não é um problema único — são dois problemas diferentes, para públicos diferentes:

1. Alguém de fora da engenharia — um gestor público, um analista, qualquer pessoa curiosa sobre gasto público do Ceará — quer ver contratos, valor pago, previsão de pagamento e contratos fora do padrão, sem nenhuma fricção técnica.
2. O próprio time quer saber se o pipeline está saudável — se a carga de hoje rodou, se alguma DAG está falhando com frequência, se o servidor está com recurso sobrando ou apertado.

Misturar os dois num painel só tende a não servir bem nenhum dos dois lados. Este projeto separou explicitamente: um painel de negócio, público-facing, em **Streamlit**; e painéis operacionais, internos, em **Apache Superset**.

## O painel de negócio: Streamlit

O painel de negócio tem quatro abas — Visão Geral, Previsão de Pagamentos, Anomalias em Contratos e Resumo (IA) — e se conecta direto ao Trino, a mesma porta de entrada usada pelo dbt e pelos dois modelos de inteligência artificial (contada nas partes 4 e 5 desta série). Cada consulta é cacheada por alguns minutos (não a cada clique do usuário), justamente para não bater no Trino a cada interação de filtro na barra lateral — um cuidado pequeno, mas real, para não sobrecarregar um motor de consulta compartilhado com outras cargas do mesmo servidor.

**Streamlit** é uma escolha específica dentro de um universo maior de ferramentas para construir esse tipo de painel interativo em Python — Dash (da mesma empresa por trás do Plotly) e Panel são exemplos de concorrentes diretos, cada um com uma filosofia própria sobre como estruturar a página. A vantagem prática do Streamlit aqui é a curva quase inexistente entre "ter uma função Python que gera um gráfico" e "esse gráfico estar na tela, interativo, com filtro" — sem escrever HTML, CSS ou JavaScript, algo relevante quando quem constrói o painel já é fluente em Python e pandas (o mesmo ecossistema usado desde a ingestão, contada na parte 2), não necessariamente em desenvolvimento web.

## Uma tentativa anterior que não deu certo — e por quê

Vale contar honestamente uma tentativa anterior que precedeu a escolha atual, porque ela expõe um problema real de interoperabilidade que só aparece na prática: antes de qualquer painel em código, uma tentativa foi feita usando **Power BI**, conectado ao Trino pelo driver ODBC disponível para essa combinação. Essa tentativa esbarrou em bugs reais e documentados do driver: nomes de órgãos públicos com acentuação apareciam corrompidos na tela (um problema sério, tratando-se de nomes oficiais de instituição pública), e contagens agregadas estouravam o limite de um tipo numérico no meio do caminho, entregando números errados sem nenhum aviso de erro.

A causa raiz não estava no Trino, nem no dado — estava especificamente na camada de tradução do driver ODBC genérico. A correção não foi tentar contornar bug de driver de terceiro: foi trocar de abordagem, usando ferramentas que falam o **protocolo nativo do Trino** direto em Python (a mesma biblioteca `trino`, usada tanto pelo Streamlit quanto pelo Superset) — sem acentuação corrompida, sem estouro silencioso de número. Uma lição prática replicável: quando uma ferramenta comercial de BI promete conectar em "qualquer banco via ODBC/JDBC genérico", vale testar cedo contra dado real com acentuação e volume, antes de investir tempo construindo painéis em cima dela.

## A única porta aberta para fora

Toda a arquitetura contada nesta série, desde a [parte 1](./01-servidor-e-infraestrutura.md), foi construída sobre uma premissa deliberada: o servidor nunca é exposto à internet aberta, só alcançável de dentro da rede privada Tailscale do time. O painel de negócio em Streamlit é a **única exceção proposital** a essa regra — ele é publicado através de um recurso do próprio Tailscale (Funnel) que expõe especificamente esse serviço, e só ele, para fora da rede privada, sem exigir VPN de quem for acessar.

Essa exceção não é um descuido — é justamente o oposto: enquanto todo o resto do ambiente (Airflow, Trino, HDFS, MLflow, os painéis operacionais do Superset) é ferramenta de trabalho interna, sem motivo nenhum para existir fora da rede do time, o painel de negócio existe especificamente para ser mostrado a alguém de fora. Abrir só essa porta, de forma explícita e restrita a um único serviço, é mais seguro do que a alternativa de expor o servidor inteiro, ou de manter tudo fechado e inviabilizar a própria razão de o painel existir.

## Painéis internos: por que o Superset foi redirecionado

O Superset, neste projeto, não é mais a ferramenta que leva número para a área de negócio — esse papel migrou inteiramente para o Streamlit. Em vez disso, o Superset foi redirecionado para um papel puramente operacional: acompanhar a saúde do próprio pipeline. Vale uma nota histórica sobre a ferramenta em si — o Superset nasceu dentro do Airbnb, por volta de 2016 (sob os nomes "Panoramix" e depois "Caravel", antes de assumir o nome atual), como resposta direta ao alto custo de licenciamento de ferramentas comerciais de BI (Tableau e Looker são os exemplos mais conhecidos da época) para um time que precisava de painel interativo em escala, sem pagar por assento individual de usuário. Hoje é um projeto Apache, de código aberto, mantido por uma comunidade bem maior do que qualquer time interno de uma empresa só.

Quatro painéis cobrem esse acompanhamento operacional hoje:

- **Cargas e qualidade do pipeline** — quantos registros cada fonte trouxe na Bronze ao longo do tempo, e o quanto disso realmente chegou até a Silver e a Gold sem se perder pelo caminho (reconciliação entre camadas).
- **Execuções do Airflow** — taxa de sucesso e falha por DAG, duração média, quais tasks especificamente mais falham, e erros de importação de DAG que nem chegam a aparecer normalmente na interface do Airflow.
- **Métricas de infraestrutura** — CPU, memória e disco de cada container ao longo do tempo, num servidor compartilhado por várias pessoas ao mesmo tempo.
- **Auditoria de acesso** — quem entrou via SSH, por quanto tempo, e qual comando rodou (inclusive via `sudo`) — o mesmo mecanismo de auditoria contado na parte 1 desta série, aqui finalmente visualizável sem precisar vasculhar log manualmente.

Um detalhe de desenho vale registrar: a métrica de infraestrutura não vinha de nenhuma ferramenta de observabilidade dedicada de mercado (Prometheus e Grafana, com o coletor cAdvisor para métricas de container, seriam a combinação mais comum hoje para esse problema especificamente). Em vez de somar mais uma peça de infraestrutura ao ambiente só para isso, um script leve roda a cada 5 minutos via `cron` — o mesmo mecanismo já usado para o deploy automático — e grava o resultado como mais uma tabela Iceberg, lida pelo Superset exatamente do mesmo jeito que qualquer outra tabela da Gold. Menos peça nova para manter no ar, ao custo de abrir mão de recursos mais sofisticados (alertas automáticos, séries históricas de longuíssimo prazo) que uma stack dedicada ofereceria — uma troca consciente para o tamanho deste projeto, não necessariamente a resposta certa para qualquer escala.

## Fechando a série

Esta série percorreu seis etapas de um mesmo pipeline: um servidor Ubuntu compartilhado, isolado numa rede privada, com auditoria própria de acesso (parte 1); a ingestão de duas fontes públicas problemáticas para dentro de uma camada bruta, imutável (parte 2); a normalização e deduplicação dessa camada bruta em tabelas Iceberg confiáveis, via Spark (parte 3); a modelagem declarativa dessas tabelas num modelo de negócio testado automaticamente, via dbt e Trino (parte 4); dois modelos de aprendizado de máquina e um componente de inteligência artificial generativa lendo esse modelo de negócio para responder perguntas que uma consulta SQL sozinha não responde (parte 5); e, por fim, dois painéis com propósitos e públicos deliberadamente diferentes, fechando o ciclo entre o dado bruto original e alguém que nunca vai escrever uma linha de SQL (esta parte).

Em cada uma dessas seis etapas, a escolha de ferramenta raramente foi "a mais nova do mercado" ou "a mais usada em qualquer lugar" — foi, quase sempre, a que resolvia o problema real que apareceu na frente, muitas vezes depois de uma primeira tentativa que não funcionou. Se este projeto tem uma lição central para além de qualquer ferramenta específica citada ao longo da série, é essa: infraestrutura de dado que se sustenta de verdade nasce menos de escolher a tecnologia certa de antemão, e mais de estar disposto a testar, errar e trocar de abordagem quando o dado real mostra que a primeira ideia não bastava.
