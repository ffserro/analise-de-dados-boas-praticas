# Análise de Dados e Boas Práticas

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/ffserro/analise-de-dados-boas-praticas/blob/main/main.ipynb)

Este repositório reúne um projeto de análise de dados sobre publicações do Diário Oficial da União ligadas ao Ministério da Defesa, com foco nas três Forças: Marinha, Exército e Aeronáutica.

A ideia central do trabalho foi sair da lógica de "só processar dados" e construir uma leitura que fizesse sentido do ponto de vista analítico: como essas instituições publicam, em que volume, em quais seções do DOU, com que perfil documental e como esse comportamento muda ao longo do tempo.

O resultado principal está no notebook [main.ipynb](/Users/ffserro/Desktop/Cursos/PUC/analise-de-dados-boas-praticas/main.ipynb), estruturado para rodar no Google Colab e também localmente.

## O que este projeto responde

Ao longo do notebook, o trabalho busca responder perguntas como:

- Quais tipos de documentos aparecem com mais frequência?
- Como o volume de publicações varia ao longo do tempo?
- Existem diferenças claras entre Marinha, Exército e Aeronáutica?
- As seções DO1, DO2 e DO3 têm perfis documentais diferentes?
- O comportamento observado muda só em quantidade ou também em composição?

## O que foi feito

O fluxo do projeto foi construído em etapas:

1. coleta e consolidação dos dados brutos;
2. limpeza e redução do escopo analítico;
3. padronização de variáveis institucionais, temporais e documentais;
4. criação de uma base final única para análise;
5. exploração descritiva com gráficos e interpretação textual;
6. construção de uma proxy temática a partir do tipo documental padronizado.

Um ponto importante: a variável temática usada no trabalho não é produto de um classificador supervisionado. Ela foi construída como um agrupamento analítico de tipos documentais, com o objetivo de manter o notebook interpretável, leve e reprodutível.

## Base de dados

Os dados utilizados têm como origem a base aberta de publicações do DOU. Neste projeto, o recorte final considera:

- documentos vinculados ao Ministério da Defesa;
- publicações associadas à Marinha, ao Exército e à Aeronáutica;
- seções DO1, DO2 e DO3;
- período a partir de 2018.

Após a limpeza e padronização, a base analítica final ficou com mais de 500 mil documentos.

## Estrutura do repositório

Os arquivos mais importantes são:

- [main.ipynb](/Users/ffserro/Desktop/Cursos/PUC/analise-de-dados-boas-praticas/main.ipynb): notebook principal, com todo o fluxo analítico e a entrega final.
- [database_2](/Users/ffserro/Desktop/Cursos/PUC/analise-de-dados-boas-praticas/database_2): arquivos `.parquet` usados como base consolidada do projeto.
- [data_fetcher/pack_downloader.py](/Users/ffserro/Desktop/Cursos/PUC/analise-de-dados-boas-praticas/data_fetcher/pack_downloader.py): utilitário de coleta dos pacotes de dados.

## Como executar

### Opção 1: Google Colab

Esta é a forma mais simples de abrir o projeto.

1. Clique no badge `Open in Colab` no topo deste README.
2. Execute o notebook [main.ipynb](/Users/ffserro/Desktop/Cursos/PUC/analise-de-dados-boas-praticas/main.ipynb) do início ao fim.
3. A primeira célula cuida da preparação do ambiente.

### Opção 2: ambiente local

Se quiser rodar localmente:

```bash
uv sync
uv run jupyter lab
```

ou, com `pip`:

```bash
pip install .
jupyter lab
```

Depois disso, basta abrir [main.ipynb](/Users/ffserro/Desktop/Cursos/PUC/analise-de-dados-boas-praticas/main.ipynb).

## Principais entregas do notebook

O notebook final contém:

- definição clara do problema e do recorte analítico;
- documentação textual de cada etapa;
- diagnóstico de qualidade da base;
- dicionário dos atributos da tabela final;
- resumo estatístico dos atributos numéricos;
- análise exploratória com gráficos de volume, composição e tamanho textual;
- análise temática baseada em agrupamento de tipos documentais;
- conclusão, limitações e checklist atendida.

## Leitura recomendada

Se alguém abrir este repositório pela primeira vez, a melhor ordem é:

1. ler este README;
2. abrir o notebook principal;
3. percorrer a introdução, a etapa de preparação dos dados e as seções 4, 5 e 7;
4. consultar os scripts auxiliares apenas se houver interesse em entender a coleta e a consolidação da base.

## Observações finais

Este projeto foi desenvolvido com a preocupação de ser legível e executável em um único notebook. Mais do que produzir gráficos, a proposta foi organizar uma análise que contasse uma história coerente sobre os dados e deixasse claras as decisões tomadas ao longo do caminho.
