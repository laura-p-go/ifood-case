# iFood Case — NYC TLC Data Pipeline (Local)

## Visão geral

Este repositório contém uma solução para o case técnico de Data Architecture da iFood, utilizando uma arquitetura de dados em **4 camadas**:

- **Landing**: ingestão e armazenamento dos arquivos brutos
- **Bronze**: padronização inicial dos dados
- **Silver**: limpeza, normalização e preparação para consumo
- **Gold**: criação das tabelas analíticas finais
- **Analysis**: consultas analíticas em **DuckDB** sobre os dados gerados na Gold

A solução foi desenhada para rodar **localmente**, sem dependência de cloud, usando:

- **Python** na camada Landing
- **PySpark** nas camadas Bronze, Silver e Gold
- **DuckDB** na camada de análise

---
## Objetivo do case

O objetivo é construir um pipeline para ingestão dos dados de corridas de táxi de Nova York (TLC Trip Record Data), considerando os arquivos de **janeiro a maio de 2023**, e disponibilizá-los para consumo analítico.

Além disso, o case pede responder às seguintes perguntas:

1. **Qual a média de valor total (`total_amount`) recebido em um mês considerando todos os yellow taxis da frota?**
2. **Qual a média de passageiros (`passenger_count`) por hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?**

A camada de consumo também deve garantir a presença das colunas:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

---
## Descrição das Camadas

### 0. Landing

A camada **Landing** é responsável por armazenar os arquivos brutos originais do dataset de corridas de táxi, sem transformações. Nesta solução, ela é implementada com um script em **Python**, que organiza localmente os arquivos de janeiro a maio de 2023 e os disponibiliza como fonte de entrada para o pipeline em PySpark.

### 1. Bronze

A camada **Bronze** é responsável por receber os arquivos brutos da **Landing** e armazená-los em um formato padronizado dentro do Data Lake local, preservando os dados o mais próximo possível da origem.

Nesta etapa, o pipeline em **PySpark** executa quatro tarefas principais:

- **Leitura dos arquivos da Landing**: carrega todos os arquivos `.parquet` disponíveis na pasta de entrada.
- **Enriquecimento com metadados técnicos**: adiciona colunas de auditoria e rastreabilidade, como:
  - `source_file`: arquivo de origem
  - `ingestion_timestamp`: momento da ingestão
  - `ingestion_date`: data da ingestão
  - `pickup_year` e `pickup_month`: derivados da data de pickup
- **Normalização de schema**: padroniza os arquivos para um único schema, tratando diferenças entre meses, renomeando colunas quando necessário, adicionando colunas ausentes e convertendo os tipos esperados.
- **Escrita particionada em parquet**: salva os dados na pasta `data/bronze/`, particionados por `pickup_year` e `pickup_month`.

O papel da camada bronze no projeto é funcionar como uma camada de **persistência confiável e padronizada**, sem aplicar regras de negócio ou agregações analíticas. Assim, ela prepara os dados para a **Silver**, garantindo rastreabilidade, consistência estrutural e melhor organização física para as próximas etapas do pipeline.

### 2. Silver

A camada **Silver** é responsável por transformar os dados da camada **Bronze** em um dataset limpo, tipado e pronto para consumo analítico. Nessa etapa, o pipeline em **PySpark** aplica curadoria sobre os dados de corridas de yellow taxi, selecionando as colunas relevantes para o case, aplicando regras de qualidade e criando atributos derivados que serão utilizados nas análises da Gold.

As principais etapas da Silver são:

- **Leitura da camada bronze**: carrega os dados já padronizados da camada anterior.
- **Seleção e tipagem das colunas de negócio**: mantém apenas as colunas relevantes para o projeto, como `VendorID`, `passenger_count`, `trip_distance`, `payment_type`, `fare_amount`, `tip_amount`, `tolls_amount`, `total_amount`, `tpep_pickup_datetime` e `tpep_dropoff_datetime`, além das colunas técnicas `source_file` e `ingestion_timestamp`.
- **Aplicação de regras de qualidade**: remove registros com valores nulos em campos essenciais, como pickup, dropoff, total amount e passenger count, além de filtrar inconsistências como quantidade de passageiros negativa e viagens em que o horário de dropoff é anterior ao pickup.
- **Criação de colunas derivadas**: adiciona atributos úteis para análise e particionamento, como `pickup_date`, `pickup_hour` e `trip_duration_minutes`.
- **Escrita particionada em parquet**: salva os dados da camada Silver em `data/silver/`, particionados por `pickup_year` e `pickup_month`.

O papel da camada Silver no projeto é consolidar uma visão confiável e analítica dos dados, eliminando inconsistências da camada anterior e preparando a base que será utilizada pela **Gold** para gerar as métricas finais do case.

### 3. Gold

A camada **Gold** é responsável por transformar os dados curados da camada **Silver** em tabelas analíticas prontas para consumo. Nessa etapa, o pipeline em **PySpark** aplica agregações de negócio para responder diretamente às perguntas propostas no case.

Nesta solução, a camada Gold é composta por **duas tabelas analíticas**, cada uma voltada para uma métrica específica:

- **`gold_monthly_metrics`**: agrega os dados por `pickup_year` e `pickup_month`, calculando métricas mensais como:
  - quantidade de corridas (`trip_count`)
  - média de valor total recebido (`avg_total_amount`)
  - soma do valor total recebido (`sum_total_amount`)
  - média de passageiros por corrida (`avg_passenger_count`)

  Essa tabela é utilizada para responder à pergunta:
  **“Qual a média de valor total (`total_amount`) recebido em um mês considerando todos os yellow taxis da frota?”**

- **`gold_may_hourly_passenger_metrics`**: filtra apenas as corridas do mês de maio e agrega os dados por `pickup_hour`, calculando:
  - quantidade de corridas por hora (`trip_count`)
  - média de passageiros por hora (`avg_passenger_count`)

  Essa tabela é utilizada para responder à pergunta:
  **“Qual a média de passageiros (`passenger_count`) por hora do dia que pegaram táxi no mês de maio?”**

As duas tabelas são gravadas em formato **Parquet** e representam a camada final de consumo do pipeline, servindo como base para as consultas analíticas realizadas posteriormente no **DuckDB**.

---
## Modelagem de dados

A solução adota uma arquitetura de **Data Lake em camadas** (**Landing, Bronze, Silver e Gold**) com foco em consumo analítico.

A camada **Silver** funciona como uma base refinada em nível transacional, na qual cada registro representa uma corrida de táxi já tratada e padronizada, contendo medidas como `passenger_count`, `trip_distance`, `fare_amount` e `total_amount`, além de atributos temporais derivados da data de pickup.

Já a camada **Gold** funciona como uma camada de **data marts analíticos agregados**, construída a partir da Silver para responder diretamente às perguntas de negócio do case. Nessa camada, os dados deixam de estar em granularidade de corrida e passam a ser organizados em tabelas resumidas, como métricas mensais e métricas horárias para o mês de maio.

Dessa forma, o projeto não segue um modelo dimensional clássico completo (como um esquema estrela com fato e dimensões separadas), mas sim uma modelagem analítica em camadas, com uma base detalhada na Silver e tabelas agregadas orientadas a consumo na Gold.

---

## Arquitetura da solução

A solução foi estruturada em um **Data Lake local em camadas**, separando responsabilidades entre ingestão, transformação, disponibilização e análise.

```text
ifood-case/
├─ data/
│  ├─ landing/         # arquivos brutos originais
│  ├─ bronze/          # dados ingeridos e padronizados
│  ├─ silver/          # dados limpos e normalizados
│  └─ gold/            # tabelas analíticas finais
│
├─ src/
│  ├─ landing/
│  │  └─ landing.py
│  ├─ bronze/
│  │  └─ bronze.py
│  ├─ silver/
│  │  └─ silver.py
│  ├─ gold/
│  │  └─ gold.py
│  └─ utils/
│     └─ spark.py
│
├─ analysis/
│  └─ analysis.py      # consultas em DuckDB
│
├─ requirements.txt
└─ README.md
```

---

## Evolução da solução para nuvem

Embora esta implementação tenha sido desenvolvida para **execução local**, a mesma arquitetura pode ser adaptada para um ambiente em nuvem com poucas mudanças conceituais. A separação em camadas (**Landing, Bronze, Silver e Gold**) já segue um padrão compatível com arquiteturas de Data Lake em cloud.

Uma possível evolução seria:

- **Landing**: armazenar os arquivos brutos em um bucket de objetos, como **Amazon S3**, mantendo os dados originais por mês.
- **Bronze / Silver / Gold**: executar as transformações em um motor distribuído gerenciado, como **Databricks**, **AWS Glue** ou **EMR**, mantendo a mesma lógica de processamento implementada em PySpark.
- **Catálogo e metadados**: registrar as tabelas em um catálogo como **AWS Glue Data Catalog** ou **Unity Catalog**, facilitando descoberta e governança dos dados.
- **Consumo analítico**: disponibilizar a camada Gold para consulta em ferramentas SQL, como **Athena**, **Databricks SQL** ou um data warehouse como **Snowflake** / **BigQuery**, dependendo da stack adotada.
- **Orquestração**: agendar e monitorar o pipeline com ferramentas como **Airflow**, **Prefect** ou os próprios jobs do Databricks.

---

### Exemplo de mapeamento local → cloud

| Camada / Componente | Implementação local | Possível equivalente em nuvem |
|---|---|---|
| Landing   | Pasta local com arquivos parquet | Bucket S3 / Azure Data Lake / GCS |
| Bronze/Silver/Gold   | Scripts PySpark executados local | Databricks Jobs / AWS Glue / EMR |
| Metadados   | Estrutura de pastas e parquet | Glue Catalog / Unity Catalog |
| Analysis   | DuckDB local | Athena / Databricks SQL / Snowflake / BigQuery |

### Fluxo em cloud

1. Um processo de ingestão envia os arquivos brutos para a **Landing** em um bucket.
2. Um job em **PySpark** processa os arquivos da Landing e grava a camada **Bronze** em parquet.
3. Um segundo job aplica limpeza e regras de qualidade, gerando a camada **Silver**.
4. Um terceiro job cria as tabelas agregadas da **Gold**.
5. As tabelas Gold ficam disponíveis para consumo por ferramentas analíticas e consultas SQL.
