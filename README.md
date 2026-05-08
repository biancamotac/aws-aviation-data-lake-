# Data Lake de Acidentes Aeronáuticos Brasileiros na AWS

## Visão Geral

Este projeto demonstra a implementação de uma arquitetura moderna de Data Lake serverless na AWS utilizando dados reais de acidentes aeronáuticos brasileiros.

O pipeline foi desenvolvido para realizar ingestão, catalogação, transformação, otimização e análise de dados utilizando serviços cloud-native da AWS e boas práticas de Engenharia de Dados.

---

# Arquitetura

```text
Dados CSV Brutos
        ↓
Amazon S3 (Camada Bronze / Raw)
        ↓
AWS Glue Crawler
        ↓
Glue Data Catalog
        ↓
AWS Glue ETL (PySpark)
        ↓
Parquet + Particionamento
        ↓
Amazon Athena
        ↓
Camada Gold (Curated)
        ↓
Dashboard Power BI
```

---

# Estrutura do Bucket S3

![Bucket Structure](images/estrutura_bucket.png)

---

# Tecnologias Utilizadas

| Tecnologia | Finalidade |
|---|---|
| AWS S3 | Data Lake |
| AWS Glue | ETL e Catalogação |
| Amazon Athena | Consultas SQL serverless |
| PySpark | Transformação de dados |
| SQL | Consultas analíticas |
| Parquet | Formato otimizado |
| Power BI | Dashboards e visualizações |

---

# Conjunto de Dados

Dataset utilizado: Acidentes aeronáuticos brasileiros.

O conjunto de dados contém:

- Informações de ocorrências aeronáuticas
- Dados das aeronaves
- Localização geográfica
- Registros de fatalidades
- Fatores contribuintes
- Dados operacionais de voo

---

# Objetivos do Projeto

- Construir uma arquitetura moderna de Data Lake na AWS
- Implementar pipelines ETL com PySpark
- Utilizar arquitetura serverless
- Aplicar estratégias de particionamento
- Realizar consultas analíticas otimizadas
- Demonstrar práticas modernas de Engenharia de Dados
- Otimizar custos utilizando Parquet e Athena

---

# Camadas do Data Lake

---

## Camada Bronze (Raw)

Armazenamento dos arquivos CSV originais sem transformação.

### Estrutura da camada raw

![Raw Layer](images/raw_layer.png)

### Arquivo CSV original

![Raw CSV](images/raw_csv.png)

### Caminho S3

```text
s3://bianca-aviation-data-lake/raw/
```

---

## Camada Silver (Processed)

Dados tratados e convertidos para formato Parquet com particionamento por ano e mês.

### Transformações realizadas

- Remoção de duplicados
- Conversão de tipos de dados
- Padronização de colunas
- Criação de colunas de particionamento
- Conversão para Parquet

### Estrutura da camada processed

![Processed Layer](images/processed_layer.png)

### Estrutura particionada

![Partitioned Data](images/partitioned_data.png)

### Exemplo de partições por ano

![Year Partition](images/year_partition.png)

### Exemplo de partições por mês

![Month Partition](images/month_partition.png)

### Caminho S3

```text
s3://bianca-aviation-data-lake/processed/
```

---

## Camada Gold (Curated)

Datasets analíticos otimizados para consultas e dashboards.

### Estrutura da camada curated

![Curated Layer](images/curated_layer.png)

### Dataset de acidentes por estado

![Acidentes por Estado](images/acidentes_estado.png)

### Dataset de fatalidades por fabricante

![Fatalidades por Fabricante](images/fatalidades_fabricante.png)

### Dataset de evolução temporal

![Evolução Temporal](images/evolucao_temporal.png)

### Caminho S3

```text
s3://bianca-aviation-data-lake/curated/
```

---

# AWS Glue Crawlers

Os crawlers foram utilizados para catalogar automaticamente os dados nas camadas Bronze, Silver e Gold.

![Glue Crawlers](images/crawlers.png)

---

# AWS Glue Jobs

Os pipelines ETL foram desenvolvidos utilizando AWS Glue e PySpark.

## Jobs criados

- etl-aviation-parquet
- etl-gold-layer

![Glue Jobs](images/glue_jobs.png)

### Execução do Glue Job Gold

![Glue Job Run](images/glue_job_run.png)

---

# Exemplo de ETL em PySpark

```python
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

# iniciar contexto spark
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# leitura CSV
df = spark.read.csv(
    "s3://bianca-aviation-data-lake/raw/aviation_accidents/",
    header=True,
    inferSchema=True
)

# remover duplicados
df = df.dropDuplicates()

# converter data
df = df.withColumn(
    "ocorrencia_dia",
    to_date(col("ocorrencia_dia"))
)

# criar colunas ano e mes
df = df.withColumn(
    "ano",
    year(col("ocorrencia_dia"))
)

df = df.withColumn(
    "mes",
    month(col("ocorrencia_dia"))
)

# salvar parquet particionado
df.write.mode("overwrite") \
    .partitionBy("ano", "mes") \
    .parquet(
        "s3://bianca-aviation-data-lake/processed/aviation/"
)
```

---

# Estratégia de Particionamento

Os dados foram particionados por:

- ano
- mês

Exemplo:

```text
ano=2008/mes=1/
```

## Benefícios

- Redução de custo no Athena
- Melhor desempenho de consultas
- Menor volume de dados escaneados
- Arquitetura escalável

---

# Consultas no Amazon Athena

As análises foram realizadas utilizando SQL serverless no Amazon Athena.

---

## Acidentes por Estado

```sql
SELECT
    ocorrencia_uf,
    COUNT(*) AS total_acidentes
FROM acidentes_por_estado
GROUP BY ocorrencia_uf
ORDER BY total_acidentes DESC;
```

![Athena Query 1](images/athena_query_1.png)

---

## Consulta de acidentes em 2018

```sql
SELECT *
FROM aviation
WHERE ano = '2018';
```

![Athena Query 2](images/athena_query_2.png)

---

# Otimização de Custos

O projeto foi desenvolvido utilizando arquitetura serverless para minimizar custos operacionais.

## Estratégias utilizadas

- Formato Parquet
- Particionamento no S3
- Consultas serverless com Athena
- Processamento sob demanda
- Glue Crawlers automáticos

## Estimativa de custo mensal

```text
Aproximadamente US$ 1–5
```

---

# Estrutura do Projeto

```text
aws-aviation-data-lake/
│
├── README.md
│
├── scripts/
│   ├── etl_aviation_parquet.py
│   └── etl_gold_layer.py
│
├── queries/
│   └── athena_queries.sql
│
├── images/
│   ├── estrutura_bucket.png
│   ├── raw_layer.png
│   ├── raw_csv.png
│   ├── processed_layer.png
│   ├── partitioned_data.png
│   ├── year_partition.png
│   ├── month_partition.png
│   ├── curated_layer.png
│   ├── acidentes_estado.png
│   ├── fatalidades_fabricante.png
│   ├── evolucao_temporal.png
│   ├── crawlers.png
│   ├── glue_jobs.png
│   ├── glue_job_run.png
│   ├── athena_query_1.png
│   └── athena_query_2.png
│
└── dashboard/
    └── powerbi_dashboard.png
```

---

# Melhorias Futuras

- Infraestrutura como código com Terraform
- Pipeline incremental
- Orquestração com Glue Workflows
- Monitoramento com CloudWatch
- Integração CI/CD
- Data Quality
- Dashboards avançados no Power BI

---

# Competências Demonstradas

- Engenharia de Dados
- AWS Cloud
- Data Lake Architecture
- ETL
- PySpark
- SQL Analytics
- Amazon Athena
- AWS Glue
- Amazon S3
- Modelagem de Dados
- Arquitetura Serverless
- Otimização de Consultas

---

# Autora

## Bianca Mota

Estudante de Engenharia de Dados e Análise de Dados.
