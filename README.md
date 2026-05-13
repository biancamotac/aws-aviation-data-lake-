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

<img width="1536" height="1024" alt="2fdc799c-a1e2-41ba-9b56-19d7142c367c" src="https://github.com/user-attachments/assets/3e148fca-d26d-4eed-bfe3-3082f9cc93a6" />



---

# Estrutura do Bucket S3

<img width="1622" height="529" alt="OBJETOSaws" src="https://github.com/user-attachments/assets/ad172cb8-5ac4-49ab-95ae-2cc06a32c4ac" />

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

Dataset utilizado: Opendata AIG Brazil

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

<img width="1615" height="332" alt="raw" src="https://github.com/user-attachments/assets/b91aacbf-f3e2-4d89-888e-dc4c6b3f834a" />


### Arquivo CSV original

<img width="1616" height="343" alt="raw_a_a" src="https://github.com/user-attachments/assets/b6626d98-6987-4e07-9bd8-155ff7ee50fa" />


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

<img width="1612" height="331" alt="processed" src="https://github.com/user-attachments/assets/2777f038-e06b-4e29-871e-59005d63d2f6" />

### Estrutura particionada

<img width="1624" height="651" alt="aviation-processed" src="https://github.com/user-attachments/assets/6676e3af-e2e1-4ee0-9377-57da895dc490" />

### Exemplo de partições por ano

<img width="1621" height="689" alt="ano-processed" src="https://github.com/user-attachments/assets/e095cb7c-c8df-46bf-8f9e-67c997a30d76" />

### Exemplo de partições por mês

<img width="1605" height="755" alt="mes-processed" src="https://github.com/user-attachments/assets/b4bdbcd0-040c-4982-be76-a6208719e1c8" />

### Caminho S3

```text
s3://bianca-aviation-data-lake/processed/
```

---

## Camada Gold (Curated)

Datasets analíticos otimizados para consultas e dashboards.

### Estrutura da camada curated

<img width="1606" height="393" alt="curated" src="https://github.com/user-attachments/assets/e0caddb3-6cc4-4b8b-99d9-f52fbc9e5ae0" />

### Dataset de acidentes por estado

<img width="1616" height="356" alt="ape" src="https://github.com/user-attachments/assets/7052821f-2663-46a5-b129-0323b30842d8" />

### Dataset de fatalidades por fabricante

<img width="1624" height="362" alt="fpf" src="https://github.com/user-attachments/assets/867e0a01-f142-4e79-b837-5436e589ae2e" />

### Dataset de evolução temporal

<img width="1603" height="348" alt="et" src="https://github.com/user-attachments/assets/4c6553f0-c1d6-4a33-b438-6311dec14abc" />

### Caminho S3

```text
s3://bianca-aviation-data-lake/curated/
```

---

# AWS Glue Crawlers

Os crawlers foram utilizados para catalogar automaticamente os dados nas camadas Bronze, Silver e Gold.

<img width="1661" height="308" alt="crawlers" src="https://github.com/user-attachments/assets/494a8204-3962-46fc-9bff-8b5cde5b7b6d" />

---

# AWS Glue Jobs

Os pipelines ETL foram desenvolvidos utilizando AWS Glue e PySpark.

## Jobs criados

- etl-aviation-parquet
- etl-gold-layer

<img width="1621" height="481" alt="glue" src="https://github.com/user-attachments/assets/89b60931-f7a8-41cc-90f8-250054838759" />

### Execução do Glue Job Gold

<img width="1617" height="362" alt="etlgold" src="https://github.com/user-attachments/assets/b194db2d-9fc7-4dad-a566-74afae543b83" />

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

<img width="1538" height="779" alt="consulta 1" src="https://github.com/user-attachments/assets/7d1454be-76e6-46d9-8bdc-a43d09318fe2" />

---

## Consulta de acidentes em 2018

```sql
SELECT *
FROM aviation
WHERE ano = '2018';
```

<img width="1537" height="790" alt="consulta 2" src="https://github.com/user-attachments/assets/bfea9a92-0c56-4f32-93c2-f8741a3806a3" />

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
