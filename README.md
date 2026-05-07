# Data Lake de Acidentes Aeronáuticos Brasileiros na AWS

## Visão Geral

Este projeto demonstra a implementação de uma arquitetura moderna de Data Lake serverless na AWS utilizando dados reais de acidentes aeronáuticos brasileiros.

O pipeline foi desenvolvido para realizar ingestão, catalogação, transformação, otimização e análise de dados públicos utilizando serviços cloud-native da AWS e boas práticas de Engenharia de Dados.

---

# Arquitetura

```text
Dados CSV Brutos
        ↓
Amazon S3 (Camada Raw)
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
Dashboard Power BI
```

---

# Tecnologias Utilizadas

| Tecnologia | Finalidade |
|---|---|
| AWS S3 | Armazenamento Data Lake |
| AWS Glue | ETL e catálogo de dados |
| Amazon Athena | Consultas SQL serverless |
| PySpark | Transformação de dados |
| Power BI | Visualização e dashboards |
| SQL | Consultas analíticas |
| Parquet | Formato otimizado de armazenamento |

---

# Dataset

Dataset utilizado: Brazilian Aviation Accident Dataset

O conjunto de dados contém:
- ocorrências aeronáuticas
- informações das aeronaves
- localização geográfica
- registros de fatalidades
- fases operacionais
- fatores contribuintes

---

# Objetivos do Projeto

- Construir uma arquitetura cloud-native de Data Lake
- Implementar pipelines ETL com PySpark
- Otimizar consultas analíticas utilizando Parquet
- Aplicar estratégias de particionamento
- Criar uma solução escalável e de baixo custo
- Demonstrar práticas modernas de Engenharia de Dados na AWS

---

# Camadas do Data Lake

## Camada Raw (Bronze)

Armazena os arquivos CSV originais sem modificações.

```text
s3://bianca-aviation-data-lake/raw/
```

---

## Camada Processed (Silver)

Armazena dados tratados e transformados em formato Parquet.

Transformações realizadas:
- remoção de duplicados
- padronização de datas
- normalização de schema
- criação de partições

```text
s3://bianca-aviation-data-lake/processed/
```

---

## Camada Curated (Gold)

Armazena datasets analíticos otimizados para dashboards e BI.

```text
s3://bianca-aviation-data-lake/curated/
```

---

# Pipeline ETL

O processo ETL foi desenvolvido utilizando PySpark e AWS Glue.

Principais transformações:
- ingestão de CSV do S3
- remoção de duplicados
- conversão de datas
- criação de colunas de partição
- conversão para formato Parquet
- armazenamento otimizado para consultas Athena

---

# Exemplo de ETL em PySpark

```python
from pyspark.sql.functions import *

df = spark.read.csv(
    "s3://bianca-aviation-data-lake/raw/aviation_accidents/",
    header=True,
    inferSchema=True
)

df = df.dropDuplicates()

df = df.withColumn(
    "ocorrencia_dia",
    to_date(col("ocorrencia_dia"))
)

df = df.withColumn(
    "ano",
    year(col("ocorrencia_dia"))
)

df = df.withColumn(
    "mes",
    month(col("ocorrencia_dia"))
)

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
ano=2024/mes=5/
```

Benefícios:
- redução de custo no Athena
- melhoria de performance
- arquitetura escalável

---

# Consultas Athena

## Acidentes por Estado

```sql
SELECT
    ocorrencia_uf,
    COUNT(*) acidentes
FROM aviation_processed
GROUP BY ocorrencia_uf
ORDER BY acidentes DESC;
```

---

## Fatalidades por Fabricante

```sql
SELECT
    aeronave_fabricante,
    SUM(quantidade_fatalidades) fatalidades
FROM aviation_processed
GROUP BY aeronave_fabricante
ORDER BY fatalidades DESC;
```

---

## Evolução Temporal dos Acidentes

```sql
SELECT
    ano,
    COUNT(*) total
FROM aviation_processed
GROUP BY ano
ORDER BY ano;
```

---

# Otimização de Custos

O projeto foi desenvolvido utilizando arquitetura serverless para minimizar custos operacionais.

Estratégias utilizadas:
- formato Parquet
- particionamento
- consultas serverless no Athena
- processamento sob demanda

Estimativa de custo mensal:
- aproximadamente US$1–5

---

# Melhorias Futuras

- Infraestrutura como código com Terraform
- Incremental Load
- Orquestração com Glue Workflows
- Monitoramento com CloudWatch
- Integração CI/CD
- Validação de qualidade dos dados

---

# Dashboard

O dashboard analítico apresenta insights sobre:
- distribuição de acidentes
- análise de fatalidades
- fabricantes de aeronaves
- evolução temporal
- concentração geográfica

---

# Competências Demonstradas

- Engenharia de Dados
- AWS Cloud
- ETL
- Arquitetura Data Lake
- PySpark
- SQL Analytics
- Otimização Cloud
- Modelagem de Dados
- Analytics Serverless

---

# Autora

Bianca Mota  
Estudante de Engenharia de Dados e Análise de Dados
