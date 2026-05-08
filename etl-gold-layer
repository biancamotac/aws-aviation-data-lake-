from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


df = spark.read.parquet(
    "s3://bianca-aviation-data-lake-2026-578101931561-sa-east-1-an/processed/aviation/"
)



df_estado = df.groupBy("ocorrencia_uf") \
    .agg(count("*").alias("total_acidentes"))

df_estado.write.mode("overwrite").parquet(
    "s3://bianca-aviation-data-lake-2026-578101931561-sa-east-1-an/curated/acidentes_por_estado/"
)



df_fatalidades = df.groupBy("aeronave_fabricante") \
    .agg(
        sum(
            col("quantidade_fatalidades").cast("int")
        ).alias("fatalidades")
    )

df_fatalidades.write.mode("overwrite").parquet(
    "s3://bianca-aviation-data-lake-2026-578101931561-sa-east-1-an/curated/fatalidades_por_fabricante/"
)



df_ano = df.groupBy("ano") \
    .agg(count("*").alias("total_acidentes"))

df_ano.write.mode("overwrite").parquet(
    "s3://bianca-aviation-data-lake-2026-578101931561-sa-east-1-an/curated/evolucao_temporal/"
)
