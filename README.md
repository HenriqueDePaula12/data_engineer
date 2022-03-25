Olá! 

Esse é o meu primeiro repo tratando de fim a fim, uma pipeline de dados abertos do governo brasileiro relacionado a compras de contrato e cronogramas anuais com spark, em pyspark e SQL! 

O código se encontra [aqui](spark_application/etl.py) e o dado pode ser obtido por meio desse [link](http://repositorio.dados.gov.br/seges/comprasnet_contratos/anual)

```python

from pyspark.sql import SparkSession

##################################################### VARIABLES #####################################################

PATH_LANDING_ZONE_CSV = '../datalake/landing/comprasnet-contratos-anual-cronogramas-latest.csv'
PATH_PROCESSING_ZONE = '../datalake/processing'
PATH_CURATED_ZONE = '../datalake/curated'

##################################################### QUERY #########################################################

QUERY = """ 

WITH tmp as (
  SELECT 
    cast(id as integer) as id,
    cast(contrato_id as integer) as contrato_id,
    tipo,
    numero,
    receita_despesa,
    observacao,
    mesref,
    anoref,
    cast(vencimento as date) as vencimento,
    retroativo,
    cast(valor as decimal (10,2)) as valor,
    year(vencimento) as year,
    month(vencimento) as month,
    dayofmonth(vencimento) as day
  FROM 
    df
)
SELECT
  *
FROM 
  tmp
WHERE   
  year = 2021 OR 
  year = 2022
ORDER BY
  year desc

"""

##################################################### SCRIPT #########################################################

def csv_to_parquet(spark, path_csv, path_parquet):
  df = spark.read.option('header', True).csv(path_csv)
  return df.write.mode('overwrite').format('parquet').save(path_parquet)

def create_view(spark, path_parquet):
  df = spark.read.parquet(path_parquet) 
  df.createOrReplaceTempView('df')

def write_curated(spark, path_curated):
 
  df2 = spark.sql(QUERY)
    
  (
      df2
      .orderBy('year', ascending=False)
      .orderBy('month', ascending=False)
      .orderBy('day', ascending=False)
      .write.partitionBy('year','month','day')
      .mode('overwrite')
      .format('parquet')
      .save(path_curated)
  )


if __name__ == "__main__":
  
  spark = (
    SparkSession.builder
    .master("local[*]")
    .getOrCreate()
  )

  spark.sparkContext.setLogLevel("ERROR")
  
  csv_to_parquet(spark, PATH_LANDING_ZONE_CSV, PATH_PROCESSING_ZONE)

  create_view(spark, PATH_PROCESSING_ZONE)
  
  write_curated(spark, PATH_CURATED_ZONE )

```

- Basicamente, extraimos os dados para a **zona landing**, depois, escrevemos o mesmo dado em diferente formato na zona processing, no caso parquet, por se tratar de um formato otimizado e mais leve.
- Após, criamos uma view do dado recém salvo na **zona processing**, já em parquet, que otimiza a leitura do spark, aplicamos uma query de transformação que enriquece o schema do dado e seleciona apenas os dados de 2021 e 2022, já pronto para ser consumido.
- E por fim, escrevemos na **zona curated** o dado já tratado, enriquecido, particionado por ano, mês e dia e pronto para consumo.

Para rodar o script, basicamente você pode fazer no terminal:

```bash

spark-submit etl.py

```

Você também encontrará o mesmo código e ideia de ETL em **notebooks**, em versão [pyspark](notebooks_colab/ETL_PYSPARK_SIMPLE_CSV.ipynb) ou [spark-sql](notebooks_colab/ETL_SQL_SPARK_SIMPLE_CSV.ipynb).

Espero que gostem!

Qualquer dúvida, entrar em contato pelo [LinkedIn](https://www.linkedin.com/in/henrique-de-paula-6613581b6/).

:)
