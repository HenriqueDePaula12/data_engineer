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