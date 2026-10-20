import dlt
from pyspark.sql.functions import current_timestamp

@dlt.table(
    comment="Raw wine data ingested from CSV files"
)
def bronze_wines():
    return(
        spark.read
        .format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load("/Volumes/workspace/default/raw/inventory/wine-prices.csv")
        .drop("_c0") #drops problematic index column
        .withColumn("ingestion_time",current_timestamp())
    )

@dlt.table(
    comment="Raw price updates from supplier feeds"
)
def bronze_price_updates():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("cloudFiles.inferColumnTypes","true")
        .option("header","true")
        .load("/Volumes/workspace/default/raw/inventory/price_updates/")
        .withColumn("ingestion_time",current_timestamp())
    )