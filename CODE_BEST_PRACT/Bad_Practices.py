# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

df0=spark.read.table("main.default.trading_data_bronze")

# COMMAND ----------

df0=spark.sql("select * from main.default.trading_data_bronze")
df1=df0.withColumnRenamed("Company", "Stock")\
    .withColumn("Time", to_timestamp(col("Time"), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn("Day", dayofmonth(col("Time")))\
    .withColumn("Date", date_format(col("Time"), "yyyy-MM-dd"))\
    .withColumn("Month", date_format(col("Time"), "MMM"))\
    .withColumn("Price", regexp_replace(col("Price"), "[^0-9.]", ""))\
    .withColumn("Price", col("Price").cast("float"))\
    .withColumn("percentage_change", regexp_replace(col("percentage_change"), "%", "").cast("double"))\
    .withColumn("return", col("Price") * (1 + col("percentage_change") / 100))\
    .withColumn("earning_per_share", col("return") - col("Price"))\
    .withColumn("earning_ratio", when(col("earning_per_share") != 0, col("return") / col("earning_per_share")).otherwise(None))\
    .select('Day','Date','Month','Time','Stock','Exchange','Price','percentage_change','return','earning_per_share','earning_ratio')

# COMMAND ----------

df2= df1.withColumn('Price', col('Price').cast('float')).groupBy( 'Exchange').agg(F.sum('Price').alias('Total_Price'))

# COMMAND ----------

df3=df1.join(df2, on='Exchange', how='left')

# COMMAND ----------

df4=df1.withColumn("Price", round(col("Price"), 0)) \
            .withColumn("return", round(col("return"), 0)) \
            .withColumn("Price_Category",when(col("Price") < 2000, "Low")
                        .when((col("Price") >= 2000) & (col("Price") <= 3000), "Medium")
                        .otherwise("High"))

# COMMAND ----------

from pyspark.sql.window import *

# COMMAND ----------

window_spec = Window.partitionBy("Price").orderBy("Date")
df5=df1.withColumn("Total_Price", F.sum("Price").over(window_spec)) \
                   .withColumn("Average_Price", F.avg("Price").over(window_spec))

# COMMAND ----------

df5.display()

# COMMAND ----------

df5.write.mode('overwrite').saveAsTable("main.default.sample_table_02")

# COMMAND ----------

application_id = get_secret(databricks_scopename, application_id_secret_key)
secret_value = get_secret(databricks_scopename, application_id_secret_value)

# COMMAND ----------

directory_id = "e4e34038-ea1f-4882-b6e8-ccd776459ca0"
directory = f"https://login.microsoftonline.com/{directory_id}/oauth2/token"

# Configure Spark to use OAuth for authentication with the storage account
spark.conf.set(f"fs.azure.account.auth.type.unitycatalog12.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.unitycatalog12.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.unitycatalog12.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.unitycatalog12.dfs.core.windows.net", secret_value)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.unitycatalog12.dfs.core.windows.net", directory)

# COMMAND ----------

path=f'abfss://hft@unitycatalog12.dfs.core.windows.net/CODE_BEST_PRACTICES/BAD_PRACTICES/11-10-2024/'
df5.write.mode('overwrite').save(path)
