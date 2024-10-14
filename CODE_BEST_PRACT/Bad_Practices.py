# Databricks notebook source
# Importing all functions from pyspark.sql.functions is a bad practice
from pyspark.sql.functions import *
# Importing all types from pyspark.sql.types is also a bad practice
from pyspark.sql.types import *

# - Importing everything with '*' can cause namespace pollution. You might 
#    accidentally overwrite functions or create conflicts between similarly named functions.
# - It's harder to track which functions or types are actually being used, reducing code readability.
# - Loading all functions and types can introduce slight performance overhead.
# - No parameterization: Hardcoded values make the code less flexible and harder to reuse.
# - No user input or external configuration: The code doesn’t allow dynamic inputs, which limits adaptability.
# - No use of Databricks widgets: Widgets can help make the notebook interactive by allowing users to input values.
# - Lack of inline comments or markdowns: The code doesn't include comments or markdown cells to explain the functionality, making it harder for others to understand and maintain.

# COMMAND ----------

df0 = spark.read.table("databricks_champ.coding_practices.trading_data_bronze")

# Bad practice:
# - Non-descriptive variable name 'df0'
# - No error handling
# - No schema validation

# COMMAND ----------

df0.display()

# Bad practice:
# - Using display() on a large dataframe can be inefficient and slow
# - Should limit or sample data when displaying large datasets

# COMMAND ----------

df0 = spark.sql("select * from databricks_champ.coding_practices.trading_data_bronze")
df1 = df0.withColumnRenamed("Company", "Stock")\
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

# Bad practices:
# - Redundant SQL query: 'spark.read.table' should be used instead of 'spark.sql'
# - Multiple transformations done in one long chain: makes debugging difficult

# COMMAND ----------

df2 = df1.withColumn('Price', col('Price').cast('float')).groupBy('Exchange').agg(F.sum('Price').alias('Total_Price'))

# Bad practices:
# - Unnecessary casting of 'Price' to float again (already done earlier)


# COMMAND ----------

df3 = df1.join(df2, on='Exchange', how='left')
df3.count()

# Bad practices:
# - Performing a `count()` immediately after a join: inefficient and expensive on large datasets
# - No validation or check on the join operation

# COMMAND ----------

df4 = df1.withColumn("Price", round(col("Price"), 0)) \
         .withColumn("return", round(col("return"), 0)) \
         .withColumn("Price_Category", when(col("Price") < 2000, "Low")
             .when((col("Price") >= 2000) & (col("Price") <= 3000), "Medium")
             .otherwise("High"))

# COMMAND ----------

from pyspark.sql.window import *

# Bad practice:
# - Importing inbetween the notebook
# - Importing everything with '*' 
# - Reduces code clarity and readability

# COMMAND ----------

window_spec = Window.partitionBy("Price").orderBy("Date")
df5=df1.withColumn("Total_Price", F.sum("Price").over(window_spec)) \
                   .withColumn("Average_Price", F.avg("Price").over(window_spec))

# COMMAND ----------

df5.display()

# Bad practice:
# - Using display() on a large dataframe can be inefficient and slow
# - Should limit or sample data when displaying large datasets

# COMMAND ----------

df5.write.mode('overwrite').saveAsTable("databricks_champ.coding_practices.silver_table")

# Bad practices:
# - No error handling or logging for the write operation
# - Hardcoding the table name can reduce flexibility and make refactoring harder

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.celeballearning.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.celeballearning.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.celeballearning.dfs.core.windows.net", "01494e40-236d-4bcc-bd7b-003858bf4406")
spark.conf.set("fs.azure.account.oauth2.client.secret.celeballearning.dfs.core.windows.net", "D328Q~W.OSo6XJqRODYmm29EOm2hMLaObGqKcbkb")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.celeballearning.dfs.core.windows.net", "https://login.microsoftonline.com/e4e34038-ea1f-4882-b6e8-ccd776459ca0/oauth2/token")

# Bad practices:
# - Hardcoding sensitive credentials (client ID and secret) directly in code: leads to security risks
# - No secure credential management or environment variables used to protect sensitive information
# - Lack of error handling during configuration setup: issues may go unnoticed
# - Potential for configuration errors due to hardcoded values, making it less maintainable

# COMMAND ----------

df5.write.format('csv').mode('overwrite').save('abfss://code-best-practices@celeballearning.dfs.core.windows.net/code-best-practices/BAD_PRACTICES/14-10-2024/')

# Bad practices:
# - Hardcoding the file path: reduces flexibility and makes code less maintainable
# - No error handling or logging for the write operation
# - No configuration for CSV options (like header inclusion, delimiter, etc.), which may lead to inconsistencies
# - Writing directly to a storage location without validating the data or its format first
