# Databricks notebook source
# MAGIC %md
# MAGIC ### 🔍 Notebook Overview
# MAGIC This notebook demonstrates best practices for organizing a Databricks notebook, including proper markdown usage, code structuring, transformation, and logging updates. 
# MAGIC
# MAGIC #### 📌 Key Elements:
# MAGIC - **Purpose:** Showcasing a structured approach to Databricks notebooks.
# MAGIC - **Source:** A dummy source table `source_table`.
# MAGIC - **Target:** A dummy target table `target_table`.
# MAGIC - **Parameters:** Job-specific parameters for flexibility in execution.
# MAGIC
# MAGIC ### 📅 Change Log
# MAGIC Maintaining a change log helps track all updates and changes made to the notebook over time. It ensures accountability and makes it easier for others to follow the development progress.
# MAGIC
# MAGIC
# MAGIC | Date       | Author          | Description                              |User Story| Version |
# MAGIC |------------|-----------------|------------------------------------------|----------|---------|
# MAGIC | 2024-10-02 | Saurav Chaudhary | Initial creation of the notebook         |<a href="https://adb-7024489078919155.15.azuredatabricks.net/?o=7024489078919155#notebook/1830848775887419/command/1830848775887420">Create best practices notebook</a>| v1.0    |
# MAGIC | 2024-10-03 | Saurav Chaudhary | Added transformation logic and target write |           | v1.1    |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Ingesting Source Data
# MAGIC The source data is fetched from `source_table`. We apply a filter to select the necessary records before applying transformations.
# MAGIC
# MAGIC #### Source Details:
# MAGIC - **Source Layer:** Bronze
# MAGIC - **Source Table:** `source_table`
# MAGIC - **Source Type:** Delta Table
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.secrets.get(scope="CELEBAL-SECRETS", key="CELEBAL-ADB-CLIENT-ID").reverse
# MAGIC dbutils.secrets.get(scope="CELEBAL-SECRETS", key="CELEBAL-ADB-SECRET-VALUE").reverse

# COMMAND ----------

from UTILITIES.error_logger import DatabricksLogger

# COMMAND ----------

# DBTITLE 1,importing utilities
# MAGIC %run ./UTILITIES/common_utilities

# COMMAND ----------

# DBTITLE 1,fetching widgets parameters
# Define source data
# Input 1: environment 
dbutils.widgets.text(name="environment", defaultValue="", label="environment")
environment = dbutils.widgets.get("environment")

# Input 2: catalog 
dbutils.widgets.text(name="catalog", defaultValue="", label="catalog")
catalog = dbutils.widgets.get("catalog")

# Input 3: schema 
dbutils.widgets.text(name="schema", defaultValue="", label="schema")
schema = dbutils.widgets.get("schema")

# Input 4: source_table
dbutils.widgets.text(name="source_table", defaultValue="", label="source_table")
source_table = dbutils.widgets.get("source_table")

# Input 4: target_table
dbutils.widgets.text(name="target_table", defaultValue="", label="target_table")
target_table = dbutils.widgets.get("target_table")


#create Logger object
logger = DatabricksLogger()
response_dict={}
error = DatabricksErrorHandler(response_dict)

# COMMAND ----------

try:
    source_table=f"{catalog}.{schema}.{source_table}"
    target_table=f"{catalog}.{schema}.{target_table}"
    # Create Dataframe
    source_df=ingest_data(source_table)
    #table exits
    logger.log_info(f"{source_table} Table exists.")
except Exception as e:
    logger.error(f"Error while applying transformations: {str(e)}")
    error.handle_error(e) 
    # pass

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 Data Transformation
# MAGIC This section applies necessary transformations based on business logic. Transformations include data cleaning, formatting, and mapping to the target schema.
# MAGIC
# MAGIC #### Transformation Details:
# MAGIC - **Filter criteria:** Filter data for the last 7 days.
# MAGIC - **Aggregation:** Aggregate data to prepare for the target table.
# MAGIC - **Target Table:** Data will be written to `target_table` in the Silver layer.
# MAGIC

# COMMAND ----------

# DBTITLE 1,transformations
# Applying transformations
try:
    # logger.log_info("Starting transformations on source_df.")

    transformations = [
    ("Stock", col("Company")), 
    ("Time", to_timestamp(col("Time"), 'yyyy-MM-dd HH:mm:ss')),
    ("Day", dayofmonth(col("Time"))),
    ("Date", date_format(col("Time"), "yyyy-MM-dd")),
    ("Month", date_format(col("Time"), "MMM")),
    ("Price", regexp_replace(col("Price"), "[^0-9.]", "").cast("float")),
    ("percentage_change", regexp_replace(col("percentage_change"), "%", "").cast("double")),
    ("return", col("Price") * (1 + col("percentage_change") / 100)),
    ("earning_per_share", col("return") - col("Price")),
    ("earning_ratio", when(col("earning_per_share") != 0, col("return") / col("earning_per_share")).otherwise(None))]
    
    transformed_df = apply_transformations(source_df, transformations)
    final_df = transformed_df.select('Day', 'Date', 'Month', 'Time', 'Stock', 'Exchange', 'Price', 'percentage_change', 'return', 'earning_per_share', 'earning_ratio')

    # Applying the aggregation function on your source dataframe
    groupby_cols = ['Company', 'Exchange']
    agg_col = 'Price'
    alias = 'Total_Price'
    # Transform the 'Price' column to float for correct aggregation
    transformations_on_source_df=[('Price', col('Price').cast('float'))]

    transformed_df=apply_transformations(source_df, transformations_on_source_df)
    # Call the aggregation function
    aggregated_df = aggregate_data(transformed_df, groupby_cols, agg_col, alias)
    join_condition = [ 'Exchange']
    joined_df = final_df.join(aggregated_df, on=join_condition, how='left')

    # logger.info("Transformations applied successfully.")

except Exception as e:
    # logger.error(f"Error while applying transformations: {str(e)}")
    # ErrorHandler.handle_exception(e) 
    pass


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Writing Transformed Data to Target
# MAGIC Once the transformations are applied, the data is written to the target Delta table in the Gold layer.
# MAGIC
# MAGIC #### Target Details:
# MAGIC - **Target Layer:** Silver
# MAGIC - **Target Table:** `target_table`
# MAGIC

# COMMAND ----------

# DBTITLE 1,writing
# Writing to target
write_data(transformed_df, target_table, overwrite)


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Conclusion
# MAGIC This notebook demonstrates best practices for organizing Databricks notebooks. By following the structure outlined here, data teams can improve code readability, maintainability, and tracking of updates.
# MAGIC
