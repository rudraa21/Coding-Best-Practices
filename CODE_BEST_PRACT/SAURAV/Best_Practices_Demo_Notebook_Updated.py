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

# from UTILITIES.error_logger import DatabricksLogger

# COMMAND ----------

# DBTITLE 1,importing utilities
# MAGIC %run ./UTILITIES/common_utilities

# COMMAND ----------

# DBTITLE 1,fetching widgets parameters
# Define source data
# Input 1: environment 
dbutils.widgets.text(name="environment", defaultValue="", label="environment")
environment = dbutils.widgets.get("environment")

# Input 2: container 
dbutils.widgets.text(name="container", defaultValue="hft", label="container")
container = dbutils.widgets.get("container")

# Input 3: storage_account 
dbutils.widgets.text(name="storage_account", defaultValue="unitycatalog12", label="storage_account")
storage_account = dbutils.widgets.get("storage_account")

# Input 4: catalog 
dbutils.widgets.text(name="catalog", defaultValue="", label="catalog")
catalog = dbutils.widgets.get("catalog")

# Input 5: schema 
dbutils.widgets.text(name="schema", defaultValue="", label="schema")
schema = dbutils.widgets.get("schema")

# Input 6: source_table
dbutils.widgets.text(name="source_table", defaultValue="", label="source_table")
source_table_name = dbutils.widgets.get("source_table")

# Input 7: target_table
dbutils.widgets.text(name="target_table", defaultValue="", label="target_table")
target_table_name = dbutils.widgets.get("target_table")

# Input 8: target_format
dbutils.widgets.text(name="target_format", defaultValue="", label="target_format")
target_format = dbutils.widgets.get("target_format")

#create Logger object
logger = DatabricksLogger()

#create error handler object
response_dict={}
error = DatabricksErrorHandler(response_dict)

# COMMAND ----------

try:
    source_table=f"{catalog}.{schema}.{source_table_name}"
    target_table=f"{catalog}.{schema}.{target_table_name}"
    table_exists = spark.catalog.tableExists(source_table)
    if check_table_exists(source_table):
        logger.log_info(f"{source_table} Table exists.")
        # Create Dataframe
        source_df=ingest_data(source_table)
        logger.log_info(f"source_df created.")
        error.handle_success(should_exit=False)
    else:
        logger.log_error(f"{source_table} Table does not exist.")
        error.handle_success(should_exit=True)
        
except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    error.handle_error(e) 

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

try:
    logger.log_info("Starting transformations on source_df.")

    source_df_with_date =  convert_timestamp_to_date(source_df, "time", "yyyy-MM-dd")

    transformations = [
                        ("Stock", col("Company")), 
                        ("Day", dayofmonth(col("Time"))),
                        ("Month", date_format(col("Time"), "MMM")),
                        ("Price", regexp_replace(col("Price"), "[^0-9.]", "").cast("float")),
                        ("percentage_change", regexp_replace(col("percentage_change"), "%", "").cast("double")),
                        ("return", col("Price") * (1 + col("percentage_change") / 100)),
                        ("earning_per_share", col("return") - col("Price")),
                        ("earning_ratio", when(col("earning_per_share") != 0, col("return") / col("earning_per_share")).otherwise(None))
                    ]
    
    transformed_df = apply_transformations(source_df_with_date, transformations)
    transformed_df1 = transformed_df.select('Day', 'Date', 'Month', 'Time', 'Stock', 'Exchange', 'Price', 'percentage_change', 'return', 'earning_per_share', 'earning_ratio')

    logger.log_info("Transformations applied successfully.")
    error.handle_success()

except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    error.handle_error(e) 

# COMMAND ----------

# DBTITLE 1,transformations
try:
    logger.log_info("Starting transformations on source_df_with_date.")

    # Applying the aggregation function on your source dataframe
    groupby_cols = ['Company', 'Exchange']
    agg_col = 'Price'
    alias = 'Total_Price'
    # Transform the 'Price' column to float for correct aggregation
    transformations_on_source_df=[('Price', col('Price').cast('float'))]

    transformed_df=apply_transformations(source_df_with_date, transformations_on_source_df)
    # Call the aggregation function
    aggregated_df = aggregate_data(transformed_df, groupby_cols, agg_col, alias)

    #joining performed on transformed_df1 and aggregated_df using left join
    join_condition = [ 'Exchange']
    joined_df = transformed_df1.join(aggregated_df, on=join_condition, how='left')

    logger.log_info("Aggregation and join applied successfully.")
    error.handle_success()

except Exception as e:
    logger.error(f"Error while applying transformations: {str(e)}")
    error.handle_error(e) 

# COMMAND ----------

try:
    logger.log_info("Starting transformations on source_df_with_date.")
    # Window Specification - Partition data by 'company' and order rows by 'company' column and apply row_number() function
    window_spec = Window.partitionBy("company").orderBy("company")
    company_rank = joined_df.withColumn("company_rank", row_number().over(window_spec))

    # mask the sensitive column
    masked_DataFrame = mask_sensitive_data(company_rank , ['earning_ratio'])

    low_threshold = 2000
    high_threshold = 5000

    transformations = [
                        ('Price', round('Price')),
                        ('return', round('return')),
                        ("Price_Category", 
                            when(col("Price") < low_threshold, "Low")
                            .when((col("Price") >= low_threshold) & (col("Price") <= high_threshold), "Medium")
                            .otherwise("High")
                        )
                ]

    transformed_DataFrame = apply_transformations(masked_DataFrame, transformations)
    transformed_DataFrame = transformed_DataFrame.drop('total_price')


    logger.log_info("transformed_dataframe created successfully.")
    error.handle_success()

except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    error.handle_error(e) 

# COMMAND ----------

try:
    logger.log_info("Starting transformations on transformed_DataFrame.")
    # Partitioning the data by 'Company'
    window_spec = Window.partitionBy("Company")

    # 'Total_Price' and 'Average_Price' columns
    transformations = [
                        ('Total_Price', F.sum("Price").over(window_spec)),
                        ('Average_Price',  F.avg("Price").over(window_spec))
                ]

    final_DataFrame = apply_transformations(transformed_DataFrame, transformations)

    logger.log_info("Transformation applied and final_DataFrame created successfully.")
    error.handle_success()

except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    error.handle_error(e) 

# COMMAND ----------

try:
    logger.log_info("Applying column name case to final_DataFrame.")
    # change all column case to upper
    final_DataFrame = change_name_case(final_DataFrame, case='upper')

    logger.log_info("Column name case applied successfully.")
    error.handle_success()

except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    error.handle_error(e) 

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

try:
    logger.log_info(f"Writing data  in {target_format} format")
    option_dictionary= {"header": "true", "delimiter": ","}
    if target_format == 'delta':
        path=target_table
        write_data(final_DataFrame, path,'overwrite',target_format,option_dictionary )
        logger.log_info(f"Data written in {target_format} format at {path}")
    else:
        current_date = datetime.now().strftime("%Y-%m-%d")
        initiate_spn('CELEBAL-SECRETS', storage_account)
        path=f'abfss://{container}@{storage_account}.dfs.core.windows.net/CODE_BEST_PRACTICES/{current_date}'
        write_data(final_DataFrame, path,'overwrite',target_format,option_dictionary )
        logger.log_info(f"Data written in {target_format} format at {path}")
except Exception as e:
    logger.log_error(f"Error while writing data in {target_format} format at {path} : {str(e)}")
    error.handle_error(e) 


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Conclusion
# MAGIC This notebook demonstrates best practices for organizing Databricks notebooks. By following the structure outlined here, data teams can improve code readability, maintainability, and tracking of updates.
# MAGIC
