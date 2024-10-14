# Databricks notebook source
# MAGIC %md
# MAGIC ### 🔍 Notebook Overview
# MAGIC This notebook demonstrates best practices for organizing a Databricks notebook, focusing on clean markdown usage, code structuring, transformation logic,  comprehensive logging updates and error handling.
# MAGIC #### 📌 Key Elements:
# MAGIC - **Purpose:** Showcases a structured approach to building Databricks notebooks.
# MAGIC - **Source Table:** A dummy source table `source_table` in the Bronze layer.
# MAGIC - **Target Table:** A dummy target table `target_table` in the Silver layer.
# MAGIC - **Parameters:** Customizable job-specific parameters for flexibility in execution and testing.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📅 Change Log
# MAGIC A detailed change log helps maintain clarity about notebook evolution. Tracking changes facilitates collaboration and smooth transition during handovers or updates.
# MAGIC
# MAGIC | Date       | Author          | Description                                | User Story | Version |
# MAGIC |------------|-----------------|--------------------------------------------|------------|---------|
# MAGIC | 2024-10-02 | Developer_name | Initial creation of the notebook            | <a href="https://adb-7024489078919155.15.azuredatabricks.net/?o=7024489078919155#notebook/1830848775887419/command/1830848775887420">Create best practices notebook</a> | v1.0    |
# MAGIC | 2024-10-03 | Developer_name | Added transformation logic and target write |            | v1.1    |
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 Ingesting Source Data
# MAGIC The source data is retrieved from the `source_table`. Initial filters are applied to select relevant records for further transformation.
# MAGIC
# MAGIC #### Source Details:
# MAGIC - **Source Layer:** Bronze (Raw Data)
# MAGIC - **Source Table:** `source_table`
# MAGIC - **Source Type:** Delta Table
# MAGIC
# MAGIC The process checks if the source table exists in the catalog before data ingestion and handles errors if the table is missing.
# MAGIC

# COMMAND ----------

# DBTITLE 1,importing utilities
# MAGIC %run ../UTILITIES/common_utilities

# COMMAND ----------

# DBTITLE 1,Importing Mail Utility
# MAGIC %run ../UTILITIES/Mail_request_Utility

# COMMAND ----------

# MAGIC %md
# MAGIC # Input Parameter Descriptions
# MAGIC
# MAGIC - **environment**: Specifies the environment (e.g., dev, prod).
# MAGIC - **container**: Name of the storage container (default: "hft").
# MAGIC - **storage_account**: Azure storage account name (default: "unitycatalog12").
# MAGIC - **catalog**: The catalog where the tables are stored.
# MAGIC - **schema**: The schema within the catalog.
# MAGIC - **source_table**: Name of the source table.
# MAGIC - **target_table**: Name of the target table.
# MAGIC - **target_format**: Format of the target table (e.g., delta, parquet).
# MAGIC - **client_id_key**: ID for fetching secret values.
# MAGIC - **client_secret_key**: Value corresponding to the secret ID.

# COMMAND ----------

# DBTITLE 1,fetching widgets parameters
# Fetch input parameters
# Input 1: environment 
dbutils.widgets.text(name="environment", defaultValue="dev", label="environment")
environment = dbutils.widgets.get("environment")

# Input 2: container 
dbutils.widgets.text(name="container", defaultValue="code-best-practices", label="container")
container = dbutils.widgets.get("container")

# Input 3: storage_account 
dbutils.widgets.text(name="storage_account", defaultValue="celeballearning", label="storage_account")
storage_account = dbutils.widgets.get("storage_account")

# Input 4: catalog 
dbutils.widgets.text(name="catalog", defaultValue="databricks_champ", label="catalog")
catalog = dbutils.widgets.get("catalog")

# Input 5: schema 
dbutils.widgets.text(name="schema", defaultValue="coding_practices", label="schema")
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

# Input 9: client_id_key
dbutils.widgets.text(name="client_id_key", defaultValue="CELEBAL-ADB-CLIENT-ID", label="client_id_key")
client_id_key = dbutils.widgets.get("client_id_key")

# Input 10: client_secret_key
dbutils.widgets.text(name="client_secret_key", defaultValue="CELEBAL-ADB-SECRET-VALUE", label="client_secret_key")
client_secret_key = dbutils.widgets.get("client_secret_key")

# Input 11: logic_app_url
dbutils.widgets.text(name="logic_app_url_key", defaultValue="LOGIC-APP-URL", label="logic_app_url_key")
logic_app_url_key = dbutils.widgets.get("logic_app_url_key")

# COMMAND ----------

# DBTITLE 1,checking and creating source_table
try:
    #create source_table and target_table using the input parameter
    source_table=f"{catalog}.{schema}.{source_table_name}"
    target_table=f"{catalog}.{schema}.{target_table_name}"
    logger.log_info(f"checking source table: {source_table} exits or not")
    #check if source_table exists
    if check_table_exists(source_table):
        logger.log_info(f"{source_table} Table exists.")
        # create Dataframe
        source_df=ingest_data_from_table(source_table)
        logger.log_info(f"source_df created.")
    else:
        #log the error if source_table does not exist
        logger.log_error(f"{source_table} Table does not exist.")
except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    send_fail_notification(error_message=str(e),logic_app_url_key=logic_app_url_key,databricks_scope='CELEBAL-SECRETS')
    error.handle_error() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔄 Data Transformation
# MAGIC
# MAGIC This section applies several key transformations to the data:
# MAGIC
# MAGIC 1. **Initial Transformations:**
# MAGIC    - Convert timestamp to date.
# MAGIC    - Perform column transformations: cleaning, type casting, and calculations like `return`, `earning_per_share`, and `earning_ratio`.
# MAGIC
# MAGIC 2. **Aggregation and Join:**
# MAGIC    - Group by `Company` and `Exchange`, and aggregate `Price`.
# MAGIC    - Join the aggregated data back with the transformed DataFrame.
# MAGIC
# MAGIC 3. **Windowing and Masking:**
# MAGIC    - Apply window function for ranking by `company`.
# MAGIC    - Mask sensitive columns such as `earning_ratio`.
# MAGIC    - Classify `Price` into categories: "Low", "Medium", or "High".
# MAGIC
# MAGIC 4. **Final Aggregations:**
# MAGIC    - Calculate `Total_Price` and `Average_Price` for each company using window functions.
# MAGIC
# MAGIC 5. **Final Adjustments:**
# MAGIC    - Change all column names to uppercase.
# MAGIC
# MAGIC Data is now ready for writing to the target table.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Applying 1st layer of transformation
try:
    logger.log_info("Starting transformations on source_df.")
    source_df_with_date = convert_timestamp_to_date(source_df, "time", "yyyy-MM-dd")
    #define transformation that needs to be applied on source_df_with_date
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
    #apply above mentioned transformation on source_df_with_date
    transformed_df = apply_transformations(source_df_with_date, transformations)
    transformed_df_with_selected_fields = transformed_df.select('Day', 'Date', 'Month', 'Time', 'Stock', 'Exchange', 'Price', 'percentage_change', 'return', 'earning_per_share', 'earning_ratio')
    #first transformation applied successfully
    logger.log_info("Transformations applied successfully.")
except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    send_fail_notification(error_message=str(e),logic_app_url_key=logic_app_url_key,databricks_scope='CELEBAL-SECRETS')
    error.handle_error() 

# COMMAND ----------

# DBTITLE 1,2nd layer of transformations
try:
    logger.log_info("Starting transformations on source_df_with_date.")

    # Applying the aggregation function on your source dataframe
    groupby_cols = ['Company', 'Exchange']
    agg_col = 'Price'
    alias = 'Total_Price'
    # transformed_df
    # Transform the 'Price' column to float for correct aggregation
    transformations_on_source_df=[('Price', col('Price').cast('float'))]
    # transformed_df.printSchema()
    transformed_df=apply_transformations(source_df_with_date, transformations_on_source_df)
    # Call the aggregation function
    aggregated_df = aggregate_data(transformed_df, groupby_cols, agg_col, alias)

    #joining performed on transformed_df and aggregated_df using left join
    join_condition = [ 'Exchange']
    joined_df = transformed_df_with_selected_fields.join(aggregated_df, on=join_condition, how='left')

    logger.log_info("Aggregation and join applied successfully.")
except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    send_fail_notification(error_message=str(e),logic_app_url_key=logic_app_url_key,databricks_scope='CELEBAL-SECRETS')
    error.handle_error() 

# COMMAND ----------

# DBTITLE 1,3rd layer of transformation
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
except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    send_fail_notification(error_message=str(e),logic_app_url_key=logic_app_url_key,databricks_scope='CELEBAL-SECRETS')
    error.handle_error() 

# COMMAND ----------

# DBTITLE 1,4th layer of transformation
try:
    logger.log_info("Starting transformations on transformed_DataFrame.")
    # Partitioning the data by 'Company'
    window_spec = Window.partitionBy("Company")

    # 'Total_Price' and 'Average_Price' columns
    transformations = [
                        ('Total_Price', F_sum("Price").over(window_spec)),
                        ('Average_Price', F_avg("Price").over(window_spec))
                ]

    final_DataFrame = apply_transformations(transformed_DataFrame, transformations)

    logger.log_info("Transformation applied and final_DataFrame created successfully.")
except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    send_fail_notification(error_message=str(e),logic_app_url_key=logic_app_url_key,databricks_scope='CELEBAL-SECRETS')
    error.handle_error()

# COMMAND ----------

# DBTITLE 1,Apply name case on final DataFrame
try:
    logger.log_info("Applying column name case to final_DataFrame.")
    # change all column case to upper
    final_DataFrame = change_name_case(final_DataFrame, case='upper')

    logger.log_info("Column name case applied successfully.")
except Exception as e:
    logger.log_error(f"Error while applying transformations: {str(e)}")
    send_fail_notification(error_message=str(e),logic_app_url_key=logic_app_url_key,databricks_scope='CELEBAL-SECRETS')
    error.handle_error() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Writing Transformed Data to Target
# MAGIC
# MAGIC Once the transformations are applied, the data is written to the specified target format.
# MAGIC
# MAGIC #### Writing Process:
# MAGIC - **Delta Format:** Data is written to the Delta table at `target_table` using overwrite mode.
# MAGIC - **Other Formats:** Data is written to Azure storage with a dynamic path based on the current date.
# MAGIC
# MAGIC #### Target Details:
# MAGIC - **Target Layer:** Silver
# MAGIC - **Formats Supported:** Delta, CSV, or other specified formats
# MAGIC - **Dynamic Path (for non-Delta formats):** Path is generated based on the current date.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Write the Dataframe in user provided format and location
try:
    logger.log_info(f"Writing data  in {target_format} format")
    option_dictionary= {"header": "true", "delimiter": ","}
    if target_format == 'delta':
        #select the target path provided by user
        path=target_table
        #write data in delta format to the target path
        write_data(final_DataFrame, path,'overwrite',target_format,option_dictionary )
        #ETL notebook is completed
        # logger.log_info(f"Data written in {target_format} format at {path}")
    else:
        #create current date varibale to store current date
        current_date = datetime.now().strftime("%Y-%m-%d")
        #initiate the service principal to access storage account
        initiate_spn('CELEBAL-SECRETS', storage_account,client_id_key, client_secret_key)
        #create a genralised path to store the data
        path=f'abfss://{container}@{storage_account}.dfs.core.windows.net/{current_date}'
        #write data on storage account
        write_data(final_DataFrame, path,'overwrite',target_format,option_dictionary)
        #ETL notebook is completed
    logger.log_info("ETL pipeline completed successfully")
    error.handle_success()
except Exception as e:
    logger.log_error(f"Error while writing data in {target_format} format at {path} : {str(e)}")
    send_fail_notification(error_message=str(e),logic_app_url_key=logic_app_url_key,databricks_scope='CELEBAL-SECRETS')
    error.handle_error() 


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Conclusion
# MAGIC This notebook demonstrates best practices for organizing Databricks notebooks. By following the structure outlined here, data teams can improve code readability, maintainability, and tracking of updates.
# MAGIC
