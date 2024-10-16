# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠 Utility Notebook: `common_utilities`
# MAGIC This notebook contains common functions, imports, and reusable data definitions that can be leveraged across multiple Databricks notebooks. It aims to promote modularization, code reuse, and prevent code duplication.
# MAGIC
# MAGIC # Logger and Error Handler
# MAGIC
# MAGIC - **Logger Object**: Initializes a logging object for capturing events.
# MAGIC - **Error Handler Object**: Initializes error handling and response dictionary.
# MAGIC
# MAGIC ### 📅 Change Log
# MAGIC
# MAGIC | Date       | Author            | Description                                 | Version |
# MAGIC |------------|-------------------|---------------------------------------------|---------|
# MAGIC | 2024-10-03 | Developer_name   | Initial utility notebook with common functions | v1.0    |
# MAGIC | 2024-10-04 | Developer_name   | Added retry and fault-tolerant functions      | v1.1    |

# COMMAND ----------

# DBTITLE 1,Importing Library
# Standard Python imports used across multiple notebooks
import os
import sys
from datetime import datetime, date
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, dayofmonth, date_format, regexp_replace, when,row_number,round,ceil,sum as F_sum ,lit, avg as F_avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# DBTITLE 1,Setting notebook relative path
# Append the required path to sys.path
var_append_working_directory_to_syspath = os.path.dirname(os.path.realpath(os.getcwd()))
sys.path.append(os.path.abspath(var_append_working_directory_to_syspath))

# COMMAND ----------

# DBTITLE 1,Import logger & Exception handler class
from UTILITIES.error_logger import DatabricksLogger
from UTILITIES.exception_handler import DatabricksErrorHandler

# COMMAND ----------

# DBTITLE 1,Importing HTML utility
# MAGIC %run ./HTML_Display_Utility

# COMMAND ----------

# DBTITLE 1,Define logger and exception handler object
#create Logger object
logger = DatabricksLogger()

#create error handler object
response_dict={}
error = DatabricksErrorHandler(response_dict)

# COMMAND ----------

# DBTITLE 1,initiate_spn
def initiate_spn(scope_name: str, storage_account_name_list: str, application_id_secret_key: str, application_id_secret_value: str) -> None:
    """
    Initiates the service principal and performs access setup for Azure storage containers.

    Args:
        scope_name (str): The name of the Databricks secret scope.
        storage_account_name_list (list): List of storage account names.
        application_id_secret_key (str): The key to retrieve the application ID from Databricks secrets.
        application_id_secret_value (str): The key to retrieve the application secret from Databricks secrets.

    Returns:
        None
    """
    try:
        # Set the Databricks secret scope name
        databricks_scopename = scope_name
        storage_account_name = storage_account_name_list
        application_id_secret_key = application_id_secret_key
        application_id_secret_value = application_id_secret_value
        logger.log_info(f"Initializing SPN for storage account: {storage_account_name}")

        # Fetch secrets from Databricks secret scope
        application_id = get_secret(databricks_scopename, application_id_secret_key)
        secret_value = get_secret(databricks_scopename, application_id_secret_value)
        # Define directory for authentication
        directory_id = "e4e34038-ea1f-4882-b6e8-ccd776459ca0"
        directory = f"https://login.microsoftonline.com/{directory_id}/oauth2/token"

        # Configure Spark to use OAuth for authentication with the storage account
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", application_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", secret_value)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", directory)

        # Print success message for the initiated service principal
        logger.log_info(f"SPN initiated successfully for {storage_account_name} ...!!!")
    
    except Exception as e:
        # Log the exception with an error message
        logger.log_error(f"Error initiating SPN for {storage_account_name}: {str(e)}")
        error.handle_error() 


# COMMAND ----------

# DBTITLE 1,get_secret
def get_secret(scope, key):
    """
    Retrieve a secret from Databricks Secrets.
    
    Args:
    - scope (str): Databricks Secret scope.
    - key (str): Secret key to retrieve.
    
    Returns:
    - str: Secret value.
    """
    try:
        secret = dbutils.secrets.get(scope=scope, key=key)
        logger.log_info(f"Retrieved secret for {key}")
        return secret
    except Exception as e:
        logger.log_error(f"Error retrieving secret {key}: {str(e)}")
        error.handle_error()

# COMMAND ----------

# DBTITLE 1,check_table_exists
def check_table_exists(three_level_namespace: str) -> bool:
    """
    Function to check if a table exists in Databricks using the three-level namespace.
    
    Args:
        three_level_namespace (str): The full namespace of the table (e.g., 'database.schema.table').
    
    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        # Check if the table exists using the full namespace
        table_exists = spark.catalog.tableExists(three_level_namespace)
        logger.log_info(f"Table exists: {table_exists}")
        return table_exists
    
    except Exception as e:
        logger.log_error(f"Error occurred while checking the table existence: {str(e)}")
        error.handle_error() 

# COMMAND ----------

# DBTITLE 1,convert_timestamp_to_date
def convert_timestamp_to_date(df, timestamp_col, date_format_str):
    """
    Convert a timestamp column to a string representation of a date using the specified format.

    Parameters:
    df (DataFrame): The Spark DataFrame containing the timestamp column.
    timestamp_col (str): The name of the timestamp column to be converted.
    date_format_str (str): The date format string (e.g., 'yyyy-MM-dd').

    Returns:
    DataFrame: A new DataFrame with an additional column containing the formatted date.
    """
    try:
        logger.log_info(f"Converting timestamp column {timestamp_col} to date format {date_format_str}")
        return df
    except Exception as e:
        logger.log_error(f"Error in converting timestamp to date: {str(e)} ")
        error.handle_error()

# COMMAND ----------

#his new code

# COMMAND ----------

# DBTITLE 1,change_name_case
def change_name_case(df, case='upper'):
    """
    Change the case of column names in a DataFrame

    Parameters:
        - df (pyspark.sql.DataFrame): The input dataFrame.
        - case (str, optional): The case to convert column name to. Defaults to 'upper'

    Returns:
        - pyspark.sql.DataFrame: The DataFrame with column names converted to the specified case.
    """
    try:
        logger.log_info(f"Changing column names case to {case}")
        if case.upper() == 'UPPER':
            df = df.toDF(*[c.upper() for c in df.columns])
        elif case.upper() == 'LOWER':
            df = df.toDF(*[c.lower() for c in df.columns])
        return df
    except Exception as e:
        logger.log_error(f"Error in changing column names case: {str(e)}")
        error.handle_error() 

# COMMAND ----------

# DBTITLE 1,apply_fransformation
def apply_transformations(df: DataFrame, transformations: list) -> DataFrame:
    """
    Apply a list of transformations to a DataFrame using withColumn.

    Args:
        df (DataFrame): The source DataFrame to transform.
        transformations (list): A list of tuples where each tuple contains:
                                - Column name (str): The name of the new or transformed column.
                                - Transformation (function): The transformation logic to apply.

    Returns:
        DataFrame: Transformed DataFrame after applying all transformations.
    """
    try:
        for column_name, transformation in transformations:
            df = df.withColumn(column_name, transformation)
        logger.log_info(f"Applied transformation: {column_name} -> {transformation}")
        return df
    except Exception as e:
        logger.log_error(f"Error in applying transformations: {str(e)}")
        error.handle_error() 


# COMMAND ----------

# DBTITLE 1,ingest_data_from_table
# Function to ingest data from a source
def ingest_data_from_table(source_table: str, filter_condition: str = None):
    """
    Ingest data from a source table with an optional filter condition.
    
    Args:
    - source_table (str): The name of the source table.
    - filter_condition (str, optional): SQL WHERE condition to filter data. If None, no filter is applied.
    
    Returns:
    - DataFrame: Ingested and filtered (or unfiltered) DataFrame.
    """
    try:
        if filter_condition:
            logger.log_info(f"Ingesting data from table: {source_table} with filter: {filter_condition}")
            df = spark.sql(f"SELECT * FROM {source_table} WHERE {filter_condition}")
        else:
            logger.log_info(f"Ingesting data from table: {source_table} without any filter")
            df = spark.sql(f"SELECT * FROM {source_table}")
        return df
    except Exception as e:
        logger.log_error(f"Error in ingesting data from {source_table}: {str(e)}")
        error.handle_error() 

# COMMAND ----------

# DBTITLE 1,transformed_data
# Function to transform data
def transform_data(df, group_column, agg_column):
    """
    Apply common transformations to the DataFrame (group by and aggregation).
    
    Args:
    - df (DataFrame): Input DataFrame.
    - group_column (str): Column name to group by.
    - agg_column (str): Column name to aggregate.
    
    Returns:
    - DataFrame: Transformed DataFrame.
    """
    try:
        logger.log_info(f"Transforming data by grouping on {group_column} and aggregating {agg_column}")
        transformed_df = df.groupBy(group_column).agg(sum(agg_column).alias('total'))
        return transformed_df
    except Exception as e:
        logger.log_error(f"Error in transforming data: {str(e)}")
        error.handle_error() 

# COMMAND ----------

# DBTITLE 1,write_data
# Function to write data to a target
def write_data(df, path: str, writing_mode: str, writing_format: str, options_dict: dict):
    
    """
    Write DataFrame to the target path or table.

    Args:
    - df (DataFrame): Transformed DataFrame.
    - path (str): Path to write the data (can be a three-level namespace for Delta tables or path for files like CSV/JSON).
    - writing_mode (str): The write mode (e.g., overwrite, append).
    - writing_format (str): Format to write the data (e.g., delta, csv, json).
    - options_dict (dict): Dictionary of additional options to pass to the write function.

    Returns:
    - None
    """
    try:
        logger.log_info(f"Writing data to {path} using format {writing_format} with mode {writing_mode}")

        # Apply options if provided
        writer = df.write.format(writing_format).mode(writing_mode)

        # Adding options from the dictionary to the writer
        for option_key, option_value in options_dict.items():
            writer = writer.option(option_key, option_value)

        # Check if the format is 'delta' to determine if it's a table write or a file write
        if writing_format == "delta":
            writer.saveAsTable(path)  # Assuming 'path' is the three-level namespace for delta tables
        else:
            writer.save(path)  # For file-based formats like CSV, JSON, etc.

        logger.log_info(f"Data written successfully to {path} in {writing_format} format.")     
    except Exception as e:
        logger.log_error(f"Error in writing data to {path}: {str(e)}")
        error.handle_error()

# COMMAND ----------

# DBTITLE 1,aggregate_data
def aggregate_data(df: DataFrame, groupby_cols: list, agg_col: str, alias: str) -> DataFrame:
    """
    Perform an aggregation on the specified DataFrame.

    Args:
    - df (DataFrame): The input DataFrame.
    - groupby_cols (list): The columns to group by.
    - agg_col (str): The column to aggregate.
    - alias (str): The alias name for the aggregated column.

    Returns:
    - DataFrame: The aggregated DataFrame.
    """
    df = df.withColumn(agg_col, regexp_replace(col(agg_col), '₹', '').cast('float'))
    # Ceil the specified aggregation column
    df = df.withColumn(agg_col, ceil(col(agg_col)))

    # Perform aggregation
    aggregated_df = df.groupBy(groupby_cols).agg(F_sum(agg_col).alias(alias))

    return aggregated_df

# COMMAND ----------

# DBTITLE 1,mask_sensitive_data
# Function to mask sensitive data before logging
def mask_sensitive_data(df, sensitive_columns):
    """
    Mask sensitive columns in the DataFrame.
    
    Args:
    - df (DataFrame): Input DataFrame with sensitive columns.
    - sensitive_columns (list): List of column names to mask.
    
    Returns:
    - DataFrame: DataFrame with sensitive data masked.
    """
    try:
        for col in sensitive_columns:
            df = df.withColumn(col, lit("********"))
        logger.log_info(f"Masked sensitive data in DataFrame.")
        return df
    except Exception as e:
        logger.log_error(f"Error masking sensitive data: {str(e)}")
        error.handle_error()


# COMMAND ----------

# DBTITLE 1,retry_on_failure
# Example: Function to handle errors gracefully with retries
def retry_on_failure(func, retries=3):
    """
    Retry a function in case of failure.
    
    Args:
    - func (function): The function to retry.
    - retries (int): Number of retries.
    
    Returns:
    - Any: Return value of the function if successful.
    """
    attempt = 0
    while attempt < retries:
        try:
            return func()
        except Exception as e:
            attempt += 1
            logger.log_error(f"Attempt {attempt} failed: {str(e)}")
            if attempt == retries:
                logger.log_error("All retry attempts failed.")
                error.handle_error()


# COMMAND ----------

# DBTITLE 1,functions_and_variables_html
html = html_intro()
html += html_header()

html += html_row_fun("Variable: logger", "log specific event with a defined log level.")
html += html_row_fun("Variable: error", "Handle and raise error with a defined log level.")
html += html_row_fun("initiate_spn()", "Initiates the service principal and performs access setup for Azure storage containers.")
html += html_row_fun("get_secret()", "Fetches a secret value from Databricks secret scope.")
html += html_row_fun("check_table_exists()", "Checks if a table exists in the specified namespace.")
html += html_row_fun("convert_timestamp_to_date()", "Converts a timestamp column to a date with the given format.")
html += html_row_fun("change_name_case()", "Changes the case of column names in a DataFrame.")
html += html_row_fun("apply_transformations()", "Applies a series of transformations to a DataFrame.")
html += html_row_fun("ingest_data_from_table()", "Ingests data from a source table with an optional filter condition.")
html += html_row_fun("transform_data()", "Transforms the DataFrame by applying aggregation on specified columns.")
html += html_row_fun("generate_html_table()", "Generates an HTML table to display function names, descriptions, and parameters.")
html += html_row_fun("write_data()", "Writes DataFrame to a specified path with mode and format.")
html += html_row_fun("aggregate_data()", "Aggregates data by grouping by specified columns and applying aggregation on a specific column.")
html += html_row_fun("mask_sensitive_data()", "Masks sensitive columns in a DataFrame.")
html += "</table></body></html>"

displayHTML(html)
