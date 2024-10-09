# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠 Utility Notebook: `common_utilities`
# MAGIC This notebook contains common functions, imports, and reusable data definitions that can be leveraged across multiple Databricks notebooks. It aims to promote modularization, code reuse, and prevent code duplication.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📅 Change Log
# MAGIC
# MAGIC | Date       | Author            | Description                                 | Version |
# MAGIC |------------|-------------------|---------------------------------------------|---------|
# MAGIC | 2024-10-03 | Saurav Chaudhary   | Initial utility notebook with common functions | v1.0    |
# MAGIC | 2024-10-04 | Saurav Chaudhary   | Added retry and fault-tolerant functions      | v1.1    |
# MAGIC

# COMMAND ----------

# Standard Python imports used across multiple notebooks
import pandas as pd
import numpy as np
import datetime
from pyspark.sql import DataFrame,SparkSession
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, dayofmonth, date_format, regexp_replace, when,row_number,round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Utility function for logging
import logging
# logging.basicConfig(level=logging.WARNING)


# COMMAND ----------

from UTILITIES.error_logger import DatabricksLogger
from UTILITIES.exception_handler import DatabricksErrorHandler

# COMMAND ----------

def initiate_spn(scope_name: str,  storage_account_name_list: str) -> None:
    """
    Initiates the service principal and performs access setup for Azure storage containers.
 
    Args:
        scope_name (str): The name of the Databricks secret scope.
        storage_account_name_list (list): List of storage account names.
 
    Returns:
        None
 
    """
    # Set the Databricks secret scope name
    databricks_scopename = scope_name
    storage_account_name=storage_account_name_list
    print(storage_account_name)
    # Iterate over each storage account in the list
    # for storage_account_name in storage_account_name_list.split(","):
    #     # ApplicationID and SecretValue can be directly assigned or retrieved from the Databricks secrets
    #     ApplicationID = dbutils.secrets.get(scope = databricks_scopename, key = f"CELEBAL-ADB-CLIENT-ID")
    #     SecretValue = dbutils.secrets.get(scope = databricks_scopename, key = f"CELEBAL-ADB-SECRET-VALUE")
    #     DirectoryID = "e4e34038-ea1f-4882-b6e8-ccd776459ca0"
    #     Directory = "https://login.microsoftonline.com/{0}/oauth2/token".format(DirectoryID)
 
    #     # Configure Spark to use OAuth for authentication with the storage account
    #     spark.conf.set("fs.azure.account.auth.type."+storage_account_name+".dfs.core.windows.net", "OAuth")
    #     spark.conf.set("fs.azure.account.oauth.provider.type."+storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    #     spark.conf.set("fs.azure.account.oauth2.client.id."+storage_account_name+".dfs.core.windows.net", ApplicationID)
    #     spark.conf.set("fs.azure.account.oauth2.client.secret."+storage_account_name+".dfs.core.windows.net", SecretValue)
    #     spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storage_account_name+".dfs.core.windows.net", Directory)
 
    #     # Print success message for the initiated service principal
    #     print("SPN initiated successfully for {0}, {1}...!!!".format(storage_account_name))


    # for storage_account_name in storage_account_name_list.split(","):
    # ApplicationID and SecretValue can be directly assigned or retrieved from the Databricks secrets
    
    ApplicationID = dbutils.secrets.get(scope = databricks_scopename, key = f"CELEBAL-ADB-CLIENT-ID")
    SecretValue = dbutils.secrets.get(scope = databricks_scopename, key = f"CELEBAL-ADB-SECRET-VALUE")
    DirectoryID = "e4e34038-ea1f-4882-b6e8-ccd776459ca0"
    Directory = "https://login.microsoftonline.com/{0}/oauth2/token".format(DirectoryID)

    # Configure Spark to use OAuth for authentication with the storage account
    spark.conf.set("fs.azure.account.auth.type."+storage_account_name+".dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type."+storage_account_name+".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id."+storage_account_name+".dfs.core.windows.net", ApplicationID)
    spark.conf.set("fs.azure.account.oauth2.client.secret."+storage_account_name+".dfs.core.windows.net", SecretValue)
    spark.conf.set("fs.azure.account.oauth2.client.endpoint."+storage_account_name+".dfs.core.windows.net", Directory)

    # Print success message for the initiated service principal
    print("SPN initiated successfully for {0} ...!!!".format(storage_account_name))

# COMMAND ----------

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
        return table_exists
    
    except Exception as e:
        print(f"Error occurred while checking the table existence: {str(e)}")
        raise

# COMMAND ----------

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
    return df.withColumn("Date", date_format(col(timestamp_col), date_format_str))

# Example usage:
# df = convert_timestamp_to_date(df, "timestamp_column", "yyyy-MM-dd")


# COMMAND ----------

def change_name_case(df, case='upper'):
    """
    Change the case of column names in a DataFrame

    Parameters:
        - df (pyspark.sql.DataFrame): The input dataFrame.
        - case (str, optional): The case to convert column name to. Defaults to 'upper'

    Returns:
        - pyspark.sql.DataFrame: The DataFrame with column names converted to the specified case.
    """

    if case.upper() == 'UPPER':
        df = df.toDF(*[c.upper() for c in df.columns])
    elif case.upper() == 'LOWER':
        df = df.toDF(*[c.lower() for c in df.columns])
    return df

# COMMAND ----------

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
    for column_name, transformation in transformations:
        df = df.withColumn(column_name, transformation)
    return df


# COMMAND ----------

# Function to ingest data from a source
def ingest_data(source_table: str, filter_condition: str = None):
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
            logging.info(f"Ingesting data from table: {source_table} with filter: {filter_condition}")
            df = spark.sql(f"SELECT * FROM {source_table} WHERE {filter_condition}")
        else:
            logging.info(f"Ingesting data from table: {source_table} without any filter")
            df = spark.sql(f"SELECT * FROM {source_table}")
        
        return df
    except Exception as e:
        logging.error(f"Error in ingesting data from {source_table}: {str(e)}")
        raise

# COMMAND ----------

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
        logging.info(f"Transforming data by grouping on {group_column} and aggregating {agg_column}")
        transformed_df = df.groupBy(group_column).agg(sum(agg_column).alias('total'))
        return transformed_df
    except Exception as e:
        logging.error(f"Error in transforming data: {str(e)}")
        raise

# COMMAND ----------

# # Function to write data to a target
# def write_data(df, target_table: str, writing_mode:str):
#     """
#     Write DataFrame to the target table.
    
#     Args:
#     - df (DataFrame): Transformed DataFrame.
#     - target_table (str): The target table name.
    
#     Returns:
#     - None
#     """
#     try:
#         logging.info(f"Writing data to target table: {target_table}")
#         df.write.format("delta").mode(f"{writing_mode}").saveAsTable(target_table)
#     except Exception as e:
#         logging.error(f"Error in writing data to {target_table}: {str(e)}")
#         raise

# COMMAND ----------

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
        logging.info(f"Writing data to {path} using format {writing_format} with mode {writing_mode}")

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

        logging.info(f"Data written successfully to {path} in {writing_format} format.")
        
    except Exception as e:
        logging.error(f"Error in writing data to {path}: {str(e)}")
        raise


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


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
    # Ceil the specified aggregation column
    df = df.withColumn(agg_col, F.ceil(F.col(agg_col)))

    # Perform aggregation
    aggregated_df = df.groupBy(groupby_cols).agg(F.sum(agg_col).alias(alias))

    return aggregated_df

# COMMAND ----------

# Function for error logging in try-except blocks
def log_error(message):
    """
    Log an error message.
    
    Args:
    - message (str): Error message to log.
    
    Returns:
    - None
    """
    logging.error(f"ERROR: {message}")

# Function for info logging
def log_info(message):
    """
    Log an information message.
    
    Args:
    - message (str): Info message to log.
    
    Returns:
    - None
    """
    logging.info(f"INFO: {message}")


# COMMAND ----------

# Example: A dictionary of configurations used in the ETL process
configurations = {
    'source_table': 'bronze_source_table',
    'target_table': 'silver_target_table',
    'key_column': 'customer_id',
    'aggregation_column': 'sales_amount',
    'default_partition_column': 'event_date',
    'filter_condition': "status = 'active'"
}

# Example: A mapping dictionary for transformations
column_mapping = {
    'src_col_1': 'target_col_1',
    'src_col_2': 'target_col_2',
    'src_col_3': 'target_col_3'
}

# Example: List of sensitive columns that should not be logged or displayed
sensitive_columns = ['password', 'social_security_number', 'credit_card_number']


# COMMAND ----------

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
    for col in sensitive_columns:
        df = df.withColumn(col, F.lit("********"))
    return df

# Example: Retrieve secrets from Databricks Secrets
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
        return secret
    except Exception as e:
        logging.error(f"Error retrieving secret {key}: {str(e)}")
        raise


# COMMAND ----------

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
            logging.error(f"Attempt {attempt} failed: {str(e)}")
            if attempt == retries:
                logging.error("All retry attempts failed.")
                raise


# COMMAND ----------


