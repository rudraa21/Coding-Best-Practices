from UTILITIES.error_logger import DatabricksLogger
from UTILITIES.exception_handler import DatabricksErrorHandler

# Standard Python imports used across multiple notebooks
import pandas as pd
import numpy as np
import datetime
from pyspark.sql import DataFrame,SparkSession
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import col, to_timestamp, dayofmonth, date_format, regexp_replace, when,row_number,round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
#Utility function for logging
import logging
#logging.basicConfig(level=logging.WARNING)


def check_table_exists(spark,three_level_namespace: str) -> bool:
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

# Function to ingest data from a source
def ingest_data(spark,source_table: str, filter_condition: str = None):
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