# Databricks notebook source
# Python Script 
#  ##### - Utility Notebook
# 
#  This script contains a utility function that performs a specific task. The utility function is designed to be reusable across different teams and projects. It provides a convenient way to accomplish a common task without duplicating code.
# 
#  **Usage**:
#  1. Import the script or module containing the utility function into your Python script or notebook.
#  2. Call the utility function with the appropriate parameters to perform the desired task.
#  3. Handle the returned value or utilize the utility function's side effects as needed.


# COMMAND ----------

#pyspark libs
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType,IntegerType,StringType,LongType,DateType,StructField,StructType
from delta import DeltaTable
from pyspark.sql import SparkSession,DataFrame

#python libs
from datetime import datetime ,timedelta
import json
import re
import pip
import sys
import subprocess
from typing import Dict,List,Optional,Any
import logging

def is_table_exists(spark,db_name: str, table_name: str) -> bool:
    """
    Check the existence of a table in the specified database.

    Args:
        db_name (str): Name of the database.
        table_name (str): Name of the table.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    # Check if the table exists in the specified database
    table_exists = spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")
    return table_exists

def notebook_exit_response(dbutils,**kwargs) -> dict:
  '''
  DESCRIPTION:-
  This function is used to print the exception
  
  IN:-

  OUT:-
  responseFrom_pipeline
  '''
  # when exception_info key present in the kwargs indicates that exception occured, 
  # so we are raising that exception, instead of exiting the notebook
  if "exception_info" in kwargs:
    exception_info_value = kwargs["exception_info"]
    raise Exception(f"Exception occured: {exception_info_value}")

  dbutils.notebook.exit(json.dumps(kwargs))


def generate_filter_condition(filter_template:dict):
  
    assert len(filter_template) !=0 ,"Please provide the List of dict as input...!"
    cond=""
    for cols in filter_template:

        if  "conditonal_operator" not in cols:
          cols.update({"conditonal_operator":"&"})

        if 'colm_function' in cols:
          exp=cols.get('colm_function')
          if ',' in exp:
            filter_exps = []
            exps = exp.split(',')
            no_of_exps= len(exps)
            for exp in exps :
              filter_exps.append(f"F.{exp}")
            filter_cond='('.join(filter_exps)
            filter_col = f"( {filter_cond} (F.col('{cols.get('col_name')}'))"
            for i in range(no_of_exps-1):
              filter_col=filter_col+')'
          else:
            filter_col = f"( F.{exp} (F.col('{cols.get('col_name')}'))"
        else:
          filter_col=f"(F.col('{cols.get('col_name')}')"
        if cols["operator"] == "isin":
          cond= cond + ''.join([f"{filter_col}.isin({cols.get('col_val')})) {cols.get('conditonal_operator')} "])
        if cols["operator"] == "not like":
          cond= cond + ''.join([f"~ {filter_col}.like({cols.get('col_val')})) {cols.get('conditonal_operator')} "])
        if cols["operator"] == "like":
          cond= cond + ''.join([f"{filter_col}.like({cols.get('col_val')})) {cols.get('conditonal_operator')} "])
        if cols["operator"] in ['>',"<",">=","<=","==","!="]:
          cond= cond + ''.join([f"{filter_col} {cols.get('operator')} {cols.get('col_val')} ) {cols.get('conditonal_operator')} "])
        if cols["operator"] == "isNull":
          cond= cond + ''.join([f"{filter_col}.isNull() ) {cols.get('conditonal_operator')} "])
        if cols["operator"] == "isNotNull":
          cond= cond + ''.join([f"{filter_col}.isNotNull() ) {cols.get('conditonal_operator')} "])
    return cond[:-3]
  
def get_dataframe(spark,database_name:str ,table_name:str, filter_template: list,list_of_columns_to_select: list,transformed_or_new_cols_dict: list, distinct_flag:bool,list_of_keys_to_fetch_distinct_rows:list=None):
  '''
  Description :
  This function is used to retrive data from a table , with given filter conditions, columns and transformations and return a Dataframe

  In :
  database_name : schema name or databse name of table
  table_name : table name
  filter_template : filter conditions to be applied on the table , should be a string with all values . None, if not required
  list_of_columns_to_select : list of required columns to be selected . None, if all columns to be selected
  transformed_or_new_cols_dict : transformations to be applied to existing column, or expression to add to a new column . 
                          dictionary - {column name : sql expression} . None, if not required
  distinct_flag : flag to fetch distinct records, true or false
  list_of_keys_to_fetch_distinct_rows : columns on which distinct records are to be fetched

  Out :
  df : Dataframe with required data
  '''
  try:
  #Check if table exists
    if is_table_exists(spark,database_name,table_name):
      df = spark.table(database_name + "." +table_name)
    else:
      raise ValueError("Table does not exist in database")
      
    if transformed_or_new_cols_dict:
        cols=list(transformed_or_new_cols_dict.keys())
        for col in cols:
          col_expr=transformed_or_new_cols_dict.get(col)
          df=df.withColumn(col,F.expr(col_expr))


      
    if filter_template:
      filter_condition = generate_filter_condition(filter_template)
      print(filter_condition)
      df=df.filter(eval(filter_condition))
      
    #Check if all the columns in the select clause are available in the table
    if list_of_columns_to_select:
      if is_column_exist(df, list_of_columns_to_select): 
          df = df.select(*list_of_columns_to_select)
      else:
        raise ValueError("One or more columns in the list do not exist in the table")
        
    if distinct_flag  :
      if list_of_keys_to_fetch_distinct_rows :
        df= df.drop_duplicates(list_of_keys_to_fetch_distinct_rows)
      else:
        df = df.distinct() 

  except Exception as e:
    raise Exception(f"Failed to get dataframe with error {e}")
  return df

def read_json_metadata(file_path: str, storage_type: str,**kwargs) -> dict:
    '''
    DESCRIPTION: Read JSON file from specified storage type and create a dictionary object

    IN:
    param: file_path : Path to the JSON file
    param: storage_type : Storage type ('adls2', 'dbfs', 'local')
    '''
    if storage_type == 'adls2':
        json_content_df = spark.read.option("multiLine",True).json(file_path).toJSON().collect()[0]
        data = json.loads(json_content_df)
        print(f"successfully access the json from {storage_type} location")
    elif storage_type == 'dbfs':
        # Read from DBFS
        with open(f'/dbfs/{file_path}', 'r') as file:
            data = json.load(file)
        print(f"successfully access the json from {storage_type} location")
    elif storage_type == 'local':
        # Read from local file system
        with open(file_path, 'r') as file:
            data = json.load(file)
        print(f"successfully access the json from {storage_type} location")
    else:
        raise ValueError(f"Unsupported storage type: {storage_type}")

    return data


def is_column_exist(df:DataFrame, column_list:list):
    """
    Check if the specified columns exist in the DataFrame.
    This function checks whether all the columns in the provided list exist in the DataFrame.
    Parameters:
    - df (DataFrame): The DataFrame to check.
    - column_list (list): A list of column names to check for existence.
    """
    assert isinstance(df,DataFrame) and len(column_list) !=0, "Recieved invalid input. It seems either df is empty or not type of dataframe or columns list is empty"
    dataframe_columns=[colm.lower() for colm in df.columns]
    column_list=[colm.lower() for colm in column_list]
    return all(col_name in dataframe_columns for col_name in column_list)

def update_processing_time_and_exit(dbutils,etl_pipeline_response_dict, etl_start_time):
    """
    Update the ETL pipeline response dictionary with status, end time, and processing time,
    and then exit the notebook with the updated response.

    Parameters:
    - etl_pipeline_response_dict (dict): The dictionary containing the ETL pipeline response.
    - etl_start_time (datetime): The start time of the ETL process.
    """
    # Update status to "succeeded"
    etl_pipeline_response_dict.update({'status': "succeeded" })

    # Update end time
    etl_end_time = datetime.now()
    etl_pipeline_response_dict.update({'etl_end_time': str(etl_end_time) })

    # Calculate and update processing time
    etl_processing_time = etl_end_time - etl_start_time
    etl_processing_time = etl_processing_time.total_seconds()
    etl_pipeline_response_dict.update({'etl_processing_time': etl_processing_time })

    # Exit notebook with the updated response
    notebook_exit_response(dbutils,notebook_response=etl_pipeline_response_dict)


def write_df(df, write_params,header=False):
    """
    DESCRIPTION: Write the DataFrame to a Delta table or an ADLS (Azure Data Lake Storage) location based on the specified parameters.
    
    Args:
        df (DataFrame): The DataFrame to write.
        write_params (dict): Key-value pair parameters for writing the DataFrame. 
            It should contain either 'delta_table_name' for writing to a Delta table or 'adls_location' for writing to an ADLS location.
            Other optional parameters include 'write_mode' for specifying the write mode and 'format' for specifying the file format.

    Returns:
        None
    """ 
    assert isinstance(df, DataFrame) and isinstance(write_params, dict), "Invalid types received. Inputs should be a Spark DataFrame and a dictionary."
    
    delta_table_name = write_params.get('delta_table_name')
    adls_location = write_params.get('adls_location')
    
    # Check if either Delta table name or ADLS location is provided
    if delta_table_name:
        # Write to Delta table
        if is_table_exists(write_params['db_name'], write_params['tbl_name']):
            if write_params['write_mode'].lower() in ('append', 'overwrite'):
                df.write.mode(write_params['write_mode']).format("delta").saveAsTable(delta_table_name)
            else:
                raise Exception('Received invalid inputs: Expected write_mode to be "append" or "overwrite"')
        else:
            raise Exception(delta_table_name)
    elif adls_location:
        # Write to ADLS location
        df.write.mode(write_params.get('write_mode', 'append')).format(write_params.get('format')).save(adls_location,header=header)
    else:
        raise Exception("Either Delta table name or ADLS location should be provided.")


def notebook_exit_response(dbutils,**kwargs) -> dict:
  '''
  DESCRIPTION:-
  This function is used to print the exception
  
  IN:-

  OUT:-
  responseFrom_pipeline
  '''
  # when exception_info key present in the kwargs indicates that exception occured, 
  # so we are raising that exception, instead of exiting the notebook
  if "exception_info" in kwargs:
    exception_info_value = kwargs["exception_info"]
    raise Exception(f"Exception occured: {exception_info_value}")

  dbutils.notebook.exit(json.dumps(kwargs))

def get_vmi_mapping_data_from_confing_json(spark: SparkSession, config_data: Dict,expected_schema) -> DataFrame:
    """
    Create a DataFrame from the provided data with the specified schema and compare it with the expected schema.

    Args:
        spark (SparkSession): The SparkSession object.
        config_data (Dict): The dictionary containing the data to create the DataFrame.

    Returns:
        DataFrame: The DataFrame created from the data if the schema matches the expected schema.

    Raises:
        Exception: If the schema of the DataFrame does not match the expected schema.
    """

    # Create DataFrame from the provided data
    edi_jda_mapping_df = spark.createDataFrame(config_data)
    actual_schema = edi_jda_mapping_df.schema

    # Compare the expected schema with the inferred schema
    if actual_schema == expected_schema:
        print("Schema matches the expected schema.")
        return edi_jda_mapping_df
    else:
        raise Exception("Schema does not match the expected schema")

def get_aggregate_data(df: DataFrame,groupby_cols: List[str],agg_col:List[str],alias: str) -> DataFrame:
  """

  ##move to utility .
    This function aggregate all the metric columns present bev metrics group by fixed audit column and location id and timeframe id.

    Args:
        df (DataFrame): The input DataFrame to be aggregated.
        groupby_cols(list) : The list of columns to be groupbed by
        agg_cols(list): The list of columns which needs to be summed
    Returns:
        DataFrame: The aggregated DataFrame.

    """
  agg_df=df.groupby(*groupby_cols).agg(*[F.sum(col).alias(alias) for col in agg_col])
  
  return agg_df
