# Databricks notebook source
# MAGIC %md
# MAGIC ##### - Utility Notebook
# MAGIC
# MAGIC This script contains a utility function that performs a specific task. The utility function is designed to be reusable across different teams and projects. It provides a convenient way to accomplish a common task without duplicating code.
# MAGIC
# MAGIC **Usage**:
# MAGIC 1. Import the script or module containing the utility function into your Python script or notebook.
# MAGIC 2. Call the utility function with the appropriate parameters to perform the desired task.
# MAGIC 3. Handle the returned value or utilize the utility function's side effects as needed.
# MAGIC
# MAGIC  ##### History
# MAGIC |Date|Author|Change Description|Event|
# MAGIC |---------|------------------|--------------------|--------------------|
# MAGIC |07-25-2023 |	Nainer Rohit |Added look_up_dataframe function | Initial Developement |
# MAGIC |11-29-2023 |	Chandra | Added get_dataframe,generate_filter_condition,get_table_max_load_time function | As part of Fact Table Developement added new functions |
# MAGIC |22-02-2024 | Rohith  |Changed write_to_delta_with_upsert and write_df_to_delta_table to store by table name| |
# MAGIC |28-02-2024 | Rohith  |Changed notebook_exit_response to throw error when exception occured | |
# MAGIC |04-03-2024 | Rohith  |Added update_processing_time_and_exit function | |
# MAGIC |07-05-2024 | Harish | Added get_dim_filter_params function to get sys specific params| |
# MAGIC |14-05-2024| Rohith | Added get_fact_filter_params function to get sys specific params| |
# MAGIC |22-05-2024| Rohith | Updated get_dim_filter_params and get_fact_filter_params functions to get LATAM specific params| |
# MAGIC |30-07-2024 | Rohith Kumar | Added `write_to_delta_dim_with_upsert` for dimension tables | |
# MAGIC |05-08-2024 | Rohith Kumar | Added `write_to_delta_fact_with_upsert` for fact tables | |
# MAGIC |26-08-2024 | Rohith | Updated get_dim_filter_params and get_fact_filter_params functions with a new system_name for Russia||
# MAGIC |27-08-2024 | Rohith | Added `get_table_system_params_dict` function||

# COMMAND ----------

#pyspark libs
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType,IntegerType,StringType
from delta import DeltaTable
from pyspark.sql import SparkSession,DataFrame

#python libs
from datetime import datetime
import json
import re
import pip
import sys
import subprocess
from typing import Dict,List,Optional, Tuple, Any
#import logging 
try:
  from pyspark.dbutils import DBUtils
  from notebooks_gold.utilities.custom_exception_utilities import TableNotFoundException

  # creating spark session and dbutils object for pytest 
  spark = SparkSession.builder.getOrCreate() 
  dbutils = DBUtils(spark)
except ImportError:
  print(" Import libraries required for manual execution through run command below ")


# COMMAND ----------

def is_table_exists(db_name: str, table_name: str) -> bool:
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

# COMMAND ----------

def get_delta_or_full_load_from_table(
    database_name: str,
    table_name: str,
    load_type: str,
    watermark_col: Optional[str] = None
    ) -> DataFrame:
    """
    DESCRIPTION: Read delta load or full load from the table.

    Args:
        database_name (str): Name of the database.
        table_name (str): Name of the table.
        load_type (str): Type of load, either "fullload" or "incremental".
        watermark_col (str, optional): Timestamp column for incremental load. Defaults to None.
        watermark_max_timestamp (str, optional): Latest max load timestamp for incremental load. Defaults to None.

    Returns:
        DataFrame: Loaded data.

    Raises:
        Exception: If the load_type is neither "fullload" nor "incremental".

    """

    assert  load_type.lower() in ["incremental","fullload"],"load_type should be either incremental or fullload"
    schema_name = database_name + "." + table_name

    if load_type.lower() == "incremental":
        df = spark.table(schema_name)
        watermark_max_timestamp = df.select(watermark_col).agg(F.max(watermark_col)).collect()[0][0]
        df = df.filter(F.col(watermark_col) == watermark_max_timestamp)
    elif load_type.lower() == "fullload":
        df = spark.table(schema_name)
        
    return df

# COMMAND ----------

def get_data_from_tables(schema_dict: dict):
    """
    Retrieves data from tables based on the specified schema dictionary.

    Parameters:
    - schema_dict (dict): A dictionary containing table details, such as database name, table name, load type, watermark column (for incremental load), and column names.
    Returns:
    - dataframe_dict (dict): A dictionary containing DataFrames with table data.
    Raises:
    - TableNotFoundException: If a specified table does not exist.

    Example Usage:
    ```python
    schema = {
        'table1': {
            'db_name': 'database1',
            'load_type': 'fullload',
            'req_col_names': ['col1', 'col2']
        },
        'table2': {
            'db_name': 'database2',
            'load_type': 'incremental',
            'watermark_col': 'timestamp',
            'req_col_names': ['col3', 'col4']
        }
    }
    result = get_data_from_tables(schema)
    ```
    """
    dataframe_dict = {}
    for tbl_name, tbl_details in schema_dict.items():
        # Check if the table exists
        if is_table_exists(tbl_details['db_name'], tbl_name):
            if tbl_details['load_type'] == 'fullload':
                # Retrieve data from the table for full load
                tbl_data = get_delta_or_full_load_from_table(tbl_details["db_name"], tbl_name,
                                                             tbl_details['load_type'])
            elif tbl_details['load_type'] == 'incremental':
                # Retrieve data from the table for incremental load
                tbl_data = get_delta_or_full_load_from_table(tbl_details["db_name"], tbl_name,
                                                             tbl_details['load_type'],
                                                             tbl_details['watermark_col'])
        else:
            raise TableNotFoundException(tbl_name)

        # Selecting required columns alone from the source DataFrame
        selected_col_df = tbl_data.select(*tbl_details['req_col_names'])
        # Add the table data to the dictionary
        dataframe_dict[tbl_name] = selected_col_df

    return dataframe_dict


# COMMAND ----------

def get_filtered_data(dfs_dict, filter_dict):
    """
    Filters each DataFrame in the given dictionary based on the specified conditions.

    Parameters:
    - dfs_dict (dict): A dictionary where keys are table names and values are DataFrames.
    - filter_dict (dict): A dictionary where keys are column names, and values are conditions for filtering.

    Returns:
    - dict: A modified dictionary with filtered DataFrames.
    """
    for tbl_name, df in dfs_dict.items():
        filtered_df = filter_data_by_conditions(df, filter_dict)
        # Add the table data to the dictionary
        dfs_dict[tbl_name] = filtered_df
    return dfs_dict


# COMMAND ----------

def get_dim_filter_params(system_name: str, env: str) -> dict:
    """
    Constructs dimension filter parameters based on the specified system name and environment.
 
    Args:
        system_name (str): The name of the system ('PIRT' or 'PGT').
        env (str): The environment identifier.
 
    Returns:
        dict: A dictionary containing dimension filter parameters.
    """
 
    # Construct dimension filter parameters based on the specified system name
    if system_name in ("PIRT", "PIRT_RU"):
        dim_filter_params_dict = {
            "XTNDFSystemId": 501008,
            "XTNDFReportingUnitId": 107240,
            "system_id": 8,
            "business_unit_code": "EUOTC_PIRT",
            "InstanceId": env[0].upper() + "16",
        }
    elif system_name == "PGT":
        dim_filter_params_dict = {
            "XTNDFSystemId": 1548,
            "XTNDFReportingUnitId": 107019,
            "system_id": 627,
            "business_unit_code": "EUOTC_PGT",
            "InstanceId": "GM" + env[0].upper(),
        }
    elif system_name == "LATAM":
        dim_filter_params_dict = {
            "XTNDFSystemId": 501002,
            "XTNDFReportingUnitId": 107240,
            "system_id": 2,
            "business_unit_code": "LATAMOTC_PIRT",
            "InstanceId": env[0].upper() + "E1",
        }
    return dim_filter_params_dict

# COMMAND ----------

def get_fact_filter_params(system_name: str, env: str) -> dict:
    """
    Constructs transaction(fact) data filter parameters based on the specified system name and environment.
    Args:
        system_name (str): The name of the system ('PIRT' or 'PGT').
        env (str): The environment identifier.
    Returns:
        dict: A dictionary containing fact filter parameters.
    """
    if system_name == 'PIRT':
        fact_filter_params = {
            "XTNDFSystemId": 501008,
            "XTNDFReportingUnitId": 106903,
            "SystemId": 8,
            "InstanceId": env[0].upper()+'16',
            "business_unit_code": "EUOTC_PIRT"
        }
    elif system_name == "PGT":
        fact_filter_params = {
            "XTNDFSystemId": 1702,
            "XTNDFReportingUnitId": 107019,
            "SystemId": 702,
            "InstanceId": 'R1'+env[0].upper(),
            "business_unit_code": "EUOTC_PGT"
        }
    elif system_name == 'LATAM':
        fact_filter_params = {
            "XTNDFSystemId": 501002,
            "XTNDFReportingUnitId": 106906,
            "SystemId": 2,
            "business_unit_code": "LATAMOTC_PIRT",
            "InstanceId": env[0].upper() + "E1",
        }
    elif system_name == 'PIRT_RU':
       fact_filter_params = {
            "XTNDFSystemId": 501008,
            "XTNDFReportingUnitId": 106903,
            "SystemId": 8,
            "InstanceId": env[0].upper()+'16',
            "business_unit_code": "EUOTC_PIRTRU"
        } 
    return fact_filter_params

# COMMAND ----------

def filter_data_by_conditions(df, conditions):
    """
    Filters a PySpark DataFrame based on the provided conditions.

    Parameters:
    - df: PySpark DataFrame
    - conditions: Dictionary with column names as keys and filter values as values

    Returns:
    - PySpark DataFrame after applying the filters
    """
    filter_expr = F.col(list(conditions.keys())[0]) == list(conditions.values())[0]
    
    for col_name, filter_value in list(conditions.items())[1:]:
        filter_expr = filter_expr & (F.col(col_name) == filter_value)

    filtered_df = df.filter(filter_expr)
    return filtered_df

# COMMAND ----------

def write_to_delta_with_upsert(df: DataFrame, db_name: str, tbl_name: str, merge_keys: list, surrogate_key:  Optional[str] = None):
    """
    Writes a DataFrame to a Delta table using upsert (merge) operation based on merge keys.
    
    Args:
        df (DataFrame): DataFrame to be written.
        db_name (str): Database name to store the data.
        tbl_name (str): Table name to store the data
        merge_keys (list): List of column names to be used as merge keys.
        surrogate_key (str) optional: surrogate key of the dataframe
    """
    # Get the list of existing columns from the DataFrame
    existing_columns = df.columns

    # Create a dictionary mapping existing columns to new columns
    update_cols_dict = {f"existing.{col_name}": f"new.{col_name}" for col_name in existing_columns}
    # From update cols dict removing xtncreatedtime and xtncreatedbyid audit cols
    update_cols_dict.pop("existing.XTNCreatedTime", None)
    update_cols_dict.pop("existing.XTNCreatedById", None)
    # From update cols dict removing surrogate key
    update_cols_dict.pop(f"existing.{surrogate_key}", None)

    # create a merge expressions based on merge key
    merge_exprs = " AND ".join(f"existing.{key} = new.{key}" for key in merge_keys)
    delta_table_name = db_name + '.' + tbl_name
    # Check if Delta table exists by name
    if is_table_exists(db_name, tbl_name):
        delta_table = DeltaTable.forName(spark, delta_table_name)
        # Upsert (merge) operation
        delta_table.alias("existing")\
            .merge(df.alias("new"), merge_exprs)\
            .whenMatchedUpdate(set=update_cols_dict)\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        raise TableNotFoundException(delta_table_name)


# COMMAND ----------

def write_df_to_delta_table(df, write_params):
    """
    DESCRIPTION:- Writing the data at specified table
    param df: dataframe
    param write_params: KV pair params for writing the dataframe
    
    Return : None
    """ 
    assert isinstance(df,DataFrame) and isinstance(write_params,dict),"Recieved invalid types. Inputs should be type of spark Dataframe 2. KV pair"
    
    delta_table_name = write_params['db_name'] + '.' + write_params['tbl_name']
    # Check if Delta table exists by name
    if is_table_exists(write_params['db_name'], write_params['tbl_name']):
        if write_params['write_mode'].lower() in ('append', 'overwrite'):
            df.write.mode(write_params['write_mode']).format("delta").saveAsTable(delta_table_name)
        else:
            raise Exception('Received invalid inputs: Expected write_mode to be "append" or "overwrite"')
    else:
        raise TableNotFoundException(delta_table_name)

# COMMAND ----------


def notebook_exit_response(**kwargs) -> dict:
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

# COMMAND ----------

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
  
def get_dataframe(database_name:str ,table_name:str, filter_condition: str,list_of_columns_to_select: list,transformed_or_new_cols_dict: list, distinct_flag:bool,list_of_keys_to_fetch_distinct_rows:list=None):
  '''
  Description :
  This function is used to retrive data from a table , with given filter conditions, columns and transformations and return a Dataframe

  In :
  database_name : schema name or databse name of table
  table_name : table name
  filter_condition : filter conditions to be applied on the table , should be a string with all values . None, if not required
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
    if is_table_exists(database_name,table_name):
      df = spark.table(database_name + "." +table_name)
    else:
      raise ValueError("Table does not exist in database")
    
    if transformed_or_new_cols_dict:
        cols=list(transformed_or_new_cols_dict.keys())
        for col in cols:
          col_expr=transformed_or_new_cols_dict.get(col)
          df=df.withColumn(col,F.expr(col_expr))

    if filter_condition:
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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC #### 🛠️ Spark 3.0 Configuration
# MAGIC ---
# MAGIC  
# MAGIC **Setting Spark Configuration**
# MAGIC  
# MAGIC * **Parameter**: `spark.sql.jsonGenerator.ignoreNullFields`
# MAGIC * **Default Value**: `True`
# MAGIC * **Parameter Description**:Whether to ignore null fields when generating JSON objects in JSON data source and JSON functions such as to_json. If false, it generates null for null fields in JSON objects.
# MAGIC  
# MAGIC **Requirement:**
# MAGIC When reading JSON data from ADLS2, keys with null values are currently unable to parse, resulting in their exclusion by the Spark JSON DataFrame reader. To address this issue, we need to set the `spark.sql.jsonGenerator.ignoreNullFields` parameter. Setting it to `True` will allow the JSON DataFrame reader to retain keys with null values.
# MAGIC  
# MAGIC **Apply:** `spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields",False)`
# MAGIC  
# MAGIC **Source:** https://spark.apache.org/docs/3.0.2/configuration.html

# COMMAND ----------

spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields",False)

# COMMAND ----------

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

# COMMAND ----------

def validate_date_format(date_string, expected_format="%Y-%m-%d"):
    try:
        # Try to parse the date string
        datetime.strptime(date_string, expected_format)
        return True
    except ValueError:
        # If parsing fails, return False
        return False

# COMMAND ----------

def check_view_exists(spark: SparkSession, view_name: str, global_temp_db: str) -> bool:
    """
    Check if the specified view exists.

    Parameters:
    - spark (SparkSession): The SparkSession instance.
    - view_name (str): The name of the view.
    - global_temp_db (str): The global temporary database name.

    Returns:
    - bool: True if the view exists, False otherwise.
    """
    return any(table.name == view_name for table in spark.catalog.listTables(global_temp_db))


# COMMAND ----------

# def print_log_pattern(text_strng):
#     logging.info("#" * 100)
#     logging.info("#" * 5  + text_strng + "#" * 5)
#     logging.info("#" * 100)

# COMMAND ----------

def perform_dq_checks(df: DataFrame, primary_key_columns: list, required_columns: list, system_id: dict, reporting_id: dict) -> bool:
    """
    Perform Data Quality checks on the DataFrame.

    Parameters:
        df (DataFrame): Input DataFrame to perform DQ checks on.
        primary_key_columns (list): List of primary key column names.
        required_columns (list): List of column names whose data availability is checked.
        system_id (str): Expected system ID for the records.
        reporting_id (str): Expected reporting  ID for the records.

    Returns:
        bool: True if all DQ checks pass, False otherwise.
    
    Raises:
        Exception: If any of the DQ checks fail.
    """
    # Check for duplicate records based on primary key
    duplicate_count = df.groupBy(primary_key_columns).count().filter("count > 1").count()
    if duplicate_count > 0:
        raise Exception(f"DQ Check Failed: Found {duplicate_count} duplicate records")
    #logging.info("Data Quality Check: Successfully processed duplicate record check.")


    # Check data availability for required columns
    if required_columns:
        for column in required_columns:
            null_count = df.filter(F.col(column).isNull()).count()
            if null_count > 0:
                raise Exception(f"DQ Check Failed: {column} has {null_count} NULL values.")
        #logging.info("Data Quality Check: Successfully processed Null record check.")


    # Check for system ID and reporting ID combination
    if system_id and reporting_id:
        sys_report_count = df.filter( (F.col(system_id["col_name"]) != system_id["value_id"]) & (F.col(reporting_id["col_name"]) != reporting_id["value_id"]) ).count()
        if sys_report_count > 0:
            raise Exception(f"DQ Check Failed: Found {sys_report_count} records with incorrect system ID or reporting ID.")
        
        #logging.info("Data Quality Check: Successfully processed System Id and Reporting Id record check.")


    # If all checks pass, return success status
    return True

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

def update_processing_time_and_exit(etl_pipeline_response_dict, etl_start_time):
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
    notebook_exit_response(notebook_response=etl_pipeline_response_dict)

# COMMAND ----------

def add_widget(dbutils, name, value):
    """
    Adds a text widget with the specified name and value.

    Args:
    dbutils: The dbutils object.
    name (str): The name of the widget.
    value (str): The default value of the widget.

    Returns:
    None
    """
    dbutils.widgets.text(name, value)

def get_widget(dbutils, name):
    """
    Retrieves the value of the text widget with the specified name.

    Args:
    dbutils: The dbutils object.
    name (str): The name of the widget.

    Returns:
    str: The value of the widget if it exists, otherwise None.
    """
    return dbutils.widgets.get(name)

def get_notebook_id(dbutils):
    notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    notebook_id = notebook_info["extraContext"]["notebook_id"] 
    return notebook_id

# COMMAND ----------

def to_sql_expression(condition, table_alias='existing'):
    """
    Generate an SQL expression from a filter condition with a table alias.

    Args:
        condition (dict): Contains 'column', 'operator', and 'value' for the SQL condition.
        table_alias (str, optional): Table alias to prefix the column name. Defaults to 'existing'.

    Returns:
        str: SQL expression formatted with the table alias and condition.
    """
    column = condition['column']
    operator = condition['operator']
    value = condition['value']
    
    return f"{table_alias}.{column} {operator} '{value}'"


# COMMAND ----------

def write_to_delta_dim_with_upsert(df: DataFrame, db_name: str, tbl_name: str, merge_keys: list,
                                   surrogate_key: str,  business_unit_code_filter: dict):
    """
    Writes a DataFrame to a Delta table using upsert (merge) operation for dimension tables.
    
    Args:
        df (DataFrame): DataFrame to be written.
        db_name (str): Database name to store the data.
        tbl_name (str): Table name to store the data.
        merge_keys (list): List of column names to be used as merge keys.
        surrogate_key (str): Surrogate key of the dataframe.
        business_unit_code_filter (dict): A dictionary which contains a column name, an operator, and a value for filtering during the merge operation.
    """
    # Get the list of existing columns from the DataFrame
    existing_columns = df.columns

    # Create a dictionary mapping existing columns to new columns
    update_cols_dict = {f"existing.{col_name}": f"new.{col_name}" for col_name in existing_columns}
    # Remove audit columns and surrogate key
    update_cols_dict.pop("existing.XTNCreatedTime", None)
    update_cols_dict.pop("existing.XTNCreatedById", None)
    update_cols_dict.pop(f"existing.{surrogate_key}", None)

    # Constructing filter expression based on the business unit code to access specific part of data while doing upsert
    business_unit_code_filter_exprs = to_sql_expression(business_unit_code_filter, 'existing')

    # Create merge expressions based on merge key
    merge_exprs = " AND ".join(f"existing.{key} = new.{key}" for key in merge_keys)
    # Adding business unit code filter in the merge expression 
    merge_exprs += f" AND {business_unit_code_filter_exprs}"
    
    delta_table_name = f"{db_name}.{tbl_name}"

    # Check if Delta table exists
    if is_table_exists(db_name, tbl_name):
        delta_table = DeltaTable.forName(spark, delta_table_name)
        # Upsert (merge) operation
        delta_table.alias("existing")\
            .merge(df.alias("new"), merge_exprs)\
            .whenMatchedUpdate(set=update_cols_dict)\
            .whenNotMatchedInsertAll()\
            .whenNotMatchedBySourceDelete(condition=business_unit_code_filter_exprs)\
            .execute()
    else:
        raise TableNotFoundException(delta_table_name)

# COMMAND ----------

def write_to_delta_fact_with_upsert(df: DataFrame, db_name: str, tbl_name: str, merge_keys: list,
                                   surrogate_key: str, load_type: str, business_unit_code_filter: dict, 
                                   cutoff_filter: Optional[dict]=None, source_mismatch_update_values: Optional[dict]=None):
    """
    Performs an upsert (merge) operation to a Delta table for fact tables using a DataFrame.
    

    Args:
        df (DataFrame): The DataFrame to be merged into the Delta table.
        db_name (str): The name of the database where the Delta table resides.
        tbl_name (str): The name of the Delta table to be updated.
        merge_keys (list): List of column names used as keys for merging the DataFrame with the Delta table.
        surrogate_key (str): The surrogate key column name for the DataFrame. Used to exclude this key from the update columns.
        load_type (str): Indicates the type of load. It can be either "full_load" or "incremental".
        business_unit_code_filter (dict): A dictionary which contains a column name, an operator, and a value for filtering during the merge operation.
        cutoff_filter: Optional[dict]: Required when performing incremental load. A dictionary which contains a column name, an operator, and a value for filtering last 90 days data
        source_mismatch_update_values: Optional[dict]: Required when performing incremental load. A dictionary specifying the columns and values to be updated when records do not match 
                                             by source. For example, {"Record_Active": False}.
    """
    
    # Get the list of existing columns from the DataFrame
    existing_columns = df.columns

    # Create a dictionary mapping existing columns to new columns for updates
    update_cols_dict = {f"existing.{col_name}": f"new.{col_name}" for col_name in existing_columns}
    
    # Remove audit columns and surrogate key from the update columns dictionary
    update_cols_dict.pop("existing.XTNCreatedTime", None)
    update_cols_dict.pop("existing.XTNCreatedById", None)
    update_cols_dict.pop(f"existing.{surrogate_key}", None)
    
    # Constructing BU filter expression to access specific part of data while doing upsert
    business_unit_code_filter_expr = to_sql_expression(business_unit_code_filter, 'existing')

    # Creating merge_exprs_with_bu_filter by combining merge expression with business unit code filter 
    merge_exprs = " AND ".join(f"existing.{key} = new.{key}" for key in merge_keys)
    merge_exprs_with_bu_filter = merge_exprs + f" AND {business_unit_code_filter_expr}"    
    
    # Formulate the full Delta table name
    delta_table_name = f"{db_name}.{tbl_name}"

    # Check if the Delta table exists
    if is_table_exists(db_name, tbl_name):
        # Obtain the Delta table instance
        delta_table = DeltaTable.forName(spark, delta_table_name)
        # Perform the upsert (merge) operation based on the load type
        if load_type == "full_load":
            delta_table.alias("existing")\
                .merge(df.alias("new"), merge_exprs_with_bu_filter)\
                .whenMatchedUpdate(set=update_cols_dict)\
                .whenNotMatchedInsertAll()\
                .execute()
        elif load_type == "incremental":
            # Creating the update_if_not_matched_condition to pick the required data alone for updating records that are not matched by source
            cutoff_filter_expr = to_sql_expression(cutoff_filter, 'existing')
            update_if_not_matched_condition = f"{business_unit_code_filter_expr} AND {cutoff_filter_expr}"

            # While load_type is incremental, adding the cutoff filter expression to the merge expressions for performance improvement. By doing so, we are predicating target table data and selecting required data alone to do the upsert operation
            merge_exprs_with_bu_filter += f" AND {cutoff_filter_expr}"
            
            delta_table.alias("existing")\
                .merge(df.alias("new"), merge_exprs_with_bu_filter)\
                .whenMatchedUpdate(set=update_cols_dict)\
                .whenNotMatchedInsertAll()\
                .whenNotMatchedBySourceUpdate(condition=update_if_not_matched_condition, set=source_mismatch_update_values)\
                .execute()
        else:
            raise ValueError(f"Unknown data load_type received {load_type}. Load type can be either full_load or incremental")

    else:
        # Raise an exception if the Delta table does not exist
        raise TableNotFoundException(delta_table_name)

# COMMAND ----------


def get_table_system_params_dict(system_name: str, table_name: str, env: str) -> Dict[str, Any]:
    """
    Generates a configuration dictionary based on the combination of system_name and table_name.

    Args:
        system_name (str): The system name (e.g., 'PIRT', 'PGT', 'LATAM', 'PIRT_RU').
        table_name (str): The table name (e.g., 'fact_tables', 'Order').
        env (str): The environment identifier (e.g., 'dev', 'prod').

    Returns:
        Dict[str, Any]: A dictionary containing the configuration parameters.
    """
    # Mapping for different system and table name combinations
    config_mapping = {
                        "EUOTC_PIRT": {
                                       
                                    "FulfillmentPlan": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRT",   #### neeed to talk source_sys_name
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                    "xtnfulfillmentplanline": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRT",   #### neeed to talk source_sys_name
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                    
                                    "dim_tables": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 107240,
                                                "system_id": 8,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": env[0].upper() + "16",
                                    },
                                    "xtnroutetransportationlogistic" : {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 107240,
                                                "SystemId": 8,
                                                "business_unit_code": None,
                                                "InstanceId": env[0].upper() + "16",    #### Not using
                                             }, 
                                    "deliverydelaytemplate" : {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,    #### Not using
                                             },
                                    "order": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                    "xtnorderline": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                    "salescustomer": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "location": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "material": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "SalesDocumentRejectionReasonDescription": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "SalesDocumentOrderReasonDescription": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "SalesOrganization": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    }
                                },
                        "EUOTC_PIRTRU": {
                                    "FulfillmentPlan": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRTRU",
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                     "xtnfulfillmentplanline": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRTRU",
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                    "dim_tables": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 107240,
                                                "system_id": 8,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": env[0].upper() + "16",
                                    }
                                    ,
                                    "xtnroutetransportationlogistic" : {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 107240,
                                                "SystemId": 8,
                                                "business_unit_code": None,
                                                "InstanceId": env[0].upper() + "16",    #### Not using
                                             }, 
                                    "deliverydelaytemplate" : {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,    #### Not using
                                             },
                                    "order": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRTRU",
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                    "xtnorderline": {
                                                "XTNDFSystemId": 501008,
                                                "XTNDFReportingUnitId": 106903,
                                                "SystemId": 8,
                                                "business_unit_code": "EUOTC_PIRTRU",
                                                "InstanceId": env[0].upper()+'16',
                                    },
                                    "salescustomer": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "location": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "material": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "SalesDocumentRejectionReasonDescription": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "SalesDocumentOrderReasonDescription": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    },
                                    "SalesOrganization": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,
                                    }
                                },

                        "EUOTC_PGT": {
                                    "FulfillmentPlan": {
                                                "XTNDFSystemId": 1702,
                                                "XTNDFReportingUnitId": 107019,
                                                "SystemId": 702,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": 'R1'+env[0].upper(),
                                    },
                                    "xtnfulfillmentplanline": {
                                                "XTNDFSystemId": 1702,
                                                "XTNDFReportingUnitId": 107019,
                                                "SystemId": 702,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": 'R1'+env[0].upper(),
                                    },
                                    "xtnroutetransportationlogistic" : {
                                                "XTNDFSystemId": 1702,
                                                "XTNDFReportingUnitId": 107019,
                                                "SystemId": 702,
                                                "business_unit_code": None,
                                                "InstanceId": 'R1'+env[0].upper(),    #### Not using
                                             }, 
                                    "deliverydelaytemplate" : {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PIRT",
                                                "InstanceId": None,    #### Not using
                                             },
                                    "order": {
                                                "XTNDFSystemId": 1702,
                                                "XTNDFReportingUnitId": 107019,
                                                "SystemId": 702,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": 'R1'+env[0].upper(),
                                    },
                                    "xtnorderline": {
                                                "XTNDFSystemId": 1702,
                                                "XTNDFReportingUnitId": 107019,
                                                "SystemId": 702,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": 'R1'+env[0].upper(),
                                    },
                                    "salescustomer": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": None,
                                    },
                                    "location": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": None,
                                    },
                                    "material": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": None,
                                    },
                                    "SalesDocumentRejectionReasonDescription": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": None,
                                    },
                                    "SalesDocumentOrderReasonDescription": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": None,
                                    },
                                    "SalesOrganization": {
                                                "XTNDFSystemId": None,
                                                "XTNDFReportingUnitId": None,
                                                "SystemId": None,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": None,
                                    },
                                    "dim_tables": {
                                                "XTNDFSystemId": 1548,
                                                "XTNDFReportingUnitId": 107019,
                                                "SystemId": 627,
                                                "business_unit_code": "EUOTC_PGT",
                                                "InstanceId": "GM" + env[0].upper(),
                                    },
                                },
                        

                        "LATAMOTC_PIRT": {
                                    "fact_tables": {
                                                "XTNDFSystemId": 501002,
                                                "XTNDFReportingUnitId": 106906,
                                                "SystemId": 2,
                                                "business_unit_code": "LATAMOTC_PIRT",
                                                "InstanceId": env[0].upper() + "E1",
                                            },
                                    "dim_tables": {
                                                "XTNDFSystemId": 501002,
                                                "XTNDFReportingUnitId": 107240,
                                                "system_id": 2,
                                                "business_unit_code": "LATAMOTC_PIRT",
                                                "InstanceId": env[0].upper() + "E1",
                                    }
                                }
    }   

    # Validate the system name and table name
    if system_name not in config_mapping:
        raise ValueError(f"Invalid system_name: {system_name}. Available options are: {', '.join(config_mapping.keys())}")
    
    if table_name not in config_mapping[system_name]:
        raise ValueError(f"Invalid table_name: {table_name} for system_name: {system_name}. Available options are: {', '.join(config_mapping[system_name].keys())}")

    # Get the base configuration based on the system and table name
    base_config = config_mapping[system_name][table_name]

    return base_config
