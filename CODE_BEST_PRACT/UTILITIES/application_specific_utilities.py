# Databricks notebook source
# MAGIC %md
# MAGIC ##### - [Application/Context Specific] Functions
# MAGIC
# MAGIC This notebook contains functions specific for [application/context]. 
# MAGIC
# MAGIC **Usage**:
# MAGIC 1. Import the script or module containing the functions into your Python script or notebook.
# MAGIC 2. Call the desired function with the appropriate parameters to perform specific tasks.
# MAGIC 3. Handle the returned values or utilize the functions' side effects as needed.
# MAGIC
# MAGIC ##### Function Overview
# MAGIC
# MAGIC |Function Name|Description|
# MAGIC |-------------|-----------|
# MAGIC |`filter_excluded_bu_to_all`| |
# MAGIC |``| |
# MAGIC |...|...|
# MAGIC
# MAGIC  ##### History
# MAGIC |Date|Author|Change Description|Event|
# MAGIC |---------|------------------|--------------------|--------------------|
# MAGIC |08-01-2023 |Chandra |Added `filter_excluded_bu_to_all` | Initial Development |
# MAGIC |28-02-2024 | Rohith    |Adding exception_info key while arising exception and importing function which is required for pytest |
# MAGIC |03-06-2024 | Rohith    | Updating `generate_excluded_reasons` to include Russia's data|

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta import DeltaTable
from datetime import datetime, timedelta
import json
from typing import Dict,List,Optional, Union

#pyspark libs
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType,IntegerType,StringType,LongType,DoubleType,DateType,BooleanType, StructType

from delta import DeltaTable
from pyspark.sql import SparkSession,DataFrame

#python libs
import json
from datetime import datetime
import re
import pip
import sys
import subprocess
from typing import Dict,List,Optional,Union,Tuple
#import logging
from itertools import product

try:
  import os
  import sys
  from pyspark.dbutils import DBUtils
  #setting path 
  current = os.path.dirname(os.path.realpath(__file__))
  sys.path.append(current)
  from util_functions import get_dataframe, generate_filter_condition, notebook_exit_response, check_view_exists, validate_date_format
  # creating spark session and dbutils object for pytest 
  spark = SparkSession.builder.getOrCreate() 
  dbutils = DBUtils(spark)
except NameError:
  print("This NameError is expected during notebook execution. The object creation and function import above are used for unit testing purposes.")

#Azure libs
#from azure.storage.blob import BlobServiceClient  
# from azure.identity import ClientSecretCredential
# from azure.storage.filedatalake import DataLakeServiceClient

# COMMAND ----------

def get_data_from_table( table_params: Dict, col_list: Optional[List] = None, \
                         filter_template: Optional[List[Dict[str, str]]] = None,
                         transformed_or_new_cols_dict: Optional[Dict[str, str]] = None,
                         distinct_flag: Optional[bool] = False
                       ) -> DataFrame:
    """
    DESCRIPTION: Get data from a table based on provided parameters.

    Args:
        table_params (Dict): Dictionary containing database and table information.
        col_list (Optional[List]): List of columns to be selected. Defaults to None.
        filter_template (Optional[List[Dict[str, str]]]): Template for filter conditions. Defaults to None.
        transformed_or_new_cols_dict (Optional[Dict[str, str]]): Dictionary for column transformations. Defaults to None.
        distinct_flag (Optional[bool]): Flag for distinct results. Defaults to False.

    Returns:
        DataFrame: Loaded data from the specified table.

    Raises:
        Exception: If unable to retrieve data.

    """

    schema_name = table_params.get('database_name')
    table_name = table_params.get('table_name')

    try:
        filter_cond = generate_filter_condition(filter_template) if filter_template else None

        table_df = get_dataframe(schema_name, table_name, filter_cond, col_list, transformed_or_new_cols_dict, distinct_flag)

        # logging.info(f"***** Input Parameters to get_data_from_table function *****")
        # logging.info(f"Database Name: {schema_name}")
        # logging.info(f"Table Name: {table_name}")
        # logging.info(f"Columns List: {col_list}")
        # logging.info(f"Filter Template: {filter_cond}")
        # logging.info(f"Transformations: {transformed_or_new_cols_dict}")
        # logging.info(f"Distinct Flag: {distinct_flag}")
        # logging.info(f"Successfully loaded data from '{schema_name}.{table_name}'.")
    

        return table_df

    except Exception as e:
        #logging.error(f"Error loading data from '{schema_name}.{table_name}': {str(e)}")
        raise e

# COMMAND ----------

def reduce_list (list_):
  """ Create a list of values, it will be used in the process of either include BU filters or exclude BU filters"""
  in_clause1 = ''
  is_first_bu_loop = True
  for x in list_:
    if is_first_bu_loop:
      in_clause1 = in_clause1 + f"'{x}'"
      is_first_bu_loop = False
    else:
      in_clause1 = in_clause1 + f",'{x}'"
  return in_clause1

def filter_excluded_bu_to_all(BU_FILTERS):
  """ Definition for: filtering data using common BU requirements(exclusion)"""
  is_first=True
  fil_ = ''
  if BU_FILTERS is None or len(BU_FILTERS) == 0:
    print("No filters applicable to exclude for all BUs")
    return ''
  else:
    for column_name, filter_values in BU_FILTERS.items():
      if is_first:
        if type(filter_values) == list:
          fil_ = fil_ + f"{column_name} not in ({reduce_list(filter_values)})"
        else:
          fil_ = fil_ + f"{column_name} != '{filter_values}'"
        is_first = False
      else:
        if type(filter_values) == list:
          fil_ = fil_ + f" AND {column_name} not in ({reduce_list(filter_values)})"
        else:
          fil_ = fil_ + f" AND {column_name} != '{filter_values}'"
  print(f"applying filter to exclude for all BUs: {fil_}")
  return fil_

def filter_excluded_bu(BU_COLUMN, BU_ORGANIZATIONS, BU_FILTERS):
  """ Definition for: filtering data using by each BU (exclusion)"""
  is_first=True
  fil_ = ''
  for bu, single_bu_filter in BU_FILTERS.items():
    if is_first:
      fil_ = fil_ + f"({BU_COLUMN} in ({reduce_list(BU_ORGANIZATIONS[bu])})" 
    else:
      fil_ = fil_ + f" OR ({BU_COLUMN} in ({reduce_list(BU_ORGANIZATIONS[bu])})"  
    
    if single_bu_filter is None:
      fil_ = fil_ + ")"
      is_first = False
      continue
    else:
      for column_name,filter_values in single_bu_filter.items():
        if type(filter_values) == list:
          fil_ = fil_ + f" AND {column_name} not in ({reduce_list(filter_values)})"
        else:
          fil_ = fil_ + f" AND {column_name} != '{filter_values}'"
      fil_ = fil_ + ")"
      is_first = False
  print(f"applying filter to exclude in the table: {fil_}")
  return fil_

# COMMAND ----------

## Definition for: filtering data using common BU requirements (including)
def filter_included_bu_to_all(BU_FILTERS):
  is_first=True
  fil_ = ''
  if BU_FILTERS is None or len(BU_FILTERS) == 0:
    print("No filters applicable to include for all BUs")
    return ''
  else:
    for column_name, filter_values in BU_FILTERS.items():
      if is_first:
        if type(filter_values) == list:
          fil_ = fil_ + f"{column_name} in ({reduce_list(filter_values)})"
        else:
          fil_ = fil_ + f"{column_name} = '{filter_values}'"
        is_first = False
      else:
        if type(filter_values) == list:
          fil_ = fil_ + f" AND {column_name} in ({reduce_list(filter_values)})"
        else:
          fil_ = fil_ + f" AND {column_name} = '{filter_values}'"
  print(f"applying filter to include for all BUs: {fil_}")
  return fil_
  
### Definition for: filtering data using by each BU (including)
def filter_included_bu(BU_COLUMN, BU_ORGANIZATIONS, BU_FILTERS):
  is_first=True
  fil_ = ''
  for bu, single_bu_filter in BU_FILTERS.items():
    if is_first:
      fil_ = fil_ + f"({BU_COLUMN} in ({reduce_list(BU_ORGANIZATIONS[bu])})" 
    else:
      fil_ = fil_ + f" OR ({BU_COLUMN} in ({reduce_list(BU_ORGANIZATIONS[bu])})"  
    
    if single_bu_filter is None:
      fil_ = fil_ + ")"
      is_first = False
      continue
    else:
      for column_name,filter_values in single_bu_filter.items():
        if type(filter_values) == list:
          fil_ = fil_ + f" AND {column_name} in ({reduce_list(filter_values)})"
        else:
          fil_ = fil_ + f" AND {column_name} = '{filter_values}'"
      fil_ = fil_ + ")"
      is_first = False
  print(f"applying filter to include in the table: {fil_}")

  return fil_

# COMMAND ----------

def apply_business_unit_filters(
                                data:DataFrame, bu_include_all_dict:Dict, 
                                bu_exclude_all_dict:Dict, 
                                bu_organizations_dict:Dict, 
                                bu_filter_included_dict:Dict, 
                                bu_filter_excluded_dict:Dict,
                                bu_column_name_str:str
                                ):
    """Apply exclusion and inclusion filters based on business units
    Args:
        data (DataFrame): The input DataFrame to be filtered.
        bu_include_all_dict (Dict[str, List[str]]): Dictionary  to include all data for each business unit.
        bu_exclude_all_dict (Dict[str, List[str]]): Dictionary  to exclude all data for each business unit.
        bu_organizations_dict (Dict[str, List[str]]): Dictionary of business unit organizations.
        bu_filter_included_dict (Dict[str, Dict[str, List[str]]]): Dictionary to apply inclusion filter for each business unit.
        bu_filter_excluded_dict (Dict[str, Dict[str, List[str]]]): Dictionary  to apply exclusion filter for each business unit.
        bu_column_name_str: string : BU column name 

    Returns:
        DataFrame: The filtered DataFrame.
    """
    filter_excluded_df = data.filter(filter_excluded_bu(bu_column_name_str, bu_organizations_dict, bu_filter_excluded_dict)).distinct()

    if bu_exclude_all_dict:
        filter_excluded_df = filter_excluded_df.filter(filter_excluded_bu_to_all(bu_exclude_all_dict)).distinct()


    filtered_included_df = filter_excluded_df.filter(filter_included_bu(bu_column_name_str, bu_organizations_dict, bu_filter_included_dict)).distinct()
    if bu_include_all_dict:
        filtered_included_df = filtered_included_df.filter(filter_included_bu_to_all(bu_include_all_dict)).distinct()

    return filtered_included_df

# COMMAND ----------

def generate_excluded_reasons(
                                config_data: Dict[str, list],
                                spark: SparkSession,
                                result_schema: StructType = None
                            )  -> DataFrame:
    """
    Generate excluded reasons DataFrame based on sales organizations and reason codes.
    (1) Exclude Cut Reason Dataframe
    (2) Exclude Cut Rejection Dataframe

    Args:
        config_data (Dict[str, list]): Dictionary containing lists of sales organizations and reason codes.
        spark (SparkSession): Spark session.
        result_schema StructType: Contains schema for the resulting DataFrame.
            Defaults to None.
    Returns:
        DataFrame: Excluded reasons DataFrame with columns specified by 'result_column_names'.

    Raises:
        ValueError: If 'config_data'  is not provided.
    """
    if not config_data:
        raise ValueError("Both 'config_data' must be provided.")

    # Generate all combinations of sales organizations and reason codes.Product function computes the Cartesian product of input iterables. 
    combinations = []
    for org_key in config_data["bu_organizations"].keys():
        combinations.extend(product(config_data['bu_organizations'][org_key], config_data['reason_code'][org_key]))
        #logging.info(f"Possible combinations are created for : {org_key,reason_key}")

    excluded_reasons_df = spark.createDataFrame(combinations, result_schema) #.withColumn("auxiliary", F.lit(True)).distinct()
    #logging.info(f"Successfully created the dataframe with {result_column_names} data")

    return excluded_reasons_df

# COMMAND ----------

def identify_three_month_filter_start_date(spark: SparkSession, target_table: str) -> None:
    """ 
    Identify the three mont data start date filter need for header tables and perform the appropriate action.
    Parameters:
    - spark: SparkSession
    - target_table: str, the name of the target table
    Returns:
    - three_month_filter_start_date: str
    """
    try:
        if not spark.table(target_table).isEmpty():  
            # Calculate the date 3 months earlier
            current_date_str = datetime.now().strftime("%Y-%m-%d")            
            three_month_filter_start_date = (datetime.strptime(current_date_str, '%Y-%m-%d') - timedelta(days=90)).strftime("%Y-%m-%d")
        else:
            three_month_filter_start_date = False

        return  three_month_filter_start_date
    except Exception as e:
        raise Exception(f"Error: {e}")

# COMMAND ----------

def load_type_and_three_month_window_selector( 
                                                load_type: str, 
                                                three_month_data_filter_start_date: str, 
                                                three_month_data_filter_end_date: str, 
                                                target_table:str,
                                                expected_format: str = "%Y-%m-%d",
                                             ) -> Tuple[str, str, str]:
    """
    Validates input parameters and sets the manual_incremental_window flag.

    Parameters:
    - load_type (str): The type of data load, e.g., "full_load" or "incremental_load".
    - three_month_data_filter_start_date (str): Start date for the incremental window.
    - three_month_data_filter_end_date (str): End date for the incremental window.
    - expected_format (str): The expected date format for validation (default is "%Y-%m-%d").

    Returns:
    - Tuple[str, str, str]: A tuple containing three values: 
        1. The start date for the incremental window.
        2. The end date for the incremental window.
        3. The type of data load.

    Raises:
    - ValueError: If there is an error during input validation.
    """
    try:
        #Step 1: Check if the user has provided manual inputs for incremental load and Perform incremental load using the provided manual inputs.
        # This condition is triggered when the user explicitly specifies incremental load parameters.
        if three_month_data_filter_start_date and three_month_data_filter_end_date:
            if (
                validate_date_format(date_string=three_month_data_filter_start_date, expected_format=expected_format)
                and validate_date_format(date_string=three_month_data_filter_end_date, expected_format=expected_format)
               ):
                load_type = "incremental"

                #logging.info("***** User-provided Manual Inputs Identified. Received input parameters and manual override details *****")
                #logging.info(f"User Widget Param(manual override): Load type set to incremental. Delta load will be captured between the specified {three_month_data_filter_start_date} and  {three_month_data_filter_end_date}.")
            else:
                raise ValueError(f"User provided details are incorrect. Invalid date format. Please provide dates in the format {expected_format}.")
        
        # This condition is triggered when the user explicitly specifies a full load.
        elif load_type.lower() == "full_load":
            ##logging.info(f"User Widget Param: load_type(manual override): {load_type}")
            #logging.info("Load type set to full. Full load will be performed for the entire dataset.")
            pass 
        # This condition is triggered when no manual inputs are provided, and the load type is Automatically determined based on the target table state.
        else:
            # If the target table has data, set the load type to incremental and define the incremental window.
            # Otherwise, perform a full load for the entire dataset.
            incremental_water_mark_col = "XTNCreatedTime"
            # Call the function to identify the load type and perform the appropriate action in an automated way
            three_month_data_filter_end_date = datetime.now().strftime("%Y-%m-%d")
            three_month_data_filter_start_date = identify_three_month_filter_start_date(spark=spark, target_table=target_table)
            #logging.info("***** Automatically determined load type and incremental window based on target table state *****")

            if three_month_data_filter_start_date and three_month_data_filter_end_date:
                load_type = "incremental"
                #logging.info(f"Since the load type has been identified as {load_type}, data from Silver header tables will be pulled for the period between {three_month_data_filter_start_date} and {three_month_data_filter_end_date}")
            else:
                load_type = "fullload"
                #logging.info(f"Successfully retrieved the load type as {load_type} based on target table state. Full load will be performed for the entire dataset.")
                
        return three_month_data_filter_start_date, three_month_data_filter_end_date, load_type

    except Exception as e:
        notebook_exit_response(
                                notebook_response=str(e),
                                exception_info={"exception_message": str(e), "status": "failed", "etl_end_time": str(datetime.now())},
                              )

# COMMAND ----------

def invoke_child_notebook( fact_table_name: str, child_notebook_input_param_dict: Dict[str, Union[str, int]], dataframe_obj_list: List[str],spark ) -> Dict[str, DataFrame]:
    """
    Invokes a child notebook and retrieves DataFrames from its outputs.

    Parameters:
    - fact_table_name (str): The name of the fact table for logging purposes.
    - child_notebook_input_param_dict (Dict): Dictionary containing input parameters for the child notebook.
      It should include keys like 'notebook_path', 'timeout_seconds', and 'arguments'.
    - dataframe_obj_list (List[str]): List of DataFrame names to retrieve from the child notebook outputs.
    - spark: The SparkSession object.

    Returns:
    - Dict[str, DataFrame]: A dictionary where keys are DataFrame names and values are Spark DataFrames.

    Raises:
    - Exception: If there is an error during the execution of the child notebook or if a specified view does not exist.
    """

    try:
        # Step 1: Invoke Child Notebook
        data_df_json_str = dbutils.notebook.run(
                                                path=child_notebook_input_param_dict["notebook_path"],
                                                timeout_seconds=child_notebook_input_param_dict["timeout_seconds"],
                                                arguments=child_notebook_input_param_dict["arguments"]
                                               )

        data_df_json_obj = json.loads(data_df_json_str)

        # Step 2: Verify if the Child Notebook executed successfully
        if data_df_json_obj["success_status"].get("return_status_flag"):
            #logging.info(f"{fact_table_name} Child notebook executed successfully", data_df_json_obj)

            # Create a dictionary to store dataframes
            dataframes_dict = {}

            # Step 3: Create DataFrame from the global temp tables
            global_temp_db = spark.conf.get("spark.sql.globalTempDatabase")

            for df_name in dataframe_obj_list:
                df_view_name = data_df_json_obj.get(df_name)

                # Check if the view exists
                if check_view_exists(spark, df_view_name, global_temp_db):
                    # If the view exists, retrieve the DataFrame
                    df = spark.table(f"{global_temp_db}.{df_view_name}")
                    dataframes_dict[df_name] = df
                    #logging.info(f"{df_name} Data retrieved successfully")
                else:
                    raise Exception(f"The view '{df_view_name}' does not exist.")

            return dataframes_dict
        else:
            raise Exception(data_df_json_obj.get("exception_info"))

    except Exception as e:
        notebook_exit_response(notebook_response=f"Error while invoking child notebook: {str(e)}", exception_info={"exception_message":str(e),"status":"failed",'etl_end_time':str(datetime.now())})


# COMMAND ----------

def pivot_dataframe(input_df: DataFrame, group_by_column: str, pivot_column: str, aggregation_columns: str) -> DataFrame:
    """
    Pivot a DataFrame based on specified columns and perform aggregation.

    Parameters:
    - input_df (DataFrame): The input DataFrame.
    - group_by_column (str): The column to group by.
    - pivot_column (str): The column to pivot.
    - aggregation_columns (str): Columns to aggregate.

    Returns:
    - DataFrame: The pivoted DataFrame.
    """
    pivot_df = (
                input_df.groupBy(group_by_column)
                .pivot(pivot_column)
                .agg(*[F.first(col).alias(col) for col in aggregation_columns])
                .fillna(0)
              )

    return pivot_df

# COMMAND ----------

def generate_quantity_conversion_expression(qnty_col: F.col, conversion_unit_prefix_name: str, alias_name: str) -> F.col:
    """
    Convert a quantity column to a new unit using the provided conversion factors.

    Parameters:
    - qnty_col (Column): The quantity column to be converted.
    - conversion_unit_prefix_name (str): The prefix name for the conversion unit. [CS or EA]
    - alias_name (str): The alias name for the new converted column.

    Returns:
    - Column: A Spark DataFrame column representing the converted quantity.
    """
    BaseUnitConversionNumeratorFactor = f"{conversion_unit_prefix_name}_BaseUnitConversionNumeratorFactor"
    BaseUnitConversionDenominatorFactor = f"{conversion_unit_prefix_name}_BaseUnitConversionDenominatorFactor"

    quantity_exp = (
                    (F.col(qnty_col) * F.col(BaseUnitConversionDenominatorFactor)) /  F.col(BaseUnitConversionNumeratorFactor)
                   ).alias(alias_name)

    return quantity_exp

# COMMAND ----------


