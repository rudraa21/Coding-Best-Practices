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

try:
  import os
  import sys
  from pyspark.dbutils import DBUtils
  #setting path 
  current = os.path.dirname(os.path.realpath(__file__))
  sys.path.append(current)
  from common_utilities import get_dataframe,read_json_metadata,update_processing_time_and_exit,notebook_exit_response,get_vmi_mapping_data_from_confing_json,write_df,is_table_exists
except NameError:
  print("This NameError is expected during notebook execution. The object creation and function import above are used for unit testing purposes.")

# COMMAND ----------

#pyspark libs
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType,IntegerType,StringType,LongType,DateType,StructField,StructType
from delta import DeltaTable
from pyspark.sql import SparkSession,DataFrame, Column
from pyspark.sql.window import Window


#python libs
import json
from datetime import datetime, timedelta
import re
import pip
import sys
import subprocess
from typing import Dict,List,Optional,Any
import logging


from typing import List
from datetime import datetime, timedelta

def get_business_specific_filter_template_for_vmi_interface_and_retailer(interface: str, retailer: str, interface_data_filter_col: str) -> List[dict]:
    """
    DESCRIPTION: Generate date filter template based on interface and retailer.
    
    PARAMS:
        interface (str): The interface type. It must be one of 'openorders', 'inventory', 'ordershistory', or 'demandtodate'.
        retailer (str): The retailer name. It must be either 'carrefour' or 'delhaize'.
        interface_data_filter_col (str): The column name used for filtering data in the interface.
        
    RETURNS:
        List[dict]: A list of dictionaries representing the filter template.
    """
    assert retailer.lower() in ['carrefour', 'delhaize'], "Retailer must be 'CARREFOUR' or 'DELHAIZE'"
    assert interface.lower() in ['openorders', 'inventory', 'ordershistory', 'demandtodate'], "Interface must be 'openorders', 'inventory', 'ordershistory', or 'demandtodate'"

    today = datetime.now().date()

    if interface.lower() == 'openorders':
        if retailer.lower() == "delhaize":
            return [{"col_name": interface_data_filter_col, "col_val": f"'{today}'", "operator": '=='}]
        if retailer.lower() == "carrefour":
            raise Exception("The open order interface does not include carrefour Retailer within its scope")

    if interface.lower() == 'inventory':
        if retailer.lower() == "delhaize":
            return [{"col_name": interface_data_filter_col, "col_val": f"'{today}'", "operator": '=='}]
        elif retailer.lower() == "carrefour":
            yesterday = today - timedelta(days=1)
            return [{"col_name": interface_data_filter_col, "col_val": f"'{yesterday}'", "operator": '=='}]

    if interface.lower() == 'demandtodate':
        if retailer.lower() == "delhaize":
            start_of_week = today - timedelta(days=(today.weekday()) % 7)
            start_of_week = start_of_week + timedelta(days=1)
            return [{"col_name": interface_data_filter_col, "col_val": f"'{start_of_week}'", "operator": '>='},
                    {"col_name": interface_data_filter_col, "col_val": f"'{today}'", "operator": '<='}]
        elif retailer.lower() == "carrefour":
             start_of_week = today - timedelta(days=(today.weekday()) % 7)
             yesterday = today - timedelta(days=1)
             return [{"col_name": interface_data_filter_col, "col_val": f"'{start_of_week}'", "operator": '>='},
                    {"col_name": interface_data_filter_col, "col_val": f"'{yesterday}'", "operator": '<='}]

    if interface.lower() == 'ordershistory':
        today = datetime.now() - timedelta(days=1)
        start_of_week = today - timedelta(days=(today.weekday()) % 7)
        today_date = today.date()
        start_of_week_date = start_of_week.date()
        return [{"col_name": interface_data_filter_col, "col_val": f"'{start_of_week_date}'", "operator": '>='},
                {"col_name": interface_data_filter_col, "col_val": f"'{today_date}'", "operator": '<='}]


def extract_edi_msg_from_silver(
                                    spark: SparkSession,
                                    retailer_table_config: Dict[str, Any],
                                    interface_name: str,
                                    retailer_name: str
                                ) -> DataFrame:
    """
    Extracts EDI messages from the silver layer based on the provided configurations and filters.

    Args:
        spark: SparkSession object.
        retailer_table_config: Dictionary containing configuration details for retailer table.
        interface_name: Name of the interface.
        retailer_name: Name of the retailer.
        interface_business_data_filter_col: Column name for interface business data filter.

    Returns:
        DataFrame: DataFrame containing EDI messages from the silver layer.
    """
    interface_and_retailer_specific_filter_template = get_business_specific_filter_template_for_vmi_interface_and_retailer(
                                                                                                                            interface=interface_name,
                                                                                                                            retailer=retailer_name,
                                                                                                                            interface_data_filter_col=retailer_table_config["watermark_column"][interface_name]
                                                                                                                             )
    

    if interface_and_retailer_specific_filter_template:
        retailer_table_config["filter_template"].extend(interface_and_retailer_specific_filter_template)

    retailer_silver_df = get_dataframe(
                                        spark=spark,
                                        database_name=retailer_table_config["database_name"],
                                        table_name=retailer_table_config["table_name"],
                                        filter_template=retailer_table_config["filter_template"],
                                        list_of_columns_to_select=retailer_table_config["required_columns"],
                                        transformed_or_new_cols_dict=None,
                                        distinct_flag=False,
                                        list_of_keys_to_fetch_distinct_rows=None
                                       )

    return retailer_silver_df

def get_edi_jda_mapping_file_data(spark: SparkSession, config_data: Dict,expected_schema) -> DataFrame:
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
    
def mapping_edi_to_vmi_jda_mapping_file(
                            spark: SparkSession,
                            retailer_silver_df: DataFrame,
                            retailer_table_config: Dict[str, Any],
                            config_file_path: str,
                 ean_to_material_mapping           config_file_storage_type: str,
                            env: str
                            ) -> DataFrame:
    """
    Maps EDI data to JDA data based on provided configurations and mappings.

    Args:
        spark: SparkSession object.
        retailer_silver_df: DataFrame containing retailer silver data.
        retailer_table_config: Dictionary containing configuration details for retailer table.
        config_file_path: Path to the configuration file.
        config_file_storage_type: Storage type of the configuration file (adls2, dbfs, local).
        env: Environment identifier.

    Returns:
        DataFrame: DataFrame containing mapped EDI and JDA data.
    """
    file_path_mapping_dict = {
                                "adls2": f"abfss://vmi@europemdip{env}adls2.dfs.core.windows.net/{config_file_path}",
                                "dbfs": config_file_path,
                                "local": config_file_path
                            }
    
    expected_schema = StructType([
        StructField("DMDGROUP", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("JDA_CODE", StringType(), True),    
        StructField("JDA_LOC", StringType(), True),
        StructField("SOURCE_SYSTEM", StringType(), True),
        StructField("SOURCE_SYSTEM_COUNTRY", StringType(), True),
        StructField("SOURCE_UOM", StringType(), True),
        StructField("SUPPLIER_GLN", StringType(), True),
        StructField("WH_GLN", StringType(), True)
                                    ])
     
    config_data = read_json_metadata(file_path=file_path_mapping_dict[config_file_storage_type], storage_type=config_file_storage_type)

    if config_data["data"]:
        logging.info(f"Successfully initialized the config and filter params")
        vmi_edi_jda_mapping_df = get_vmi_mapping_data_from_confing_json(spark=spark, config_data=config_data["data"],expected_schema=expected_schema)

    edi_jda_mapping_join_cond = (
                                 (F.col("edi_msg.RetailWarehouseGlobalLocationNumber") == F.col("jda_mapping.WH_GLN")) &
                                 (F.col("edi_msg.SupplierGlobalLocationNumber") == F.col("jda_mapping.SUPPLIER_GLN"))
                                 )

    edi_jda_join_df = retailer_silver_df.alias("edi_msg").join(
                                                                vmi_edi_jda_mapping_df.alias("jda_mapping"),
                                                                on=edi_jda_mapping_join_cond,
                                                                how="inner"
                                                               )
    return edi_jda_join_df

def ean_to_material_mapping(
                            edi_jda_join_df: DataFrame,
                            ean_material_mapping_df: DataFrame
                            ) -> DataFrame:
    """
    Maps EAN to material based on provided mappings.

    Args:
        edi_jda_join_df: DataFrame containing joined EDI and JDA data.
        ean_material_mapping_df: DataFrame containing EAN to material mapping data.
        retailer_silver_to_gold_mapping_column_list: List of columns to select from the mapped DataFrame.

    Returns:
        DataFrame: DataFrame containing mapped data selected from the provided column list.
    """
    ean_to_material_map_df = edi_jda_join_df.alias("edi_jda_map").join(
                                                                        ean_material_mapping_df.alias("ean_mat_map"),
                                                                        on = F.col("ean_mat_map.XTNUniversalProductCode") == F.col("edi_jda_map.MaterialGlobalTradeItemNumber"),
                                                                        how="left"
                                                                     )


    return ean_to_material_map_df

def write_edi_to_adls2(
                            interface_output_df: DataFrame,
                            log_output_df:DataFrame,
                            log_output_adls2_path: str,
                            interface_output_adls2_path: str,
                            write_params_adls :dict,
                            interface: str,
                            retailer_name: str,
                            bronze_table_details : Dict
                       ) -> None:
    """
    Writes EDI to JDA data to appropriate locations and logs potential issues.

    Args:
        edi_to_jda_df: DataFrame containing EDI to JDA data.
        log_output_adls2_path: ADLS2 path to write log output.
        interface_output_adls2_path: ADLS2 path to write interface output.
        interface : Interface Name

    Returns:
        None
    """
    
    ########################################### Writing Data: Sending EDI messages with correct EAN-to-material mappings and valid data quality records to the interface via VMI ADLS2 ####
    write_params_adls.update({"adls_location":interface_output_adls2_path}) #This line of code is responsible for updating the parameters dictionary with the new ADLS location specified by interface_output_adls2_path
    interface_output_single_file_df = interface_output_df.coalesce(1)
    write_df(interface_output_single_file_df, write_params_adls,header=True)
    
    if not interface_output_single_file_df.isEmpty():                             
        write_params_adls.update({"interface_write_status":"Successfully Written the Interface data to VMI adls2"})
    else:
        write_params_adls.update({"interface_write_status":" No Interface records available to be written to Xface"})

    ########################################### Joining the log output DataFrame with the subset of bronze columns needed for logging purposes #####################################################################################
    bronze_columns_for_log_data = get_dataframe(
                                                    spark=spark,
                                                    database_name=bronze_table_details["database_name"],
                                                    table_name=bronze_table_details["table_name"],
                                                    filter_template=bronze_table_details["filter_template"],
                                                    list_of_columns_to_select=["MAILBOXID", "PARTNER", "DELIVERY_NUM", "INV_DATE", "INV_TIME", "MSG_GEN_DATE", "MSG_GEN_TIME", "RECEIPT_CONF", "DEPOT_NUM", "SENDER_GLN", "RECEIVER_GLN", "VMICUST_GLN", "SUPPLIER_GLN", "WH_GLN", "LINENUM", "STORE_ORDER_QTY", "MATERIAL_EAN", "PROM_VAR_CODE", "QTY_MISSING", "QTY_SHIPPED", "QTY_STOCK", "OPEN_PURCHORDER", "MSG_SNT_DATE", "MSG_SNT_TIME", "STORE_ORDER_QTY_PRM", "QTY_STOCK_PRM", "OPEN_PURCHORDER_PRM", "QTY_SHIPPED_PRM", "OPEN_ORDER_QTY", "OPEN_ORDER_QTY_PRM", "OPEN_ORDER_N1_QTY", "OPEN_ORDER_N1_QTY_PRM", "QTY_MISSING_PRM"],
                                                    transformed_or_new_cols_dict=None,
                                                    distinct_flag=False,
                                                    list_of_keys_to_fetch_distinct_rows=None
                                                )

    join_cond = ( 
                        ( F.col("log.RetailWarehouseGlobalLocationNumber").cast(LongType()) == F.col("brnz.WH_GLN").cast(LongType()) )  &
                        ( F.col("log.SupplierGlobalLocationNumber").cast(LongType()) == F.col("brnz.SUPPLIER_GLN").cast(LongType()) ) &
                        ( F.to_date(F.col('log.BusinessDate'),'yyyyMMdd').cast('date')  == F.to_date(F.col('brnz.INV_DATE'),'yyyyMMdd').cast('date') )  &
                        ( F.col("log.MaterialGlobalTradeItemNumber").cast(StringType()) == F.col("brnz.MATERIAL_EAN").cast(StringType()) )                                  
                )

    silver_log_bronze_joined_df = log_output_df.alias("log").join(bronze_columns_for_log_data.alias("brnz"), join_cond ,"inner").select(
                                                                                                                                        "brnz.*",
                                                                                                                                        F.to_date(F.current_date(), 'ddMMyyyy').cast(DateType()).alias('EXEC_DATE'),
                                                                                                                                        "log.Comment"
                                                                                                                                        )
    
    ########################################### Logging Data: This involves capturing EDI messages where EAN-to-material mappings are unavailable, and quantity columns contain zeros or negative values, adhering to business filter constraints.
    log_output_single_file_df = silver_log_bronze_joined_df.coalesce(1)
    write_params_adls.update({"adls_location":log_output_adls2_path})  #This line of code is responsible for updating the parameters dictionary with the new ADLS location specified by interface_output_adls2_path
    write_df(log_output_single_file_df, write_params_adls,header=True)

    if not log_output_single_file_df.isEmpty():
        write_params_adls.update({"log_write_status":"Success Written the Log data to VMI adls2"})
    else:
        write_params_adls.update({"log_write_status":"No records have been identified with negative quantity or lacking EAN to material mapping"})

    return write_params_adls
