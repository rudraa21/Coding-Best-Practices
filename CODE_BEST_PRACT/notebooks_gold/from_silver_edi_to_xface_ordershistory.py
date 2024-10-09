# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### **🏭📦🚚🏢📦🛒 - VMI Inventory Interface - Silver layer EDI msg to gold/xface/Blue Yonder Layer**
# MAGIC
# MAGIC ##### Overview
# MAGIC > ##### As part of the VMI program, order history report needs to be sent to Planning systems via Xface and Blu Yonder on an agreed frequency. This notebook collects the shipped quantity details from parsed EDI messages in the Silver layer and applies transformations/mappings as per business requirements to send inventory details to the Xface/Downstream Blu Yonder systems.
# MAGIC
# MAGIC ##### Key Features
# MAGIC - **🔄 Transformation:** Applies necessary transformations to EDI messages.
# MAGIC - **📈 Mapping:** Maps data as per business requirements.
# MAGIC - **🚀 Integration:** Writing the data to VMI ADLS2,from where we are Integrating with Xface and Blu Yonder for downstream delivery via ADF.
# MAGIC
# MAGIC ##### Frequency of Sending Details to the Downstream Blu Yonder:
# MAGIC - **Aggregation/Bucketing:** Aggregation is happening in this notebook. EDI messages, after applying the business date filter, we are rolling up the data which will become the benchmark data for delivery to downstream.
# MAGIC - **When:** Once a Week , Monday
# MAGIC
# MAGIC ##### 📅 History
# MAGIC | Date       | Author  | Workflow | Description                |
# MAGIC |------------|---------|-----------|-----------------|
# MAGIC | 2024-04-29 | Robin |    OrdersHistory         |Initial Development       |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🔄 High Level Data Transformations
# MAGIC
# MAGIC 1. **Applying the Business date filter as per business**
# MAGIC       - **For Carrefour Retail:** 🛒
# MAGIC            - Apply filter for previous week from Monday to Sunday.
# MAGIC
# MAGIC       - **For Delhaize Retailer:** 🛒
# MAGIC           - Apply filter for previous week from Monday to Sunday.
# MAGIC
# MAGIC 2.  **Applying the sum agg on grouping column as per business need**
# MAGIC       - Apply agg on previous week edi data and get the sum of order qty for a respective EAN per Retailer. 
# MAGIC
# MAGIC 2. **VMI Master Filter Mapping**
# MAGIC       - Based on WHGL and SupplierGLN, pull JDA-related information from config file. 🗄️
# MAGIC
# MAGIC 3. **EAN to Material Mapping 📊**
# MAGIC       - Retrieve the Material ID corresponding to each EAN in the EDI message by querying the orderline table and customer table.
# MAGIC
# MAGIC 4. **Filtering the EDI Message into Two Streams**
# MAGIC    > ##### EDI messages with Proper EAN to Material Mapping  (Goes to ADLS) 📤
# MAGIC       - Route EDI messages meeting criteria to ADLS.
# MAGIC
# MAGIC    > ##### EDI Messages with  No EAN to Material Mapping (Sent to Log) 📝
# MAGIC       - Route EDI messages with lacking EAN to material mapping to log.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##### 📚 Importing Required Libraries
# MAGIC
# MAGIC This section focuses on importing the necessary libraries to facilitate data processing and analysis within the Databricks notebook.

# COMMAND ----------

import logging

# Set up logging configuration
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.info(f"Setting the default logging level to INFO")


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
import sys,os
import subprocess
from typing import Dict,List,Optional,Any
import logging

# COMMAND ----------



# Input 1: retailer name 
# Description:The widget 'retailer_name' is either DELHAIZE/CARREFOUR
dbutils.widgets.text(name="retailer_name", defaultValue="", label="retailer_name")
# Initialize configuration variables
retailer_name = dbutils.widgets.get("retailer_name")

# Input 3: environment 
dbutils.widgets.text(name="environment", defaultValue="", label="environment")
environment = dbutils.widgets.get("environment") 

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC #### ⚙️ Run Utilities for VMI  Gold
# MAGIC
# MAGIC This section involves executing utilities specifically designed for VMI Gold, streamlining operations and enhancing the functionality of the Databricks notebook.

# COMMAND ----------

# MAGIC %run ./utilities/common_utilities

# COMMAND ----------

# MAGIC %run ./utilities/application_specific_utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 🧩 Defining Custom Functions

# COMMAND ----------

def aggregate_data(silver_edi_df,retailer):

    silver_edi_df = silver_edi_df.withColumn("RetailWarehouseToStoreShipmentRegularQuantity",F.ceil("RetailWarehouseToStoreShipmentRegularQuantity")).withColumn("RetailStoreToWarehouseOrderTodayRegularQuantity",F.ceil("RetailStoreToWarehouseOrderTodayRegularQuantity"))

    groupby_cols=["MaterialGlobalTradeItemNumber","RetailWarehouseGlobalLocationNumber","SupplierGlobalLocationNumber","MaterialId"]
    if retailer.lower() == "delhaize":
        agg_col=["RetailStoreToWarehouseOrderTodayRegularQuantity"]
    elif retailer.lower() == "carrefour":
        agg_col=["RetailWarehouseToStoreShipmentRegularQuantity"]
    
    alias = "total_open_order_qty"

    grouped_df=get_aggregate_data(silver_edi_df,groupby_cols,agg_col,alias)
     

    return grouped_df

# COMMAND ----------

def retailer_silver_to_gold_mapping() -> list:
    """
    Maps retailer silver data to gold data by applying transformations and calculations.

    Returns:
        list: A list of required columns for mapping.
    """
    today = datetime.now() - timedelta(days=1)
    start_of_week = today - timedelta(days=(today.weekday()) % 7)
    start_of_week_date = start_of_week.date()

    required_columns_list = [
                                    F.col("jda_mapping.SOURCE_SYSTEM_COUNTRY").alias("SOURCE_SYSTEM_COUNTRY"),
                                    F.col("jda_mapping.SOURCE_SYSTEM").alias("SOURCE_SYSTEM"),
                                    F.col("jda_mapping.JDA_CODE").alias("JDA_CODE"),
                                    F.col("jda_mapping.DMDGROUP").alias("DMDGROUP"),
                                    F.col("edi_msg.MaterialId").cast('long').alias("DMDUNIT"),
                                    F.col("edi_msg.RetailWarehouseGlobalLocationNumber").alias("LOC"),
                                    F.coalesce(F.col("edi_msg.total_open_order_qty"), F.lit(0)).alias("SOURCE_QTY"),
                                    F.col("jda_mapping.SOURCE_UOM").alias("SOURCE_UOM"),
                                    F.lit("L").alias("STATUS_STAGING"),
                                    F.current_timestamp().alias("LOAD_TIMESTAMP"),
                                    F.lit(start_of_week_date).alias("STARTDATE"),
                                    
                                    #F.when( F.col("edi_msg.XTNUniversalProductCode").isNull(), True).otherwise(False).alias("IsEANMaterialMappingNotAvailable")
                               ]

    return required_columns_list


# COMMAND ----------

def main(
                spark: SparkSession,
                retailer_table_config: Dict,
                interface_name: str,
                retailer_name: str,
                config_file_path: str,
                config_file_storage_type: str,
                environment: str,
                log_output_adls2_path: str,
                interface_output_adls2_path: str,
                required_columns_for_interface : list,
                bronze_table_details :Dict
            ) -> None:
    """
    Main function to orchestrate the ETL process.

    Args:
        spark: SparkSession object.
        retailer_table_config: Configuration dictionary for retailer table.
        interface_name: Name of the interface.
        retailer_name: Name of the retailer.
        config_file_path: Path to the configuration file.
        config_file_storage_type: Storage type of the configuration file.
        environment: Environment identifier.
        log_output_adls2_path: ADLS2 path for log output.
        interface_output_adls2_path: ADLS2 path for interface output.

    Returns:
        None
    """
    # Extract EDI messages from silver
    silver_edi_msg_with_business_filtered_df = extract_edi_msg_from_silver(
                                                                            spark=spark,
                                                                            retailer_table_config=retailer_table_config,
                                                                            interface_name=interface_name,
                                                                            retailer_name=retailer_name
                                                                            )
    
    # Perform EAN to material mapping and Final Selection of columns
    edi_to_jda_df = ean_to_material_mapping(
                                                    edi_jda_join_df=silver_edi_msg_with_business_filtered_df,
                                                    ean_material_mapping_df=ean_material_mapping_df
                                            )

    # Filter DataFrame to select records where 'Comment' is  null, indicating EAN to material mapping is unavailable
    ean_to_material_unavailable_df = edi_to_jda_df.filter(F.col("ean_mat_map.XTNUniversalProductCode").isNull()).withColumn("Comment", F.lit('Missing join with order item or customer'))

    # Filter DataFrame to select records where 'XTNUniversalProductCode' is not null, indicating EAN to material mapping is available.
    ean_to_material_available_df = edi_to_jda_df.filter(F.col("ean_mat_map.XTNUniversalProductCode").isNotNull())

    ean_to_material_available_aggregated_df = aggregate_data(
                                                             silver_edi_df = ean_to_material_available_df,retailer=retailer_name
                                                            )


    # Map EDI to JDA data from Config File
    edi_constant_values_jda_join_df = mapping_edi_to_vmi_jda_mapping_file(
                                                                            spark=spark,
                                                                            retailer_silver_df=ean_to_material_available_aggregated_df,
                                                                            retailer_table_config=retailer_table_config,
                                                                            config_file_path=config_file_path,
                                                                            config_file_storage_type=config_file_storage_type,
                                                                            env=environment
                                                                            )

    mapping_silver_to_xface_schema_df = edi_constant_values_jda_join_df.select( required_columns_for_interface )


    # Write EDI to JDA data
    write_status_dict  = write_edi_to_adls2(
                                            interface_output_df=mapping_silver_to_xface_schema_df,
                                            log_output_df=ean_to_material_unavailable_df,
                                            log_output_adls2_path=log_output_adls2_path,
                                            interface_output_adls2_path=interface_output_adls2_path,
                                            write_params_adls = retailer_table_config["write_params_adls"],
                                            interface=interface_name,
                                            retailer_name=retailer_name,
                                            bronze_table_details =bronze_table_details
                                            )


    
   
    return edi_constant_values_jda_join_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Integration of the EAN to Material Mapping Notebook
# MAGIC
# MAGIC > #####  This integration 🤝 facilitates the mapping of 🛒 EANs from the EDI message to their corresponding 📦 Material IDs. It is essential as the downstream Xface/Blue Yonder system generates orders based on Material IDs.

# COMMAND ----------

# MAGIC %run ./Vmi_EAN_to_Material_Mapping_Nb $env = $environment $retailer_name = $retailer_name

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 🚀 Main Execution
# MAGIC
# MAGIC  > ##### This section contains the main execution logic of the code, which is triggered when the script is run as the main program.

# COMMAND ----------

if __name__ == "__main__":

    try:
        # Input 1: Configuration File Path
        # Description: Path to the configuration file containing setup details.
        dbutils.widgets.text(name="config_file_path", defaultValue="", label="config_file_path")

        # Input 2: Config json file Storage type
        # Description: The user input indicates the file's location, which could be either in ADLS2, DBFS, or locally.
        dbutils.widgets.text(name="config_file_storage_type", defaultValue="", label="config_file_storage_type")
        
        # Input 3: environment 
        dbutils.widgets.text(name="environment", defaultValue="", label="environment")

        # Initialize configuration variables
        config_file_path = dbutils.widgets.get("config_file_path")
        config_file_storage_type = dbutils.widgets.get("config_file_storage_type") 
        environment = dbutils.widgets.get("environment")
        
        interface_name = "ordershistory"

        retailer_table_config =  {
                                                "table_name": "xtnretailwarehouseinventorydistributiondaysummary",
                                                "database_name": "fdn_sales_internal",                           
                                                "required_columns" :  ["MaterialGlobalTradeItemNumber","RetailWarehouseGlobalLocationNumber","SupplierGlobalLocationNumber","RetailStoreToWarehouseOrderTodayRegularQuantity","RetailWarehouseToStoreShipmentRegularQuantity","BusinessDate"],
                                                "primary_key_columns": [],
                                                "system_id": { "col_name":"XTNDFSystemId" , "value_id":101300 },
                                                "reporting_id":{ "col_name":"XTNDFReportingUnitId" , "value_id":200152 },
                                                "filter_template" : [
                                                                                {"col_name": "XTNDFSystemId", "col_val": f"'{101300}'", "operator": '=='},

                                                                                {"col_name": "XTNDFReportingUnitId", "col_val": f"'{200152}'", "operator": '=='}        ,   
                                                                                {"col_name": "RetailerCustomerName", "col_val": f"'{retailer_name.upper()}'", "operator": '=='}                                                               
                                                                    ],
                                                
                                                "watermark_column" : {      
                                                                        "ordershistory":"BusinessDate"
                                                                     },
                                                "write_params_adls" : {
                                                                        'adls_location': '',
                                                                        'write_mode': 'overwrite', 
                                                                        'format': 'csv' 
                                                                    },
                                                 "bronze_details" : { 
                                                                    "DELHAIZE":  {  
                                                                                    'table_name': 'tbl_snt_vmi_bedelhaize' , 
                                                                                    'database_name': "fdn_sales_internal_bronze", 
                                                                                    'filter_template': [
                                                                                                        {"col_name": "XTNDFSystemId", "col_val": f"'{101300}'", "operator": '=='},
                                                                                                        {"col_name": "XTNDFReportingUnitId", "col_val": f"'{200152}'", "operator": '=='},
                                                                                                        {"col_name": "SUPPLIER_GLN", "col_val": f"{5410048000019}", "operator": '=='} ######Filtering only foods        
                                                                                                      ] 
                                                                                    },
                                                                    "CARREFOUR":  {  
                                                                                    'table_name': 'tbl_snt_vmi_becarrefour' , 
                                                                                    'database_name': "fdn_sales_internal_bronze" ,
                                                                                    'filter_template': [
                                                                                                        {"col_name": "XTNDFSystemId", "col_val": f"'{101300}'", "operator": '=='},
                                                                                                        {"col_name": "XTNDFReportingUnitId", "col_val": f"'{200152}'", "operator": '=='},
                                                                                                        {"col_name": "SUPPLIER_GLN", "col_val": f"{5410048000019}", "operator": '=='} ######Filtering only foods        
                                                                                                      ]
                                                                                  }

                                                                  }

                                                }    
    
        etl_start_time=datetime.now()

        ##### Initializing ETL Runtime Paramters & Metrics variable
        # To store the required parameters passed by the caller ADF
        user_input_params_dict = retailer_table_config

        # To store runtime metrics (measurable outputs of the pipeline), such as the count of records loaded, time taken to run, records skipped, etc.
        runtime_metrics_dict = {}

        # To store runtime parameters required to execute the ETL pipeline. These parameters are retrieved from the Metatable control table.
        runtime_pipeline_parameters_dict = {}

        # To store all the above ETL pipeline-related information in one dictionary to send out as a response from DBX to any external service like ADF or Log Analytics.
        etl_pipeline_response_dict = {}

        # Getting notebook path 
        notebook_nm = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

        # Log input parameters
        user_input_params_dict.update(
                                        {
                                            "config_file_path":config_file_path,
                                            "config_file_storage_type":config_file_storage_type,
                                            "environment":environment,
                                            "retailer_name":retailer_name,
                                            "interface_name":interface_name
                                        }
                                      )

        # Updating pipeline parameters in the variable
        runtime_pipeline_parameters_dict.update(
                                                {
                                                    "notebook_path":notebook_nm,
                                                    "user_input_from_ADF":user_input_params_dict
                                                }
                                              )

        etl_pipeline_response_dict.update(  
                                        {
                                            "etl_start_time":str(etl_start_time),
                                            "runtime_pipeline_parameters":runtime_pipeline_parameters_dict,
                                            "runtime_metrics":runtime_metrics_dict
                                        }
                                        )
        
        adls2_env = environment.lower()
        today_date = datetime.now().strftime("%Y_%m_%d")
        interface_output_adls2_path = f"abfss://vmi@europemdip{adls2_env}adls2.dfs.core.windows.net/gold/SendXface/{retailer_name.upper()}/ST_IN_HIST/{today_date}/"
        log_output_adls2_path = f"abfss://vmi@europemdip{adls2_env}adls2.dfs.core.windows.net/gold/SendXface/{retailer_name.upper()}/ST_IN_HIST/LOG/{today_date}/"

        write_status_dict = main(
                                    spark=spark,
                                    retailer_table_config=retailer_table_config,
                                    interface_name=interface_name,
                                    retailer_name=retailer_name,
                                    config_file_path=config_file_path,
                                    config_file_storage_type=config_file_storage_type,
                                    environment=environment,
                                    log_output_adls2_path=log_output_adls2_path,
                                    interface_output_adls2_path=interface_output_adls2_path,
                                    required_columns_for_interface=retailer_silver_to_gold_mapping(),
                                    bronze_table_details = retailer_table_config["bronze_details"][retailer_name.upper()]
                               )

        #### Here, we are capturing the file name of the interface and log outputs to push the output to the ADF. This ensures that subsequent activities in ADF will pick up the files to be written to the interface.
        interface_file_list=dbutils.fs.ls(interface_output_adls2_path)
        interface_file_name_to_adf=[file.name for file in interface_file_list if 'csv' in file.name][0]

        log_file_list=dbutils.fs.ls(log_output_adls2_path)
        log_file_name_to_adf=[file.name for file in log_file_list if 'csv' in file.name][0]

        etl_pipeline_response_dict.update( 
                                          {"interface_file_name":interface_file_name_to_adf,"log_file_name":log_file_name_to_adf}
                                         )


        # #### 💼 Exporting Results and Notebook Termination
        # #### This code snippet orchestrates the transformation of dictionaries into JSON strings, helping in the graceful termination of the notebook.
        update_processing_time_and_exit(dbutils,etl_pipeline_response_dict, etl_start_time)

    except Exception as e:
        notebook_exit_response(dbutils,notebook_response=etl_pipeline_response_dict,exception_info={"exception_message":str(e),"status":"failed",'etl_end_time':str(datetime.now())})
