# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### **🏭📦🚚🏢📦🛒 - VMI Inventory Interface - Silver layer EDI msg to gold/xface/Blue Yonder Layer** 
# MAGIC
# MAGIC ##### Overview
# MAGIC > ##### As part of the VMI program, an inventory report needs to be sent to Planning systems via Xface and Blu Yonder on an agreed frequency. This notebook collects the inventory details from parsed EDI messages in the Silver layer and applies transformations/mappings as per business requirements to send inventory details to the Xface/Downstream Blu Yonder systems.
# MAGIC
# MAGIC ##### Key Features
# MAGIC - **🔄 Transformation:** Applies necessary transformations to EDI messages.
# MAGIC - **📈 Mapping:** Maps data as per business requirements.
# MAGIC - **🚀 Integration:** Writing the data to VMI ADLS2,from where we are Integrating with Xface and Blu Yonder for downstream delivery via ADF.
# MAGIC
# MAGIC ##### Frequency of Sending Details to the Downstream Blu Yonder:
# MAGIC - **Aggregation/Bucketing:** No aggregation is happening in this notebook. EDI messages, after applying the business date filter, will become the benchmark data for delivery to downstream.
# MAGIC - **When:** Daily, Monday to Friday
# MAGIC
# MAGIC ##### 📅 History
# MAGIC | Date       | Author  | Description                |
# MAGIC |------------|---------|----------------------------|
# MAGIC | 2024-04-21 | Chandra | Initial Development       | 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🔄 High Level Data Transformations
# MAGIC
# MAGIC 1. **Applying the Inventory date filter as per business**
# MAGIC       - **For Carrefour Retail:** 🛒
# MAGIC            - Apply filter for yesterday's date.
# MAGIC
# MAGIC       - **For Delhaize Retailer:** 🛒
# MAGIC           - Apply filter for today's date.
# MAGIC
# MAGIC 2. **VMI Master Filter Mapping**
# MAGIC       - Based on WHGL and SupplierGLN, pull JDA-related information from config file. 🗄️
# MAGIC
# MAGIC 3. **EAN to Material Mapping 📊**
# MAGIC       - Retrieve the Material ID corresponding to each EAN in the EDI message by querying the orderline table and customer table.
# MAGIC
# MAGIC 4. **Filtering the EDI Message into Two Streams**
# MAGIC    > ##### EDI messages with Proper EAN to Material Mapping and Non-negative Total Source Qty per EAN (Goes to Xface) 📤
# MAGIC       - Route EDI messages meeting criteria to Xface.
# MAGIC
# MAGIC    > ##### EDI Messages with Negative Values or No EAN to Material Mapping (Sent to Log) 📝
# MAGIC       - Route EDI messages with negative values or lacking EAN to material mapping to log.

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

# Input 1: retailer name 
# Description:The widget 'retailer_name' is either DELHAIZE/CARREFOUR
# 
dbutils.widgets.text(name="retailer_name", defaultValue="", label="retailer_name")

retailer_name = dbutils.widgets.get("retailer_name") 

# Input 3: environment 
dbutils.widgets.text(name="environment", defaultValue="", label="environment")
environment = dbutils.widgets.get("environment") 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 🧩 Defining Custom Functions

# COMMAND ----------

def get_material_description(
                               spark: SparkSession,
                               edi_silver_material_id_df,
                                    material_table_config
                                 ) -> DataFrame:
    """
    Extracts the material description from the specified table using the provided configuration.

    Args:
        spark (SparkSession): The Spark session to use for data extraction.
        material_table_config (dict): Configuration dictionary containing:
            - database_name (str): Name of the database.
            - table_name (str): Name of the table.
            - filter_template (str): Filter condition to apply.
            - required_columns (list): List of columns to select.

    Returns:
        DataFrame: DataFrame containing the extracted material description.
    """

    material_df  = get_dataframe(
                                        spark=spark,
                                        database_name=material_table_config["database_name"],
                                        table_name=material_table_config["table_name"],
                                        filter_template=material_table_config["filter_template"],
                                        list_of_columns_to_select=material_table_config["required_columns"],
                                        transformed_or_new_cols_dict=None,
                                        distinct_flag=False,
                                        list_of_keys_to_fetch_distinct_rows=None
                                       )
    
    joining_condition = F.concat( F.col("edi.MaterialId").cast(LongType()), F.lit("_"), F.col("edi.JDA_CODE") ) == F.col("mat.MaterialId")

    edi_material_desc_df = edi_silver_material_id_df.alias("edi").join(
                                                                        material_df.alias("mat"),
                                                                        on=joining_condition,
                                                                        how="inner"
                                                                     )
    edi_material_desc_df = edi_material_desc_df.select(   
                                                          F.date_format("edi.BusinessDate", "yyyyMMdd").alias("MSG_GEN_DATE"),
                                                          F.col("edi.JDA_LOC").alias("VMI_LOC"),
                                                          F.col("mat.MaterialId").alias("ITEM"),  
                                                          F.col("mat.MaterialName").alias("ITEM_DESCR"),
                                                          F.col("edi.RetailWarehouseToStoreStockOutRegularQuantity").alias("QTY_MISSING"),
                                                          F.col("edi.SOURCE_UOM").alias("UOM")

                                                       ).filter(F.col("QTY_MISSING") > 0).coalesce(1) 
    return edi_material_desc_df


# COMMAND ----------

def calculate_and_classify_source_qty(df: DataFrame, retailer_name: str) -> DataFrame:
    """
    Calculate source quantity and add comments based on the retailer name.

    Args:
        df (DataFrame): Input DataFrame containing necessary columns.
        retailer_name (str): Name of the retailer, either 'DELHAIZE' or 'carrefour'.

    Returns:
        DataFrame: DataFrame with added columns 'SOURCE_QTY' and 'comment'.

    Raises:
        ValueError: If retailer name is not recognized.
    """ 
    # Calculate source inventory quantity based on retailer name
    # if retailer_name.upper() == 'DELHAIZE':
    #     source_inventory_qty = (
    #                             F.col("edi_msg.RetailStoreToWarehouseOrderTodayRegularQuantity") +
    #                             F.col("edi_msg.RetailWarehouseOnHandInventoryRegularQuantity") +
    #                             F.col("edi_msg.RetailStoreToWarehouseOrderNextDaysRegularQuantity") -
    #                             F.col("edi_msg.RetailWarehouseToStoreStockOutRegularQuantity")
    #                            )
    # elif retailer_name.upper() == "CARREFOUR":
    #     source_inventory_qty = (
    #                             F.col("edi_msg.RetailWarehouseOnHandInventoryRegularQuantity") +
    #                             F.col("edi_msg.RetailStoreToWarehouseTotalOrdersRegularQuantity") -
    #                             F.col("edi_msg.RetailWarehouseToStoreStockOutRegularQuantity")
    #                            )
    # else:
    #     raise ValueError("Retailer name not recognized. Should be either CARREFOUR or DELHAIZE")

    if retailer_name.upper() == 'DELHAIZE':
        source_inventory_qty = (
                                F.ceil("edi_msg.RetailStoreToWarehouseOrderTodayRegularQuantity") +
                                F.ceil("edi_msg.RetailWarehouseOnHandInventoryRegularQuantity") +
                                F.ceil("edi_msg.RetailStoreToWarehouseOrderNextDaysRegularQuantity") -
                                F.ceil("edi_msg.RetailWarehouseToStoreStockOutRegularQuantity")
                               )
    elif retailer_name.upper() == "CARREFOUR":
        source_inventory_qty = (
                                F.ceil("edi_msg.RetailWarehouseOnHandInventoryRegularQuantity") +
                                F.ceil("edi_msg.RetailStoreToWarehouseTotalOrdersRegularQuantity") -
                                F.ceil("edi_msg.RetailWarehouseToStoreStockOutRegularQuantity")
                               )
    else:
        raise ValueError("Retailer name not recognized. Should be either CARREFOUR or DELHAIZE")

    # Add 'SOURCE_QTY' column to DataFrame
    df = df.withColumn("SOURCE_QTY", source_inventory_qty)

    # Add 'comment' column based on conditions
    df = df.withColumn(
                         "comment",
                                    F.when(
                                        (F.col("edi_msg.XTNUniversalProductCode").isNull()) & (F.col("SOURCE_QTY") < 0),
                                        F.concat(
                                            F.lit("Missing Join with Order Item or Customer"),
                                            F.lit(" || "),
                                            F.lit("SOURCE_QTY Is Negative ")
                                        )
                                    ).when(
                                        F.col("edi_msg.XTNUniversalProductCode").isNull(),
                                        F.lit("Missing Join with Order Item or Customer")
                                    ).when(
                                        F.col("SOURCE_QTY") < 0,
                                        F.lit("SOURCE_QTY Is Negative")
                                    ).otherwise(
                                        F.lit(None).cast(StringType())
                                          )
                     )

    return df

# COMMAND ----------

def retailer_silver_to_gold_mapping() -> list:
    """
    Maps retailer silver data to gold data by applying transformations and calculations.

    Returns:
        list: A list of required columns for mapping.
    """
    required_columns_list = [
                                F.col("jda_mapping.SOURCE_SYSTEM_COUNTRY").alias("SOURCE_SYSTEM_COUNTRY"),
                                F.col("jda_mapping.SOURCE_SYSTEM").alias("SOURCE_SYSTEM"),
                                F.col("jda_mapping.JDA_CODE").alias("JDA_CODE"),
                                F.col("edi_msg.MaterialId").cast(LongType()).alias("ITEM"),
                                F.col("edi_msg.RetailWarehouseGlobalLocationNumber").alias("LOC"),
                                F.to_date(F.col('edi_msg.XTNUpdatedTime'), 'ddMMyyyy').cast(DateType()).alias('AVAILDATE'),
                                F.lit(None).cast(StringType()).alias("EXPDATE"),
                                F.col("SOURCE_QTY").alias("SOURCE_QTY"),
                                F.col("jda_mapping.SOURCE_UOM").alias("SOURCE_UOM"),
                                F.lit("N").alias("QUARANTINE"),
                                F.lit("L").alias("STATUS_STAGING"),
                                F.current_timestamp().alias("LOAD_TIMESTAMP"),

                                # F.col("edi_msg.RetailWarehouseGlobalLocationNumber").alias("RetailWarehouseGlobalLocationNumber"),
                                # F.col("edi_msg.SupplierGlobalLocationNumber").alias("SupplierGlobalLocationNumber"),
                                # F.col("edi_msg.BusinessDate").alias("BusinessDate"),
                                # F.col("edi_msg.MaterialGlobalTradeItemNumber").alias("MaterialGlobalTradeItemNumber")

                                # F.col("ean_mat_map.XTNUniversalProductCode").alias("EAN_OrderLine"),
                                #F.col("edi_jda_map.MaterialGlobalTradeItemNumber").alias("EAN_EDI"),

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
                required_columns_for_interface :list,
                bronze_table_details :Dict,
                dim_material_details :Dict
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
    # Step 1: Extract EDI messages from the silver layer based on the provided configurations
    silver_edi_msg_with_business_filtered_df = extract_edi_msg_from_silver(
                                                                            spark=spark,
                                                                            retailer_table_config=retailer_table_config,
                                                                            interface_name=interface_name,
                                                                            retailer_name=retailer_name
                                                                            )
    
    

    # Step 2: Perform EAN to material mapping using the extracted EDI messages
    ean_materail_mapping_df = ean_to_material_mapping(
                                                      edi_jda_join_df=silver_edi_msg_with_business_filtered_df,
                                                      ean_material_mapping_df=ean_material_mapping_df
                                                     )
    
    # Step 3: Map EDI messages to JDA static data using the configuration file
    edi_msg_constant_values_jda_join_df = mapping_edi_to_vmi_jda_mapping_file(
                                                                                spark=spark,
                                                                                retailer_silver_df=ean_materail_mapping_df,
                                                                                retailer_table_config=retailer_table_config,
                                                                                config_file_path=config_file_path,
                                                                                config_file_storage_type=config_file_storage_type,
                                                                                env=environment
                                                                            )

    # Step 4: Calculate the source quantity and classify records based on EAN to material mapping and inventory source quantity constraints 
    source_qty_df = calculate_and_classify_source_qty(df = edi_msg_constant_values_jda_join_df , retailer_name = retailer_name)

    # Step 5: Filter the DataFrame to select records where 'Comment' is null, indicating successful EAN to material mapping
    ean_to_material_available_and_qty_positive_df = source_qty_df.filter(F.col("Comment").isNull())

    # Step 6: Filter the DataFrame to select records where 'Comment' is not null, indicating unsuccessful EAN to material mapping
    ean_to_material_unavailable_and_qty_negative_df= source_qty_df.filter(F.col("Comment").isNotNull())
    
    #Step 7: Perform column mapping from the silver schema to the Xface schema, selecting the required columns for the interface
    mapping_silver_to_xface_schema_df = ean_to_material_available_and_qty_positive_df.select(required_columns_for_interface)

    #Step 8: Retrieve material dimension data based on the provided material details configuration.
    edi_customer_missing_qty_df = get_material_description(
                                                            spark=spark,
                                                            edi_silver_material_id_df=edi_msg_constant_values_jda_join_df,
                                                            material_table_config=dim_material_details
                                                            )
    
    # Step 9: Write the DataFrame with material descriptions to VMI ADLS2, which is then sent to the business via a logic app. This is to track the missed order qty's from customer store to their DC's 
    write_df(edi_customer_missing_qty_df,dim_material_details["write_params_adls"],header=True)

    #Step 10: Write the EDI messages to VMI ADLS2, logging unavailable mappings and writing the interface output to the specified paths
    write_status_dict  = write_edi_to_adls2(
                                            interface_output_df=mapping_silver_to_xface_schema_df,
                                            log_output_df=ean_to_material_unavailable_and_qty_negative_df,
                                            log_output_adls2_path=log_output_adls2_path,
                                            interface_output_adls2_path=interface_output_adls2_path,
                                            write_params_adls = retailer_table_config["write_params_adls"],
                                            interface=interface_name,
                                            retailer_name=retailer_name,
                                            bronze_table_details =bronze_table_details
                                            )
                                            

    return write_status_dict

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

        # Input 4: retailer name 
        # Description:The widget 'retailer_name' is either DELHAIZE/CARREFOUR
        dbutils.widgets.text(name="retailer_name", defaultValue="", label="retailer_name")

        # Initialize configuration variables
        config_file_path = dbutils.widgets.get("config_file_path")
        config_file_storage_type = dbutils.widgets.get("config_file_storage_type")
        environment = dbutils.widgets.get("environment")
        retailer_name = dbutils.widgets.get("retailer_name") 
        
        
        interface_name = "inventory"
        adls2_env = environment.lower()
        today_date = datetime.now().strftime("%Y_%m_%d")

        retailer_table_config =  {
                                                "table_name": "xtnretailwarehouseinventorydistributiondaysummary",
                                                "database_name": "fdn_sales_internal",                           
                                                "required_columns" :  [],
                                                "primary_key_columns": [],
                                                "system_id": { "col_name":"XTNDFSystemId" , "value_id":101300 },
                                                "reporting_id":{ "col_name":"XTNDFReportingUnitId" , "value_id":200152 },
                                                "filter_template" : [
                                                                                {"col_name": "XTNDFSystemId", "col_val": f"'{101300}'", "operator": '=='},

                                                                                {"col_name": "XTNDFReportingUnitId", "col_val": f"'{200152}'", "operator": '=='}        ,   
                                                                                {"col_name": "RetailerCustomerName", "col_val": f"'{retailer_name.upper()}'", "operator": '=='}                                                               
                                                                    ],
                                                
                                                "watermark_column" : {      
                                                                        "openorders":"BusinessDate",
                                                                        "inventory":"BusinessDate"
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

                                                                  },
                                                
                                                "xcf_dim_details" : { 
                                                                        'table_name': 'XCFMaterialCore' , 
                                                                         'database_name': 'fdn_masterdata_internal' if env.lower()=='prod' else 'fdn_masterdata_internal_p2d', 
                                                                         "required_columns" :  [],
                                                                        'filter_template': [
                                                                                                        {"col_name": "XTNDFSystemId", "col_val": f"'{1564}'", "operator": '=='}
                                                                                                        # {"col_name": "systemid", "col_val": f"'{8}'", "operator": '=='},
                                                                                                        # {"col_name": "instanceid", "col_val": f"{8}", "operator": '=='}     
                                                                                            ],
                                                                        "write_params_adls" : {
                                                                                                'adls_location': f"abfss://vmi@europemdip{adls2_env}adls2.dfs.core.windows.net/gold/vmi_inventory_customer_missing_qty/{retailer_name.upper()}/{today_date}/",
                                                                                                'write_mode': 'overwrite', 
                                                                                                'format': 'csv'
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
    
        interface_output_adls2_path = f"abfss://vmi@europemdip{adls2_env}adls2.dfs.core.windows.net/gold/SendXface/{retailer_name.upper()}/ST_IN_INVENTORY/{today_date}/"
        log_output_adls2_path = f"abfss://vmi@europemdip{adls2_env}adls2.dfs.core.windows.net/gold/SendXface/{retailer_name.upper()}/ST_IN_INVENTORY/LOG/{today_date}/"
        edi_customer_missing_qty_msg_path = retailer_table_config["xcf_dim_details"]["write_params_adls"]["adls_location"]

        # write_status_dict = main(
        #                             spark=spark,
        #                             retailer_table_config=retailer_table_config,
        #                             interface_name=interface_name,
        #                             retailer_name=retailer_name,
        #                             config_file_path=config_file_path,
        #                             config_file_storage_type=config_file_storage_type,
        #                             environment=environment,
        #                             log_output_adls2_path=log_output_adls2_path,
        #                             interface_output_adls2_path=interface_output_adls2_path,
        #                             required_columns_for_interface=retailer_silver_to_gold_mapping(),
        #                             bronze_table_details = retailer_table_config["bronze_details"][retailer_name.upper()],
        #                             dim_material_details=retailer_table_config["xcf_dim_details"]
        #                        )

        # #### Here, we are capturing the file name of the interface and log outputs to push the output to the ADF. This ensures that subsequent activities in ADF will pick up the files to be written to the interface.
        # interface_file_list=dbutils.fs.ls(interface_output_adls2_path)
        # interface_file_name_to_adf=[file.name for file in interface_file_list if 'csv' in file.name][0]

        # log_file_list=dbutils.fs.ls(log_output_adls2_path)
        # log_file_name_to_adf=[file.name for file in log_file_list if 'csv' in file.name][0]    

        # edi_customer_missing_qty_msg_file_list =dbutils.fs.ls(edi_customer_missing_qty_msg_path)
        # edi_customer_missing_qty_msg_to_adf =[file.name for file in edi_customer_missing_qty_msg_file_list if 'csv' in file.name][0]

        # etl_pipeline_response_dict.update( 
        #                                   {"interface_file_name":interface_file_name_to_adf,"log_file_name":log_file_name_to_adf,"customer_missing_qty_msg_to_adf":edi_customer_missing_qty_msg_to_adf}
        #                                  )


        # #### 💼 Exporting Results and Notebook Termination
        # #### This code snippet orchestrates the transformation of dictionaries into JSON strings, helping in the graceful termination of the notebook.
        # update_processing_time_and_exit(dbutils,etl_pipeline_response_dict, etl_start_time)

    except Exception as e:
        notebook_exit_response(dbutils,notebook_response=etl_pipeline_response_dict,exception_info={"exception_message":str(e),"status":"failed",'etl_end_time':str(datetime.now())})

# COMMAND ----------

# MAGIC %md
# MAGIC #### -------------------------------------------------------------------*|| End of the Notebook  ||*-----------------------------------------------------------------------------

# COMMAND ----------

# Step 1: Extract EDI messages from the silver layer based on the provided configurations
silver_edi_msg_with_business_filtered_df = extract_edi_msg_from_silver(
                                                                        spark=spark,
                                                                        retailer_table_config=retailer_table_config,
                                                                        interface_name=interface_name,
                                                                        retailer_name=retailer_name
                                                                        )



# Step 2: Perform EAN to material mapping using the extracted EDI messages
ean_materail_mapping_df = ean_to_material_mapping(
                                                    edi_jda_join_df=silver_edi_msg_with_business_filtered_df,
                                                    ean_material_mapping_df=ean_material_mapping_df
                                                    )

# Step 3: Map EDI messages to JDA static data using the configuration file
edi_msg_constant_values_jda_join_df = mapping_edi_to_vmi_jda_mapping_file(
                                                                                spark=spark,
                                                                                retailer_silver_df=ean_materail_mapping_df,
                                                                                retailer_table_config=retailer_table_config,
                                                                                config_file_path=config_file_path,
                                                                                config_file_storage_type=config_file_storage_type,
                                                                                env=environment
                                                                            )

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from fdn

# COMMAND ----------

# MAGIC %sql
# MAGIC select MATERIAL_EAN,QTY_MISSING,INV_DATE,FILE_NAME  from fdn_sales_internal_bronze.tbl_snt_vmi_becarrefour where to_date(XTNUpdatedTime)='2024-09-25' and QTY_MISSING > 0 and MATERIAL_EAN in (8710398527301,
# MAGIC  8710398528087,
# MAGIC  8710398528100,
# MAGIC  8710398524799,
# MAGIC  8710398529749,
# MAGIC  8710398530080,
# MAGIC  8710398515742,
# MAGIC  8710398527363,
# MAGIC  8710398529695,
# MAGIC  8710398529916,
# MAGIC  8710398530004,
# MAGIC  8710398160874,
# MAGIC  8710398160935,
# MAGIC  8710398527592,
# MAGIC  8710398528858,
# MAGIC  8710398528872,
# MAGIC  8710398530394,
# MAGIC  8710398160843,
# MAGIC  8710398522740,
# MAGIC  8710398525901,
# MAGIC  8710398525963,
# MAGIC  8710398525987,
# MAGIC  8710398526007,
# MAGIC  8710398526021,
# MAGIC  8710398302960,
# MAGIC  8710398307576,
# MAGIC  8710398504555,
# MAGIC  8710398504593,
# MAGIC  8710398504630,
# MAGIC  8710398517937,
# MAGIC  8710398523563,
# MAGIC  8710398523587,
# MAGIC  8710398523624,
# MAGIC  8710398524072,
# MAGIC  8710398524102,
# MAGIC  8710398524140,
# MAGIC  8710398524188,
# MAGIC  8710398524218,
# MAGIC  8710398525123,
# MAGIC  8710398529473,
# MAGIC  8710398530134,
# MAGIC  8710398504494,
# MAGIC  8710398525185,
# MAGIC  8710398525208,
# MAGIC  8710398525710,
# MAGIC  8710398526601,
# MAGIC  8710398529602)

# COMMAND ----------

MaterialGlobalTradeItemNumberlist

# COMMAND ----------

#silver  carrefour Inventory today data query
silver_edi_msg_with_business_filtered_df.filter(F.col("MaterialGlobalTradeItemNumber").isin(MaterialGlobalTradeItemNumberlist)).filter("RetailWarehouseToStoreStockOutRegularQuantity > 0").select("MaterialGlobalTradeItemNumber","RetailWarehouseToStoreStockOutRegularQuantity","RetailerCustomerName").display()

# COMMAND ----------

#EAN- Mappeed Material ID data today for missing qty.
ean_materail_mapping_df.withColumn("Item",F.col("MaterialId").cast(LongType())).filter(F.col("Item").isin(materialIDlist)).filter(F.col("RetailWarehouseToStoreStockOutRegularQuantity")>0).select("Item","RetailWarehouseToStoreStockOutRegularQuantity","MaterialGlobalTradeItemNumber").display()

# COMMAND ----------

edi_msg_constant_values_jda_join_df.withColumn("Item",F.concat(F.col("MaterialId").cast(LongType()), F.lit("_"), F.col("JDA_CODE"))).filter("Item='340049641_01'").display()

# COMMAND ----------

dim_material_details=retailer_table_config["xcf_dim_details"]

# COMMAND ----------

edi_customer_missing_qty_df = get_material_description(
                                                            spark=spark,
                                                            edi_silver_material_id_df=edi_msg_constant_values_jda_join_df,
                                                            material_table_config=dim_material_details
                                                            )

# COMMAND ----------

F.col("edi.MaterialId").cast(LongType()), F.lit("_"), F.col("edi.JDA_CODE")

# COMMAND ----------

materialIDlist=[
340053303,
340051483,
340051322,
340054060,
340056548,
340057621,
340057394,
340025923,
340047866,
340057345,
340049638,
340051482,
340049641,
340050553,
340057018,
340049997,
340057390,
340049644,
340017659,
340051995,
340025939,
340055558,
340055545,
340057066,
340055559,
340051491,
340050551,
340049635,
340056966,
340017664,
340046414,
340057019,
340040295,
340055550,
340055311,
340053599,
340017656,
340042654,
340050360,
340057620,
340025945,
340053376,
340057615,
340054064,
340049640,
340057262,
340051481]

# COMMAND ----------

edi_customer_missing_qty_df.display()

# COMMAND ----------


