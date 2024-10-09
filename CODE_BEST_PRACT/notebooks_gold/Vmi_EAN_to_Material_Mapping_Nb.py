# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### EAN to Material Mapping Notebook
# MAGIC
# MAGIC ##### Overview
# MAGIC
# MAGIC   > ##### This notebook is responsible for mapping EAN (European Article Number) to its corresponding Material ID based on certain business filters. The mapping provided by this notebook is essential for internal processing within our systems.
# MAGIC
# MAGIC ##### Key Features
# MAGIC
# MAGIC - **EAN to Material Mapping:** Provides a comprehensive mapping of EAN to Material ID.
# MAGIC - **Business Filters:** Applies business filters to ensure accurate mapping.
# MAGIC
# MAGIC ###### Frequency
# MAGIC - **Execution:** As needed based on the arrival of new EDI messages.
# MAGIC
# MAGIC ##### 📅 History
# MAGIC | Date       | Author  | Description                |
# MAGIC |------------|---------|----------------------------|
# MAGIC | 2024-04-21 | Chandra | Initial Development       |
# MAGIC | 2024-06-18 | Chandra | Added ZOR Filter to retrieve only finished goods from Order Header table      |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ##### 📚 Importing Required Libraries
# MAGIC
# MAGIC This section focuses on importing the necessary libraries to facilitate data processing and analysis within the Databricks notebook.

# COMMAND ----------

#pyspark libs
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType,IntegerType,StringType,LongType,StructType,StructField,ArrayType,MapType,DateType
from delta import DeltaTable
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.window import Window

#python libs
import json
from datetime import datetime
import re
import pip
import sys
import subprocess
from typing import Dict,List,Optional,Any,Callable
import logging
from itertools import product

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC #### ⚙️ Run Utilities for VMI  Gold
# MAGIC
# MAGIC This section involves executing utilities specifically designed for VMI Gold, streamlining operations and enhancing the functionality of the Databricks notebook.

# COMMAND ----------

# MAGIC %run ./utilities/common_utilities

# COMMAND ----------

# MAGIC %md
# MAGIC #### 🔄 Material ID Retrieval
# MAGIC
# MAGIC   > This section outlines the process of retrieving Material ID from EAN
# MAGIC
# MAGIC   > **Tables Used:** Order table, Order Line table, Sales Customer Table
# MAGIC #### Steps
# MAGIC 1. **Retrieve Customer IDs:**
# MAGIC      - **Using GLNs:** Utilize Global Location Numbers (GLNs) under VMI scope to retrieve the corresponding Customer IDs from the Master Customer dimensional table.
# MAGIC  
# MAGIC 2. **Filter Orders:**
# MAGIC      - **Using Customer IDs:** Filter orders created by the identified customers from the Order Fact table.
# MAGIC
# MAGIC 3. **Retrieve Latest Order:**
# MAGIC      - **Selection Criteria:** Select the latest order out of the filtered orders.
# MAGIC      - **Purpose:** This step aims to obtain the most recent order to ensure accurate EAN to Material ID mapping.
# MAGIC
# MAGIC 4. **EAN to Material Mapping:**
# MAGIC      - **Using Customer IDs:** Filter orders created by the identified customers from the Order Fact table.
# MAGIC      - **Utilization:** Utilize the EAN information from the selected order to map it to its corresponding Material ID.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 🚀 Main Execution
# MAGIC
# MAGIC  > ##### This section contains the main execution logic of the code, which is triggered when the script is run as the main program.

# COMMAND ----------

if __name__ == "__main__":

    try:
        # Input 1: env 
        dbutils.widgets.text(name="env", defaultValue="", label="env")

        # Input 2: retailer name 
        # Description:The widget 'retailer_name' is either DELHAIZE/CARREFOUR
        dbutils.widgets.text(name="retailer_name", defaultValue="", label="retailer_name")

        # Input 3: retailer name 
        dbutils.widgets.text(defaultValue="",label="EAN",name="EAN")

        # Input 4: retailer name 
        dbutils.widgets.text(defaultValue="",label="MATERIAL",name="MATERIAL")

        # Initialize configuration variables
        env = dbutils.widgets.get("env")
        retailer_name = dbutils.widgets.get("retailer_name") 
        EAN = dbutils.widgets.get('EAN')
        MATERIAL = dbutils.widgets.get('MATERIAL')

        # Log input parameters
        logging.info(f"***** Received input parameters to the notebook *****")
        logging.info(f"env: {env}")
        logging.info(f"retailer_name: {retailer_name}")

        ##### Step 1 Define table configurations

        table_config = {
                        "order_header_schema": {
                                              "table_name": "order",
                                              "database_name": 'fdn_sales_confidential' if env.lower()=='prod' else 'fdn_sales_confidential_p2d',
                                              "watermark_column": "XTNRecordCreatedDate",
                                              "required_columns" : [ 
                                                                    "OrderId","XTNRecordCreatedDate","XTNSalesDocumentTypeCode"
                                                                    ],
                                              "filter_template" : [
                                                                    {"col_name": "XTNDFSystemId", "col_val": f"'{501008}'", "operator": '=='},
                                                                    {"col_name": "XTNDFReportingUnitId", "col_val": f"'{106903}'", "operator": '=='},
                                                                    {"col_name": "InstanceId", "col_val": "'P16'", "operator": '=='},
                                                                    {"col_name": "XTNSalesDocumentTypeCode", "col_val": "'ZOR'", "operator": '=='}                  
                                                                  ]
                                           },
                        "order_item_schema": {
                                              "table_name": "xtnorderline",
                                              "database_name": 'fdn_sales_confidential' if env.lower()=='prod' else 'fdn_sales_confidential_p2d',
                                              "watermark_column": "XTNRecordCreatedDate",                      
                                              "required_columns" :  [
                                                                     "SalesDocumentNumber","XTNUniversalProductCode","MaterialId"
                                                                    ],
                                              "filter_template" : [
                                                                    {"col_name": "XTNDFSystemId", "col_val": f"'{501008}'", "operator": '=='},
                                                                    {"col_name": "XTNDFReportingUnitId", "col_val": f"'{106903}'", "operator": '=='},
                                                                    {"col_name": "InstanceId", "col_val": "'P16'", "operator": '=='},
                                                                    {"col_name": "XTNUniversalProductCode", "operator": 'isNotNull'} 
                                                                                      
                                                                  ]
                                              },   
                                    
                            "dim_sales_customer_schema": {
                                                            "table_name": "Customer",
                                                            "database_name":'fdn_masterdata_confidential' if env.lower()=='prod' else 'fdn_masterdata_confidential_p2d' ,
                                                            "required_columns": [ "CustomerId", "XTNInternationalLocationPrimaryNumber", 
                                                                                  "XTNInternationalLocationSecondaryNumber","XTNInternationalLocationNumberDigitCheckIndicator","GLN"
                                                                                ],

                                                            "filter_template" : [
                                                                                    {"col_name": "XTNDFSystemId", "col_val": f"'{501008}'", "operator": '=='},
                                                                                    {"col_name": "XTNDFReportingUnitId", "col_val": f"'{107240}'", "operator": '=='},
                                                                                    {"col_name": "InstanceId", "col_val": f"'P16'", "operator": '=='} 
                                                                                      
                                                                                ] 
                                                          }  


                } 
    
        #Capturing ETL start time
        etl_start_time=datetime.now()

        ##### Initializing ETL Runtime Paramters & Metrics variable
        # To store the required parameters passed by the caller ADF
        user_input_params_dict = {}

        # To store runtime metrics (measurable outputs of the pipeline), such as the count of records loaded, time taken to run, records skipped, etc.
        runtime_metrics_dict = {}

        # To store runtime parameters required to execute the ETL pipeline. These parameters are retrieved from the Metatable control table.
        runtime_pipeline_parameters_dict = {}

        # To store all the above ETL pipeline-related information in one dictionary to send out as a response from DBX to any external service like ADF or Log Analytics.
        etl_pipeline_response_dict = {}

        # Getting notebook path 
        notebook_nm = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

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
        
        ######################################### Preparing the List of GLN's required #############################################################
        dlh_wh_gln_lst = ["5400110000122","5400110000313","5400110000306"]
        dlh_cust_gln_lst = ["5400110000009"]
    
        crf_wh_gln_lst = ["5400102000048","5400102000024"]
        crf_cust_gln_lst = ["5400102000086"]

        glns_lst =[]
        if retailer_name.upper()=="DELHAIZE":
            glns_lst = dlh_wh_gln_lst + dlh_cust_gln_lst
        elif retailer_name.upper()=="CARREFOUR":
            glns_lst = crf_wh_gln_lst + crf_cust_gln_lst
        else:
            glns_lst = dlh_wh_gln_lst + dlh_cust_gln_lst + crf_wh_gln_lst + crf_cust_gln_lst


        ######################################### Customer Master Data ########################################################################
        table_config["dim_sales_customer_schema"]["filter_template"].append({"col_name": "GLN", "col_val": f"{glns_lst}", "operator": 'isin'} )

        transformed_or_new_cols_dict = { 
                                         "XTNInternationalLocationPrimaryNumber":"lpad(XTNInternationalLocationPrimaryNumber,7,0)",            
                                          "XTNInternationalLocationSecondaryNumber" :"lpad(XTNInternationalLocationSecondaryNumber,5,0)" ,
                                          "GLN":'concat(XTNInternationalLocationPrimaryNumber,XTNInternationalLocationSecondaryNumber,XTNInternationalLocationNumberDigitCheckIndicator)'
                                       } 
         
        dim_customer_df = get_dataframe(
                                            spark=spark,
                                            database_name=table_config["dim_sales_customer_schema"]["database_name"],
                                            table_name=table_config["dim_sales_customer_schema"]["table_name"],
                                            filter_template=table_config["dim_sales_customer_schema"]["filter_template"],
                                            list_of_columns_to_select=table_config["dim_sales_customer_schema"]["required_columns"],
                                            transformed_or_new_cols_dict=transformed_or_new_cols_dict,
                                            distinct_flag=False,
                                            list_of_keys_to_fetch_distinct_rows=None
                                          )

        row_customerid_list = dim_customer_df.select("CustomerId").collect()
        customerid_list = [row["CustomerId"] for row in row_customerid_list]


        ######################################### Order Header Fact Data ########################################################################
        table_config["order_header_schema"]["filter_template"].append({"col_name": "CustomerId", "col_val": f"{customerid_list}", "operator": 'isin'} )
        fact_order_header_df = get_dataframe(
                                            spark=spark,
                                            database_name=table_config["order_header_schema"]["database_name"],
                                            table_name=table_config["order_header_schema"]["table_name"],
                                            filter_template=table_config["order_header_schema"]["filter_template"],
                                            list_of_columns_to_select=table_config["order_header_schema"]["required_columns"],
                                            transformed_or_new_cols_dict=None,
                                            distinct_flag=False,
                                            list_of_keys_to_fetch_distinct_rows=None
                                          )
        
        ######################################### Order Line Fact Data ########################################################################

        fact_order_item_df = get_dataframe(
                                            spark=spark,
                                            database_name=table_config["order_item_schema"]["database_name"],
                                            table_name=table_config["order_item_schema"]["table_name"],
                                            filter_template=table_config["order_item_schema"]["filter_template"],
                                            list_of_columns_to_select=table_config["order_item_schema"]["required_columns"],
                                            transformed_or_new_cols_dict=None,
                                            distinct_flag=False,
                                            list_of_keys_to_fetch_distinct_rows=None
                                          )
        
        ######################################### Join Order HDR and Line Data ##################################################################
        order_header_item_join_cond = (F.col("ord_hdr.OrderId").cast(LongType()) == F.col("ord_itm.SalesDocumentNumber").cast(LongType()))


        #print("fact_order_header_df:",fact_order_header_df.count(),"fact_order_item_df:",fact_order_item_df.count())


        if not fact_order_header_df.isEmpty() and not fact_order_item_df.isEmpty():
            # order_header_item_joined_df =  fact_order_header_df.alias("ord_hdr").join(           
            #                                                                         fact_order_item_df.alias("ord_itm"),  
            #                                                                         on=order_header_item_join_cond, 
            #                                                                         how="inner"
            #                                                                         )
            order_header_item_joined_df =  fact_order_item_df.alias("ord_itm").join(           
                                                                                    fact_order_header_df.alias("ord_hdr"),  
                                                                                    on=order_header_item_join_cond, 
                                                                                    how="inner"
                                                                                    )
        else:
            raise Exception("Data Error: Either 'fact_order_header_df' or 'fact_order_item_df' is empty. Cannot proceed.")

        
        window_spec = Window.partitionBy("XTNUniversalProductCode").orderBy(F.col("XTNRecordCreatedDate").desc())
        ean_material_mapping_df = (
                                    order_header_item_joined_df
                                    .withColumn("rank", F.row_number().over(window_spec))
                                    .filter("rank == 1")  # Keep only the rows with rank 1, i.e., the rows with the maximum "CREATEDON" within each partition
                                    .drop("rank")  # Drop the temporary rank column
                                    .select("XTNUniversalProductCode", "XTNRecordCreatedDate", "MaterialId")
                                    .distinct()
                                   )

    except Exception as e:
        raise e

# COMMAND ----------

# ean_material_mapping_df.display()

# COMMAND ----------

# if EAN:
#     material_ean_mapping_df= ean_material_mapping_df.filter(  ean_material_mapping_df["XTNUniversalProductCode"].isin(EAN.split(",")) )

# if MATERIAL:
#     material_ean_mapping_df = ean_material_mapping_df.filter(  ean_material_mapping_df["MaterialId"].cast(LongType()) == F.lit(MATERIAL).cast(LongType()) )


# COMMAND ----------


