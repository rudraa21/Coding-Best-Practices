from datetime import datetime,timedelta
from pyspark.sql.types import *
"""
Test Data for utils_function Module

This module contains test data for the functions defined in the utils_function module. The provided data covers a range of scenarios, inputs, and edge cases

Test Data Structure:
- The test data is organized based on the functions it is designed to test.
- Each list provides tuple of input data, expected outputs, and test case name
"""
# Parametrized test cases for filter_data_by_conditions function
get_filter_template_test_cases = [
    (
        "Openorders with Delhaize",
        "openorders", "delhaize", "date_col",
        [{"col_name": "date_col", "col_val": f"'{datetime.now().date()}'", "operator": '==' }]
    ),
    (
        "Openorders with Carrefour",
        "openorders", "carrefour", "date_col",
        Exception
    ),
    (
        "Inventory with Delhaize",
        "inventory", "delhaize", "date_col",
        [{"col_name": "date_col", "col_val": f"'{datetime.now().date()}'", "operator": '==' }]
    ),
    (
        "Inventory with Carrefour",
        "inventory", "carrefour", "date_col",
        [{"col_name": "date_col", "col_val": f"'{datetime.now().date() - timedelta(days=1)}'", "operator": '==' }]
    ),
    ("Demandtodate with Delhaize", 
     "demandtodate", "delhaize", "date_col", 
    [{"col_name": "date_col", "col_val": f"'{datetime.now().date() - timedelta(days=(datetime.now().date().weekday()) % 7) + timedelta(days=1)}'", "operator": '>='},
     {"col_name": "date_col", "col_val": f"'{datetime.now().date()}'", "operator": '<='} ]
),(
    "Demandtodate with Carrefour", 
     "demandtodate", "carrefour", "date_col", 
    [{"col_name": "date_col", "col_val": f"'{datetime.now().date() - timedelta(days=(datetime.now().date().weekday()) % 7)}'", "operator": '>='},
     {"col_name": "date_col", "col_val": f"'{datetime.now().date() - timedelta(days=1)}'", "operator": '<='} ]
)
,
   ("Ordershistory", 
    "ordershistory", "carrefour", "date_col", 
 [
     {"col_name": "date_col", "col_val": f"'{((datetime.now() - timedelta(days=1))-timedelta(days=((datetime.now()-timedelta(days=1)).weekday()) % 7)).date()}'", "operator": '>='},
     {"col_name": "date_col", "col_val": f"'{(datetime.now() - timedelta(days=1)).date()}'", "operator": '<='}
 ]
)
]
# Parametrized test cases for get edi message function
get_extract_edi_msg_test_case=[
    ( 
     [('alice',18,((datetime.now() - timedelta(days=1))-timedelta(days=((datetime.now()-timedelta(days=1)).weekday()) % 7)).date().strftime('%Y-%m-%d')),('bob',18,(datetime.now() - timedelta(days=1)).date().strftime('%Y-%m-%d')),('cat',20,'2024-05-06')],
        {"table_name": "test_table",
        "database_name":"sample",                           
        "required_columns" :["name"],
        "primary_key_columns": [],
        "filter_template" : [{"col_name": "age", "col_val": f"'{18}'", "operator": '=='}],
        "watermark_column" : {  "ordershistory":"BusinessDate"}
        } ,
        "ordershistory",
        "delhaize",
        [('alice',),('bob',)]
    ),
    (
        [('alice',19,(datetime.now()- timedelta(days=1)).date().strftime('%Y-%m-%d')),('cat',20,'2024-05-06')],
        {"table_name": "test_table",
        "database_name":"sample",                           
        "required_columns" :["name"],
        "primary_key_columns": [],
        "filter_template" : [{"col_name": "age", "col_val": f"'{19}'", "operator": '=='}],
        "watermark_column" : {"inventory":"BusinessDate"}
        } ,
        "inventory",
        "carrefour",
        [('alice',)]
    ),
]
#Parametrized test cases for get edi jda mapping function
get_edi_jda_mapping_test_case = [
    # Test case: List of dictionaries where each dictionary represents a row of data
    (
        [
            {"col1": "value1_col1", "col2": "value1_col2"},
            {"col1": "value2_col1", "col2": "value2_col2"}
        ],
        StructType([
            StructField("col1", StringType(), True),
            StructField("col2", StringType(), True)
        ])
    ),
    (
        [
            {"col3": "1", "col4": "2"},
            {"col3": "3", "col4": "4"}
        ],
        StructType([
            StructField("col3", StringType(), True),
            StructField("col4", StringType(), True)
        ])
    )
]
#Parametrized test cases for get mapping edi to vmi jda
get_mapping_edi_to_vmi_jda_mapping_test_cases = [
    (
         {"table_name": "test_table",
        "database_name":"sample"
         },
         "localpath",
         "adls2"

    )
]
#Parametrized test cases for get ean material mapping function
get_ean_to_material_mapping_test_cases = [
    (
        # Sample Data for edi_jda_join_df
        [("123","test"),("324","test2"),("412","test3")],
        # Sample Data for ean_material_mapping_df
        [("123","India"),("21","Americia")],
        # Sample Data for expected_schema_df
        [("123","test","123","India"),("324","test2",None,None),("412","test3",None,None)]
    )
]
