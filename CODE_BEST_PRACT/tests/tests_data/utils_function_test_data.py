from pyspark.sql import types as T
"""
Test Data for utils_function Module

This module contains test data for the functions defined in the utils_function module. The provided data covers a range of scenarios, inputs, and edge cases

Test Data Structure:
- The test data is organized based on the functions it is designed to test.
- Each list provides tuple of input data, expected outputs, and test case name
"""

#  Parametrized test cases for filter_data_by_conditions function
filter_by_cond_test_cases = [
    (
        # Test case name
        "Applying filter based on two columns",
        # input parameters for the filter_data_by_conditions func
        [(1, 'Alice'), (1, 'Bob'), (3, 'Charlie')],
        {"col_0": 1, "col_1": "Alice"},
        # expected output of the filter_data_by_conditions func
        [(1, 'Alice')]
    ),
    (
        "Applying filter based on one column",
        [(1, 'Alice'), (1, 'Bob'), (3, 'Charlie')],
        {"col_0": 1},
        [(1, 'Alice'), (1, 'Bob')]
    ),
    (
        "Applying filter which returns no data",
        [(1, 'Alice'), (1, 'Bob'), (3, 'Charlie')],
        {"col_0": 5},
        []
    )
    # Add more test case scenarios if needed 
]

# Parametrized test cases for get_filtered_data function
get_filtered_data_test_cases = [
    (
        # Test case name
        "Passing multiple table as dataframe",
        # input parameters for the get_filtered_data func
        {
            'table1':[(1, 'Alice', 12), (1, 'Bob', 23), (3, 'Charlie', 23)],
            'table2':[(1, 'Alice', 'ghh'), (2, 'Hunt', 'ere'), (3, 'Boat', 'sdf')],
        }, 
        {"col_0": 1, "col_1": "Alice"},
        # expected output of the get_filtered_data func
        {
            'table1':[(1, 'Alice', 12)],
            'table2':[(1, 'Alice', 'ghh')],
        }
    ),
    (
        "Filter which returns empty dataframe",
        {
            'table1':[(1, 'Alice', 12), (1, 'Bob', 23), (3, 'Charlie', 23)],
        }, 
        {"col_0": 10}, 
        {
            'table1':[],
        }
    ), 
]
# Parametrized test cases for is_table_exists function
is_tbl_exist_test_cases = [
    (
        # Test case name
        "Non Existing table in database",
        # input parameters for the is_table_exists func
        "database2", "table2", 
        # Expected output 
        False
    ), 
]
gen_filter_cond_test_cases = [
    (
        "Valid input with default conditional operator",
        [
            {"col_name": "column1", "operator": "==", "col_val": "'value'"},
            {"col_name": "column2", "operator": ">", "col_val": 10}
        ],
        "(F.col('column1') == 'value' ) & (F.col('column2') > 10 )"
    ),

    (
        "Valid input with custom conditional operator",
        [
            {"col_name": "column1", "operator": "==", "col_val": "'value1'", "conditonal_operator": "|"},
            {"col_name": "column2", "operator": ">", "col_val": 10, "conditonal_operator": "|"},
            {"col_name": "column3", "operator": "like", "col_val": "'sample'", "conditonal_operator": "|"},
            {"col_name": "column3", "operator": "not like", "col_val": "'not req'"}
        ],
        "(F.col('column1') == 'value1' ) | (F.col('column2') > 10 ) | (F.col('column3').like('sample')) | ~ (F.col('column3').like('not req'))"
    ),

    (
        "Valid input with colm_function and diffent operators",
        [
            {"col_name": "column1", "operator": "isin", "col_val": "'value1'", "colm_function": "trim,lower"},
            {"col_name": "column2", "operator": "isNull", "colm_function": "lower"},
            {"col_name": "column2", "operator": "isNotNull"},
        ],
        "( F.trim(F.lower (F.col('column1'))).isin('value1')) & ( F.lower (F.col('column2')).isNull() ) & (F.col('column2').isNotNull() )"
    ),

    (
        "Empty filter_template",
        [],
        None  # None is used to handle the case of an empty filter_template
    ),
]
get_data_from_tbl_test_cases = [
    (
        # Test case name
        "Test fn with incremental load type",
        # Input for the get_delta_or_full_load_from_table
        "sample_db", "sample_tbl", "incremental", "timestamp_col",
        [("Alice", 30, "2024-02-20 12:00:00"),
          ("Bob", 35, "2024-02-20 12:00:00"),
          ("Charlie", 40, "2024-02-20 12:30:00")],
        # Expected output of the get_delta_or_full_load_from_table
        [("Charlie", 40, "2024-02-20 12:30:00")]
    ),
    (
        "Test fn with full load load type",
        "sample_db", "sample_tbl", "fullload", None,
        [("Alice", 30, "2024-02-20 12:00:00"),
         ("Bob", 35, "2024-02-20 12:00:00"),
         ("Charlie", 40, "2024-02-20 12:30:00")],

        [("Alice", 30, "2024-02-20 12:00:00"),
         ("Bob", 35, "2024-02-20 12:00:00"),
         ("Charlie", 40, "2024-02-20 12:30:00")]
    )
]
get_notebook_exit_response_test_cases = [
    ({"exception_info": "Some error occurred"}, "Exception occured: Some error occurred"),  # Exception case
]
get_is_column_exist_test_cases = [
    # Test case 1: All columns present
    (["name"], True),
    # Test case 2: Some columns present
    (["name", "age"], True),
    # Test case 3: Missing columns
    (["name", "height"], False),
    # Test case 4: Empty column list
    ([], "Recieved invalid input. It seems either df is empty or not type of dataframe or columns list is empty")
]
get_vmi_mapping_data_from_confing_json_test_case = [
    # Test case: List of dictionaries where each dictionary represents a row of data
    (
        [
            {"column1": "value1_", "column2": "value3"},
            {"column1": "value2_", "column2": "value4"}
        ],
        T.StructType([
            T.StructField("column1", T.StringType(), True),
            T.StructField("column2", T.StringType(), True)
        ])
    )

]
get_vmi_mapping_data_from_confing_json_wrong_params_test_cases=[
(      
        [
            {"col1":"1"},
            {"col2":"2"}
        ],
        T.StructType([
            T.StructField("col3",T.StringType(),True),
            T.StructField("col4",T.StringType(),True)
        ]),
        "Schema does not match the expected schema"
)
]