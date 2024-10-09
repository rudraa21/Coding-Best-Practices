from notebooks_gold.utilities.common_utilities import  get_dataframe,read_json_metadata,update_processing_time_and_exit,notebook_exit_response,is_table_exists,generate_filter_condition,is_column_exist,get_aggregate_data,get_vmi_mapping_data_from_confing_json
from tests_data import utils_function_test_data as uf_test_data
from conftest import are_dataframes_equal
from datetime import datetime
import logging
import pytest
from delta import DeltaTable
import json
from pyspark.sql.types import *

@pytest.mark.parametrize("test_name, database, table, expected_result", uf_test_data.is_tbl_exist_test_cases)
def test_is_table_exists(spark,test_name, database, table, expected_result):
    """
    Test the is_table_exists function with different scenarios.

    Args:
        test_case_name (str): A descriptive name for the test case.
        database (str): The name of the database to check.
        table (str): The name of the table to check.
        expected_result (bool): The expected result of the is_table_exists function.
    """
    # Calling the func to be tested
    result = is_table_exists(spark,database, table)

    assert result == expected_result

@pytest.mark.parametrize("kwargs, expected_exception_message", uf_test_data.get_notebook_exit_response_test_cases)
def test_notebook_exit_response(mocker, kwargs, expected_exception_message,dbutils):
        with pytest.raises(Exception,match=expected_exception_message):
            notebook_exit_response(dbutils, **kwargs)
        

@pytest.mark.parametrize("test_case_name, filter_template, expected_result", uf_test_data.gen_filter_cond_test_cases)
def test_generate_filter_condition(test_case_name, filter_template, expected_result):
    """
    Test the generate_filter_condition function with different scenarios.

    Parameters:
    - test_case_name (str): A descriptive name for the test case.
    - filter_template (list): The input filter template for the function.
    - expected_result (str or None): The expected result if not None, else an Assertion Error is expected.
    """
    if expected_result is not None:
        result = generate_filter_condition(filter_template)
        assert result == expected_result
    else:
        with pytest.raises(AssertionError):
            generate_filter_condition(filter_template)
 
def test_read_json_metadata_unsupported_storage_type():
    """
    Test case for validating the behavior of the read_json_metadata function with an unsupported storage type.
 
    This test ensures that the read_json_metadata function correctly raises a ValueError when an unsupported storage type
    is provided.
    """
    
    # Specify an unsupported storage type
    unsupported_storage_type = "unsupported"
 
    # Use pytest's context manager to check for the expected exception
    with pytest.raises(ValueError, match=f"Unsupported storage type: {unsupported_storage_type}"):
        # Call the function with the unsupported storage type
        read_json_metadata("test_file.json", unsupported_storage_type)



def test_get_dataframe(mocker, create_mock_dataframe,spark):
    """
    Test function for get_dataframe method.
    Parameters:
    - mocker: pytest mocker object for mocking dependencies.
    - create_mock_dataframe: Function to create mock DataFrame for testing.
    """
    # mocking few function which is env specific
    mocked_data = create_mock_dataframe([("Alice", 34), ("Bob", 45), ("Cat", 25)], ["name", "age"])
    mocker.patch("notebooks_gold.utilities.common_utilities.is_table_exists", return_value=True)
    mocker.patch.object(spark,"table", return_value=mocked_data)
    # Call the function to get DataFrame
    result_df = get_dataframe(spark=spark,
                              database_name="sample",
                              table_name="test_table",
                              filter_template=[{"col_name":"age", "col_val":30,"operator":">="}],
                              list_of_columns_to_select=["name", "new_col"],
                              transformed_or_new_cols_dict={"new_col": "length(name)"},
                              distinct_flag=True,
                              list_of_keys_to_fetch_distinct_rows=["name"])
 
    # Assert that the result DataFrame has the expected values
    expected_data = [("Alice", 5), ("Bob", 3)]
    expected_df = create_mock_dataframe(expected_data, ["name", "age"])
    assert are_dataframes_equal(result_df, expected_df)

def test_get_dataframe_wrong_params(mocker,spark):
    """
    Test function for get_dataframe function when incorrect parameters are provided.
    Args:
        mocker: pytest mocker fixture for mocking dependencies.
    """
    mocker.patch("notebooks_gold.utilities.common_utilities.is_table_exists", return_value=True)
    with pytest.raises(Exception, match='Failed to get dataframe with error *'):
        get_dataframe(spark=spark,database_name="non_existing_db",
                      table_name="non_existing_table",
                      filter_template="F.col('age') > 30",
                      list_of_columns_to_select=["name", "new_col"],
                      transformed_or_new_cols_dict={"new_col": "length(name)"},
                      distinct_flag=True,
                      list_of_keys_to_fetch_distinct_rows=["name"])

@pytest.mark.parametrize("column_list, expected_result",uf_test_data.get_is_column_exist_test_cases)
def test_is_column_exist(column_list, expected_result,create_mock_dataframe):
    
    df = create_mock_dataframe([("Alice",30),("Bob",25)],["name","age"])
    if isinstance(expected_result, str):
        with pytest.raises(AssertionError) as colinfo:
            is_column_exist(df, column_list)
        assert str(colinfo.value) == expected_result
    else:
        assert is_column_exist(df, column_list) == expected_result

def test_update_processing_time_and_exit(mocker,dbutils):
    """
    Test function for update_processing_time_and_exit.

    Parameters:
    - mocker: pytest mocker fixture for mocking dependencies.
    """
    # Mock notebook_exit_response function
    mocker.patch('notebooks_gold.utilities.common_utilities.notebook_exit_response', return_value={})

    # Initialize parameters
    etl_pipeline_response_dict = {}
    etl_start_time = datetime.now()

    # Call the function  dbutils,etl_pipeline_response_dict, etl_start_time
    update_processing_time_and_exit(dbutils,etl_pipeline_response_dict, etl_start_time)

    # Asserting to ensure that the function updates the ETL pipeline response dictionary correctly
    assert 'status' in etl_pipeline_response_dict
    assert etl_pipeline_response_dict['status'] == 'succeeded'

    assert 'etl_end_time' in etl_pipeline_response_dict
    assert isinstance(etl_pipeline_response_dict['etl_end_time'], str)

    assert 'etl_processing_time' in etl_pipeline_response_dict
    assert isinstance(etl_pipeline_response_dict['etl_processing_time'], float)


def test_get_aggregate_data(spark, create_mock_dataframe):
    # Create test data using create_mock_dataframe
    df = create_mock_dataframe([
        ("audit1", "loc1", "time1", 10, 20),
        ("audit1", "loc1", "time1", 15, 25),
        ("audit1", "loc2", "time1", 10, 20),
        ("audit2", "loc1", "time1", 10, 20)]
    , ["audit_col", "location_id", "timeframe_id", "metric1", "metric2"])
    
    # Define the groupby columns, aggregation columns, and alias
    groupby_cols = ["audit_col", "location_id", "timeframe_id"]
    agg_col = ["metric1", "metric2"]
    alias = "total"
    
    # Run the function
    result_df = get_aggregate_data(df, groupby_cols, agg_col, alias)
    
    # Create expected data using create_mock_dataframe
    expected_df = create_mock_dataframe([
        ("audit1", "loc1", "time1", 25,45),
        ("audit1", "loc2", "time1", 10,20),
        ("audit2", "loc1", "time1", 10,20)
    ], ["audit_col", "location_id", "timeframe_id", "total"])
    
    # Assert the results
    result_df_sorted = result_df.orderBy("audit_col", "location_id", "timeframe_id").collect()
    expected_df_sorted = expected_df.orderBy("audit_col", "location_id", "timeframe_id").collect()
    assert result_df_sorted == expected_df_sorted


@pytest.mark.parametrize("config_data, expected_schema",uf_test_data.get_vmi_mapping_data_from_confing_json_test_case)
def test_get_vmi_mapping_data_from_confing_json(config_data, expected_schema,spark):
    # Call the function under test
    result_df = get_vmi_mapping_data_from_confing_json(spark, config_data, expected_schema)

    # Perform assertions
    assert result_df.schema == expected_schema

@pytest.mark.parametrize("config_data,expected_schema,exception",uf_test_data.get_vmi_mapping_data_from_confing_json_wrong_params_test_cases)
def test_get_vmi_mapping_data_from_confing_json_wrong_params(config_data,expected_schema,exception,spark):
    with pytest.raises(Exception,match=exception):
        get_vmi_mapping_data_from_confing_json(spark,config_data, expected_schema)
