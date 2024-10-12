from CODE_BEST_PRACT.UTILITIES.Common_Utilities import check_table_exists,convert_timestamp_to_date,change_name_case,apply_transformations,ingest_data
from CODE_BEST_PRACT.Test import utils_function_test_data1 as uf_test_data
from unittest.mock import patch
import conftest
from pyspark.sql import SparkSession, Row
import pytest

@pytest.mark.parametrize("three_level_namespace, expected_result", uf_test_data.check_table_exists_test_cases)
def test_check_table_exists(spark,three_level_namespace, expected_result):
    # If the expected result is an exception, we test for it
    if expected_result == Exception:
            with pytest.raises(Exception):
                check_table_exists(spark,three_level_namespace)
                print("test passed")
    else:
        # Mock the return of spark.catalog.tableExists based on the expected result
        with patch('pyspark.sql.catalog.Catalog.tableExists', return_value=expected_result):
           output = check_table_exists(spark,three_level_namespace)
           print(output)
        assert output == expected_result

############################################################################################################################

@pytest.mark.parametrize("input_data, date_format_str, expected_data", uf_test_data.valid_conversion_test_cases)
def test_valid_conversion_timestamp(spark, input_data, date_format_str, expected_data):
    # Input DataFrame creation
    columns = ["timestamp_column"]
    df = spark.createDataFrame(input_data, columns)

    # Expected DataFrame creation
    expected_columns = ["Date"]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Call the conversion function
    result_df = convert_timestamp_to_date(df, "timestamp_column", date_format_str)

    # Select only the "Date" column for comparison
    result_df_selected = result_df.select("Date")

    # Assert that the result matches the expected data
    assert result_df_selected.collect() == expected_df.collect(), f"Expected {expected_df.collect()}, but got {result_df_selected.collect()}"

##############################################################################################################################
@pytest.mark.parametrize("input_columns, case, expected_columns", uf_test_data.change_name_case_test_cases)
def test_change_name_case(spark, input_columns, case, expected_columns):
    # Create a DataFrame using input_columns
    data = [Row(**{col: 'test' for col in input_columns})]
    df = spark.createDataFrame(data)

    # Call the function and pass the parameters
    result_df = change_name_case(df, case) if case else change_name_case(df)

    # Assert if the result columns match the expected columns
    assert result_df.columns == expected_columns, f"Expected {expected_columns}, but got {result_df.columns}"

##############################################################################################################################

@pytest.mark.parametrize("input_data, transformations, expected_data", uf_test_data.transformations_test_cases)
def test_apply_transformations(spark, input_data, transformations, expected_data):
    # Create a DataFrame from input_data
    input_columns = ["_1"] if len(input_data[0]) == 1 else ["_1", "_2"]
    df = spark.createDataFrame(input_data, input_columns)

    # Apply transformations using the function
    transformed_df = apply_transformations(df, transformations)
    
    # Select only the transformed columns for comparison
    transformed_columns = [name for name, _ in transformations]
    transformed_df = transformed_df.select(*transformed_columns)

    # Create a DataFrame for expected output
    expected_columns = [name for name, _ in transformations]
    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Collect results and compare
    assert transformed_df.collect() == expected_df.collect(),f"Expected {expected_df.collect()}, but got {transformed_df.collect()}"

##############################################################################################################################
@pytest.mark.parametrize("source_table, filter_condition, expected_data",uf_test_data.ingest_data_test_cases)
def test_ingest_data(spark, source_table, filter_condition, expected_data):
    # Create test table inside the test
    if source_table == "test_table":
        data = [(1, "John Doe"), (2, "Jane Doe"), (3, "Alice"), (4, "Bob")]
        df = spark.createDataFrame(data, ["id", "name"])
        df.createOrReplaceTempView(source_table)  # Create temporary test table

    try:
        if source_table == "non_existent_table":
            with pytest.raises(Exception):
                ingest_data(spark,source_table, filter_condition)  
        else:
            df = ingest_data(spark,source_table, filter_condition) 
            result = [tuple(row) for row in df.collect()]
            assert result == expected_data, f"Expected {expected_data}, but got {result}"

    finally:
        # Drop the test table after the test
        if source_table == "test_table":
            spark.sql(f"DROP VIEW IF EXISTS {source_table}")


    