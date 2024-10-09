from pyspark.sql.functions import *

check_table_exists_test_cases = [
   # Table does not exist, expected result is False    
    ("database.schema.non_existent_table", False)  
]
#########################################################################################
valid_conversion_test_cases = [
    # Case 1: Valid timestamp conversion to 'yyyy-MM-dd'
    ([("2024-10-07 12:34:56",)], 'yyyy-MM-dd', [("2024-10-07",)]),

    # Case 2: Valid timestamp conversion to 'MM/dd/yyyy'
    ([("2024-10-07 12:34:56",)], 'MM/dd/yyyy', [("10/07/2024",)]),

    # Case 3: Valid timestamp conversion to 'dd-MM-yyyy'
    ([("2024-10-07 12:34:56",)], 'dd-MM-yyyy', [("07-10-2024",)]),

    # Case 4: Valid timestamp conversion with different time
    ([("2023-03-15 18:45:30",)], 'dd/MM/yyyy', [("15/03/2023",)])
]
########################################################################################
change_name_case_test_cases = [
    # Case 1: Convert column names to upper case
    (['name', 'age', 'address'], 'upper', ['NAME', 'AGE', 'ADDRESS']), 
    
    # Case 2: Convert column names to lower case
    (['NAME', 'AGE', 'ADDRESS'], 'lower', ['name', 'age', 'address']),  
    
    # Case 3: Default case (upper case)
    (['name', 'age', 'address'], None, ['NAME', 'AGE', 'ADDRESS']),  
    
    # Case 4: Invalid case, should return the original column names
    (['Name', 'Age', 'Address'], 'invalid', ['Name', 'Age', 'Address'])  
    
]
#########################################################################################
transformations_test_cases = [
    (
        # Test Case 1: Incrementing a column by 1
        [(1,), (2,), (3,)],
        [("incremented_col", col("_1") + 1)],
        [(2,), (3,), (4,)]
    ),
    (
        # Test Case 3: Combining multiple transformations
        [(1, 2), (3, 4)],
        [("incremented_col", col("_1") + 1), ("doubled_col", col("_2") * 2)],
        [(2, 4), (4, 8)]
    ),
    (
        # Test Case 4: Adding a string suffix to a column
        [("John",), ("Jane",)],
        [("name_with_suffix", concat(col("_1"),lit("_Doe")))],
        [("John_Doe",), ("Jane_Doe",)]
    ),
]
#####################################################################################################################
ingest_data_test_cases=[
    # Ingest without filter
    ("test_table", None, [(1, "John Doe"), (2, "Jane Doe"), (3, "Alice"), (4, "Bob")]),

    # Ingest with a valid filter
    ("test_table", "id = 1", [(1, "John Doe")]),

    # Ingest with a valid filter (multiple conditions)
    ("test_table", "id > 2", [(3, "Alice"), (4, "Bob")]),

     # Test with non-existent table
    ("non_existent_table", None, None),  # Expected to raise an error
]