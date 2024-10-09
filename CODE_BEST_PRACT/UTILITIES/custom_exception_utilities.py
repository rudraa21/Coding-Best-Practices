# Databricks notebook source
# MAGIC %md
# MAGIC ##### This notebook defines custom exception classes to handle specific error scenarios. The custom exception classes are as follows:
# MAGIC - TableNotFoundException: This exception is raised when a table is not found. It inherits from the base Exception class and includes the table name in the error message
# MAGIC - ValueErrorException: This exception is raised for value errors. It inherits from the base Exception class and includes a custom error message.
# MAGIC - AssertionErrorException: This exception is raised for assertion errors. It inherits from the base Exception class and includes a custom error message.
# MAGIC - ValueNotFoundException: This exception is raised when a value is not found. It inherits from the base Exception class and includes the value in the error message.
# MAGIC

# COMMAND ----------

class TableNotFoundException(Exception):
    """Exception raised when a table is not found."""

    def __init__(self, table_name):
        self.table_name = table_name
        super().__init__(f"Table not found: {table_name}")

class PathNotFoundException(Exception):
    """Exception raised when a table is not found."""

    def __init__(self, path):
        self.path = path
        super().__init__(f"path not found: {path}")

class FileNotFoundException(Exception):
    """Exception raised when a table is not found."""

    def __init__(self, FileName):
        self.FileName = FileName
        super().__init__(f"File not found: {FileName}")

class SystemReportingIdNotFoundException(Exception):
    """Exception raised when a value is not found."""

    def __init__(self, value):
        self.value = value
        super().__init__(f"Value not found: {value}")
    

class ValueErrorException(Exception):
    """Exception raised for value errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Value Error: {message}")


class AssertionErrorException(Exception):
    """Exception raised for assertion errors."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Assertion Error: {message}")

class TimestampNotFoundException(Exception):
    """Exception raised when a timestamp is not found."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Timestamp not found: {message}")

class ReportingYearNotFoundException(Exception):
    """Exception raised when a reporting year is not found."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Reporting year not found: {message}")

class FilterFailedException(Exception):
    """Exception raised when a filter is failed."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Filter failed: {message}")

class TransformationFailedException(Exception):
    """Exception raised when transformation is failed."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Transformation failed: {message}")    

class writeFailedException(Exception):
    """Exception raised when write operation is failed."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Write to target failed: {message}")      
        
class ValidationFailedException(Exception):
    """Exception raised when validation is failed."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Validation of notebook failed: {message}")    


class EmptyDataFrameException(Exception):
    """Exception raised when Data is Empty."""

    def __init__(self, message):
        self.message = message
        super().__init__(f"Reading Dataframe Failed: {message}")           
