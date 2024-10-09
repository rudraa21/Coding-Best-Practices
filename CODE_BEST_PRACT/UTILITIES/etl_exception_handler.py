from datetime import datetime
import logging
import sys,json
from typing import Dict, Optional, Any
from pyspark.sql import SparkSession
import IPython
import traceback


def _get_spark() -> SparkSession:
    user_ns = IPython.get_ipython().user_ns
    if "spark" in user_ns:
        return user_ns["spark"]
    else:
        spark = SparkSession.builder.getOrCreate()
        user_ns["spark"] = spark
        return spark

def _get_dbutils(spark: SparkSession):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        dbutils = IPython.get_ipython().user_ns.get("dbutils")
        if not dbutils:
            logging.warning("Could not initialize dbutils!")
    return dbutils

class ETLErrorHandler:
    """
    Handles ETL errors and updates the ETL pipeline response dictionary.

    Attributes:
        etl_pipeline_response_dict (Dict[str, Any]): Dictionary to store ETL status and timing.
        spark (SparkSession): The Spark session.
        dbutils (object): Databricks utilities object.
        etl_start_time (datetime): The start time of the ETL process.
    """

    def __init__(self, etl_pipeline_response_dict: Dict[str, Any], 
                 spark: Optional[SparkSession] = None, 
                 dbutils: Optional[object] = None):
        self.etl_pipeline_response_dict = etl_pipeline_response_dict
        self.spark = spark or _get_spark()
        self.dbutils = dbutils or _get_dbutils(self.spark)
        self.etl_start_time = datetime.now()
        self.etl_pipeline_response_dict.update({'etl_start_time': str(self.etl_start_time)})

    def update_response(self, status: str, error_message: str = None):
        """
        Updates the ETL pipeline response dictionary with the current status and timing information.
        
        Args:
            status (str): The status of the ETL process ('succeeded' or 'failed').
            error_message (str, optional): The error message if the ETL process failed.
        """
        etl_end_time = datetime.now()
        etl_processing_time = (etl_end_time - self.etl_start_time).total_seconds()

        self.etl_pipeline_response_dict.update({
            'status': status,
            'etl_end_time': str(etl_end_time),
            'etl_processing_time': etl_processing_time
        })
        
        if error_message:
            self.etl_pipeline_response_dict.update({'error_message': error_message})

        logging.info(f"ETL process {status}. Response: {self.etl_pipeline_response_dict}")

    def handle_success(self, should_exit: bool = False):
        """
        Handles the success scenario of the ETL process.
        
        Args:
            should_exit (bool): Whether to exit the notebook after handling success.
        """
        self.update_response(status="succeeded")
        if should_exit:
            self.notebook_exit_response()

    def handle_error(self, should_exit: bool = False):
        """
        Handles the error scenario of the ETL process.
        
        Args:
            should_exit (bool): Whether to exit the notebook after handling error.
        """
        exc_type, exc_value, _= sys.exc_info()
        error_message = str(exc_value)  # Capture the exception message
        
        if exc_value is not None:  # Ensure there's an actual error to handle
            error_info = {
                        "error_message": error_message,
                        "detailed_error_info": traceback.format_exc()
                        }
            self.update_response(status="failed", error_message=error_info)

            if should_exit:
                self.notebook_exit_response()


    def notebook_exit_response(self):
        """
        Exits the notebook with the ETL pipeline response if dbutils is available.
        Serializes the ETL pipeline response dictionary to a JSON string to avoid serialization errors.
        """

        if self.dbutils:
            try:
                # Prepare the dictionary for JSON serialization
                response_json = json.dumps(self.etl_pipeline_response_dict)               
                self.dbutils.notebook.exit(response_json)
            except Exception as e:
                print(f"Warning: Exit Message not Serialized properly: {e}")
        else:
            print("dbutils not available. Exiting notebook skipped.")
