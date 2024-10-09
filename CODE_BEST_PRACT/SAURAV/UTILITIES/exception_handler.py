import logging
import json
import sys
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

class DatabricksErrorHandler:
    """
    A class for handling exceptions and managing responses in Databricks notebooks.

    Attributes:
        response_dict (Dict[str, Any]): Dictionary to store the status and information of the notebook.
        spark (SparkSession): The Spark session, initialized if not provided.
        dbutils (object): Databricks utility object, initialized if not provided.
        start_time (datetime): The start time of the notebook execution.
    """

    def __init__(self, response_dict: Dict[str, Any], 
                 spark: Optional['SparkSession'] = None, 
                 dbutils: Optional[object] = None):
        self.response_dict = response_dict
        self.spark = spark or self._get_spark_session()
        self.dbutils = dbutils or self._get_dbutils(self.spark)
        self.start_time = datetime.now()
        self.response_dict.update({'start_time': str(self.start_time)})

    def _get_spark_session(self):
        # Initialize Spark session if needed
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    def _get_dbutils(self, spark):
        # Initialize dbutils if needed
        try:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
            return dbutils
        except KeyError:
            logging.warning("dbutils not available. Proceeding without it.")
            return None

    def update_response(self, status: str, error_message: str = None):
        """
        Updates the response dictionary with the current status and timing information.
        
        Args:
            status (str): The status of the process ('succeeded' or 'failed').
            error_message (str, optional): The error message if the process failed.
        """
        end_time = datetime.now()
        processing_time = (end_time - self.start_time).total_seconds()

        self.response_dict.update({
            'status': status,
            'end_time': str(end_time),
            'processing_time': processing_time
        })

        if error_message:
            self.response_dict.update({'error_message': error_message})

        logging.info(f"Process {status}. Response: {self.response_dict}")

    def handle_success(self, should_exit: bool = False):
        """
        Handles the success scenario, updating the response and optionally exiting the notebook.
        
        Args:
            should_exit (bool): Whether to exit the notebook after success.
        """
        self.update_response(status="succeeded")
        if should_exit:
            self.notebook_exit_response()

    def handle_error(self, should_exit: bool = False):
        """
        Handles errors, updates the response with error details, and optionally exits the notebook.
        
        Args:
            should_exit (bool): Whether to exit the notebook after error.
        """
        exc_type, exc_value, _ = sys.exc_info()
        error_message = str(exc_value)

        if exc_value is not None:
            error_info = {
                "error_message": error_message,
                "detailed_error_info": traceback.format_exc()
            }
            self.update_response(status="failed", error_message=error_info)

            if should_exit:
                self.notebook_exit_response()

    def notebook_exit_response(self):
        """
        Exits the notebook with the response dictionary if dbutils is available.
        Converts the response to JSON format for notebook exit.
        """
        if self.dbutils:
            try:
                response_json = json.dumps(self.response_dict)
                self.dbutils.notebook.exit(response_json)
            except Exception as e:
                logging.error(f"Error during notebook exit: {e}")
        else:
            logging.warning("dbutils not available. Skipping notebook exit.")

