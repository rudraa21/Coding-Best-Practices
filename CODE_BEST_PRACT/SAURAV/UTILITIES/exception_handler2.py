import logging
import json
import sys
import traceback
from datetime import datetime
from typing import Dict, Any, Optional

class DatabricksExceptionManager:
    """
    A custom class for handling exceptions and logging process outcomes in Databricks notebooks.

    Attributes:
        result_dict (Dict[str, Any]): A dictionary to store process status, start and end times, and error messages.
        spark (SparkSession): Optional Spark session used within the notebook.
        dbutils (object): Optional Databricks utilities object.
        start_time (datetime): Records the start time of the notebook's execution.
    """

    def __init__(self, result_dict: Dict[str, Any], 
                 spark: Optional['SparkSession'] = None, 
                 dbutils: Optional[object] = None):
        self.result_dict = result_dict
        self.spark = spark if spark else self._init_spark_session()
        self.dbutils = dbutils if dbutils else self._init_dbutils()
        self.start_time = datetime.now()
        self.result_dict['start_time'] = str(self.start_time)

    def _init_spark_session(self):
        # Initializes Spark session if not provided
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    def _init_dbutils(self):
        # Tries to initialize dbutils for notebook control if not provided
        try:
            import IPython
            return IPython.get_ipython().user_ns["dbutils"]
        except KeyError:
            logging.warning("dbutils not available. Notebook exit might not work.")
            return None

    def log_status(self, status: str, error_info: Optional[str] = None):
        """
        Logs and updates the status of the process, along with any error information if applicable.

        Args:
            status (str): 'success' or 'failure' to indicate the notebook's result.
            error_info (str, optional): Detailed error message in case of failure.
        """
        end_time = datetime.now()
        process_duration = (end_time - self.start_time).total_seconds()

        self.result_dict.update({
            'status': status,
            'end_time': str(end_time),
            'duration_seconds': process_duration
        })

        if error_info:
            self.result_dict['error'] = error_info

        logging.info(f"Notebook process {status}. Details: {self.result_dict}")

    def process_success(self, exit_notebook: bool = False):
        """
        Handles the successful execution of the process and exits the notebook if specified.

        Args:
            exit_notebook (bool): Whether to exit the notebook after handling success.
        """
        self.log_status('success')
        if exit_notebook:
            self.notebook_exit()

    def process_failure(self, exit_notebook: bool = False):
        """
        Handles failure by logging the error and optionally exiting the notebook.

        Args:
            exit_notebook (bool): Whether to exit the notebook after logging the failure.
        """
        exc_type, exc_value, _ = sys.exc_info()
        error_details = str(exc_value)

        if exc_value:
            full_error_trace = {
                "error_message": error_details,
                "traceback": traceback.format_exc()
            }
            self.log_status('failure', error_info=full_error_trace)

        if exit_notebook:
            self.notebook_exit()

    def notebook_exit(self):
        """
        Exits the notebook with the result_dict. Handles dbutils unavailability gracefully.
        """
        if self.dbutils:
            try:
                exit_json = json.dumps(self.result_dict)
                self.dbutils.notebook.exit(exit_json)
            except Exception as e:
                logging.error(f"Failed to exit notebook properly: {e}")
        else:
            logging.warning("dbutils not available; skipping notebook exit.")
