import logging
import os
from typing import Optional

class DatabricksLogger:
    """
    A utility class for creating a custom logger to log events, warnings, and errors in Databricks notebooks.

    Attributes:
        log_file (str): The file path to store the log, if applicable.
        logger (logging.Logger): The configured logger object.
    """

    def __init__(self, log_level: str = "INFO", log_file: Optional[str] = None):
        """
        Initializes the logger with the specified log level and optional log file.

        Args:
            log_level (str): The logging level ('INFO', 'ERROR', 'DEBUG'). Defaults to 'INFO'.
            log_file (str, optional): File path to save logs. If not provided, logs output to the console.
        """
        self.log_file = log_file
        self.logger = self._create_logger(log_level)

    def _create_logger(self, log_level: str):
        """
        Sets up the logger with the desired log level and log file configuration.

        Args:
            log_level (str): The logging level (INFO, ERROR, DEBUG).

        Returns:
            logging.Logger: The configured logger object.
        """
        logger = logging.getLogger('databricks_custom_logger')
        log_format = "%(asctime)s - %(levelname)s - %(message)s"

        # Set the logging level based on the input
        if log_level == "INFO":
            logger.setLevel(logging.INFO)
        elif log_level == "ERROR":
            logger.setLevel(logging.ERROR)
        elif log_level == "DEBUG":
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)  # Default to INFO if invalid log level is passed

        # Create formatter
        formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")

        # Clear any existing handlers
        logger.handlers.clear()

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Add file handler if log_file is provided
        if self.log_file:
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        logger.propagate = False  # Avoid propagating to ancestor loggers

        return logger

    def log_info(self, message: str):
        """Logs an informational message."""
        self.logger.info(message)

    def log_error(self, message: str):
        """Logs an error message."""
        self.logger.error(message)

    def log_debug(self, message: str):
        """Logs a debug message."""
        self.logger.debug(message)

    def log_warning(self, message: str):
        """Logs a warning message."""
        self.logger.warning(message)

# # Example usage:
# # Initialize the logger (with or without file logging)
# logger = DatabricksLogger(log_level="INFO", log_file="/dbfs/logs/databricks_log.log")

# # Log different types of messages
# logger.log_info("Starting the ETL pipeline.")
# logger.log_warning("Data volume might be higher than expected.")
# logger.log_error("Error encountered while reading from source.")
# logger.log_debug("Debugging information for detailed analysis.")
