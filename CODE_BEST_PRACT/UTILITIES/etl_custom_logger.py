import logging
# file location -> adls
# make default to info 
class CustomLogger(object):
    _LOG = None

    def __init__(self, spark=None):
        self.spark = spark

    def __create_logger(self, log_level="INFO", log_file=None):
   
        # set the logging format
        log_format = "%(asctime)s:%(levelname)s:%(message)s"

        if self.spark:
            # Use Spark log4j logging
            log4j_logger = self.spark._jvm.org.apache.log4j
            CustomLogger._LOG = log4j_logger.LogManager.getLogger("custom_logger")
            if log_level == "INFO":
                CustomLogger._LOG.setLevel(log4j_logger.Level.INFO)
            elif log_level == "ERROR":
                CustomLogger._LOG.setLevel(log4j_logger.Level.ERROR)
            elif log_level == "DEBUG":
                CustomLogger._LOG.setLevel(log4j_logger.Level.DEBUG)
            
        else:
            # Use Python logging
            CustomLogger._LOG = logging.getLogger('custom_logger')

            #####     ALL < TRACE < DEBUG < INFO < WARN < ERROR < FATAL < OFF
            #### based on env 

            # Set the logging level based on the user selection
            if log_level == "INFO":
                CustomLogger._LOG.setLevel(logging.INFO)
            elif log_level == "ERROR":
                CustomLogger._LOG.setLevel(logging.ERROR)
            elif log_level == "DEBUG":
                CustomLogger._LOG.setLevel(logging.DEBUG)
            
            # Create console handler and set level to log_level
            ch = logging.StreamHandler()
            ch.setLevel(log_level)

            # Create formatter and add it to the handler
            formatter = logging.Formatter(log_format, datefmt="%Y-%m-%d %H:%M:%S")
            ch.setFormatter(formatter)

            # Clearing the existing handler and adding the current handler to the logger
            CustomLogger._LOG.handlers.clear()
            CustomLogger._LOG.addHandler(ch)

            #  Avoiding logging messages are passed to the handlers of ancestor loggers.
            CustomLogger._LOG.propagate = False

            # If log_file is provided, also create a file handler
            if log_file:
                fh = logging.FileHandler(log_file)
                fh.setLevel(log_level)
                fh.setFormatter(formatter)
                CustomLogger._LOG.addHandler(fh)
        
        return CustomLogger._LOG

    def get_logger(self, log_level, log_file=None):
        """
        A static method called by other modules to initialize logger in
        their own module.
        
        Args:
        log_level (str): The logging level (INFO, ERROR, DEBUG).
        log_file (str, optional): The file to log to. If not provided, logs will be output to the console.
        
        Returns:
        logger: The configured logger object.
        """
        logger = self.__create_logger(log_level, log_file)
        
        # return the logger object
        return logger
