import os, logging
from logging import Logger, FileHandler, StreamHandler
from typing import Text, Tuple



# --------------------------------------------------------------
# Logger Setup.
# --------------------------------------------------------------

# Logging config. 
DEBUG = False
LOG_LEVEL = logging.DEBUG if DEBUG else logging.WARNING 
LOG_FORMATTER = logging.Formatter("[%(asctime)s] %(levelname)s – %(name)s | %(message)s")


# Logger setup. 
def setup_logger(logger: Logger, log_filename: Text) -> Tuple[Logger, FileHandler, StreamHandler]: 
    # Create a new folder for the log filename if it doesn't exist. 
    if not os.path.exists('logs/runtime'): 
        os.makedirs('logs/runtime')

    logger.setLevel(LOG_LEVEL)

    # Log config for output in the log file. 
    file_handler = FileHandler(log_filename)
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(LOG_FORMATTER)

    # Log config for terminal output. 
    stream_handler = StreamHandler()
    stream_handler.setLevel(LOG_LEVEL)
    stream_handler.setFormatter(LOG_FORMATTER)

    # Add custom logging configuration for this module. 
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger, file_handler, stream_handler
