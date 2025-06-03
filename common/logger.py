import logging
import os
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime

import time


class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    Custom TimedRotatingFileHandler that inserts the date before the file extension.
    For example, it creates log files like 'name.2024-04-27.log'.
    """

    def __init__(
        self,
        filename,
        when="midnight",
        interval=1,
        backupCount=7,
        encoding="utf-8",
        utc=False,
        atTime=None,
    ):
        # Extract base filename and extension
        self.base_filename, self.ext = os.path.splitext(filename)
        super().__init__(
            self.base_filename + self.ext,
            when,
            interval,
            backupCount,
            encoding,
            delay=False,
            utc=utc,
            atTime=atTime,
        )

    def rotation_filename(self, default_name):
        """
        Override the rotation_filename method to change the rotated file naming convention.
        """
        # Extract the timestamp from the default rotated filename
        # Default rotated filename is: base_filename.ext + "." + suffix
        # e.g., "name.log.2024-04-27"
        timestamp = default_name.split(self.base_filename + self.ext + ".")[-1]

        # Construct the new rotated filename: base_filename + "." + timestamp + ext
        new_filename = f"{self.base_filename}.{timestamp}{self.ext}"
        return new_filename


def get_logger(name, log_dir, level=logging.INFO):
    """
    Configures and returns a logger that saves logs into a directory organized by date.

    Args:
        name (str): The name of the logger.
        log_dir (str): The directory where log files will be stored.
        level (int): Logging level (e.g., logging.DEBUG, logging.INFO).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent adding multiple handlers to the logger
    if not logger.handlers:
        # Ensure the log directory exists
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Define the log file path
        log_filename = os.path.join(log_dir, f"{name}.log")

        # Create CustomTimedRotatingFileHandler
        handler = CustomTimedRotatingFileHandler(
            filename=log_filename,
            when="midnight",
            interval=1,
            backupCount=7,
            encoding="utf-8",
            utc=False,  # Set to True if you want to use UTC time
        )
        handler.suffix = (
            "%Y-%m-%d"  # This suffix is used in the default rotation_filename method
        )

        # Define log message format
        formatter = Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)

        # Add handler to the logger
        logger.addHandler(handler)

        # Optionally, add a StreamHandler to output logs to console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


if __name__ == "__main__":
    import time

    # Example usage
    logger = get_logger("myapp", "/home/pyspark_event/logs", logging.DEBUG)

    for i in range(100):
        logger.info(f"Logging message {i}")
        time.sleep(1)  # Sleep to simulate time passing
