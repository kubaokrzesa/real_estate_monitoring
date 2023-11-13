import logging
from termcolor import colored


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter for logging, which adds color to log messages based on their severity.

    Attributes:
        COLORS (dict): Mapping of log level names to color names.
    """
    COLORS = {
        'WARNING': 'yellow',
        'INFO': 'white',
        'DEBUG': 'green',
        'CRITICAL': 'red',
        'ERROR': 'red'
    }

    def format(self, record: logging.LogRecord) -> str:
        """
        Formats the log record with color based on its level.

        Args:
            record (logging.LogRecord): The log record.

        Returns:
            str: The formatted log message.
        """
        log_message = super().format(record)
        return colored(log_message, self.COLORS.get(record.levelname))


class Logger:
    """
    Custom logger class that sets up a logger with a specific name and colored output.

    Attributes:
        logger (logging.Logger): The underlying logger instance.
    """
    def __init__(self, name: str) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        c_handler = logging.StreamHandler()
        c_handler.setLevel(logging.INFO)
        c_format = ColoredFormatter(
            "%(name)s -- %(filename)s -- %(levelname)s -- %(asctime)s -- %(message)s"
        )
        c_handler.setFormatter(c_format)
        self.logger.addHandler(c_handler)

    def get_logger(self) -> logging.Logger:
        """
        Returns the configured logger instance.

        Returns:
            logging.Logger: The logger instance.
        """
        return self.logger
