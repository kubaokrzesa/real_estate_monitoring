import logging
from termcolor import colored


class ColoredFormatter(logging.Formatter):
    COLORS = {
        'WARNING': 'yellow',
        'INFO': 'white',
        'DEBUG': 'green',
        'CRITICAL': 'red',
        'ERROR': 'red'
    }

    def format(self, record):
        log_message = super().format(record)
        return colored(log_message, self.COLORS.get(record.levelname))


class Logger:
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
        return self.logger