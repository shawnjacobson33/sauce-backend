import logging
import os


# Custom filter to direct DEBUG messages based on keywords
class KeywordFilter(logging.Filter):
    def __init__(self, keywords):
        super().__init__()
        self.keywords = keywords

    def filter(self, record):
        # Check if any keyword is in the log message
        for keyword in self.keywords:
            if keyword in record.getMessage():
                return True  # Allow the log record
        return False  # Disallow the log record


def create_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    return logger


def create_handler(bookmaker_name: str, handler_name: str) -> logging.FileHandler:
    # create custom file path
    file_path = f'utils/reporting/reports/{bookmaker_name}/{handler_name}.txt'
    # create any directories if they don't exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # create the file handler
    handler = logging.FileHandler(file_path, mode='w')  # overwrites previous content
    handler.setLevel(logging.DEBUG)
    handler.addFilter(KeywordFilter(keywords=[handler_name[:-1].upper()]))
    # return the configured file handler
    return handler


def add_handler(logger: logging.Logger, bookmaker_name: str, handler_name: str) -> logging.Logger:
    # create a custom file handler
    handler = create_handler(bookmaker_name, handler_name)
    # add the file handler to the logger
    logger.addHandler(handler)
    # return the configured logger
    return logger


def setup_reporter(bookmaker_name: str, content: list[str]) -> logging.Logger:
    # get a base logger instance
    logger = create_logger(bookmaker_name)
    # for each area to log
    for content_to_log in content:
        # add a custom file handler to log desired content
        logger = add_handler(logger, bookmaker_name, content_to_log)

    # return the configured logger
    return logger
