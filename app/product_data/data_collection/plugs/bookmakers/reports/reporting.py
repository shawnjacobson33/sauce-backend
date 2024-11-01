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


def add_handlers(logger: logging.Logger, name: str) -> logging.Logger:
    # make sure any intermediary directories exist, if not then create
    os.makedirs(os.path.dirname(f'reports/logs/{name}/metrics.txt'), exist_ok=True)
    os.makedirs(os.path.dirname(f'reports/logs/{name}/markets.txt'), exist_ok=True)
    os.makedirs(os.path.dirname(f'reports/logs/{name}/subjects.txt'), exist_ok=True)
    # configure a metrics file handler, set level to debug Messages and above, apply a filter to log metric mess. to file
    metric_handler = logging.FileHandler(f'reports/logs/{name}/metrics.txt')
    metric_handler.setLevel(logging.DEBUG)
    metric_handler.addFilter(KeywordFilter(keywords=['METRIC']))
    # configure a markets file handler, set level to debug Messages and above, apply a filter to log market mess. to file
    markets_handler = logging.FileHandler(f'reports/logs/{name}/markets.txt')
    markets_handler.setLevel(logging.DEBUG)
    markets_handler.addFilter(KeywordFilter(keywords=['MARKET']))
    # configure a subjects file handler, set level to debug Messages and above, apply a filter to log subject mess. to file
    subjects_handler = logging.FileHandler(f'reports/logs/{name}/subjects.txt')
    subjects_handler.setLevel(logging.DEBUG)
    subjects_handler.addFilter(KeywordFilter(keywords=['SUBJECT']))
    # add all the file handlers
    logger.addHandler(metric_handler)
    logger.addHandler(markets_handler)
    logger.addHandler(subjects_handler)
    # return the configured logger
    return logger


def setup_reporter(name: str) -> logging.Logger:
    logger = create_logger(name)
    logger = add_handlers(logger, name)
    return logger
