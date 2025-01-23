


class BaseManager:
    def __init__(self, configs: dict):
        self.configs = configs

    def log_message(self, e: Exception = None, level: str = 'EXCEPTION', message: str = None):
        """
        Logs a message with the specified level.

        Args:
            e (Exception, optional): The exception to log. Defaults to None.
            level (str, optional): The log level. Defaults to 'EXCEPTION'.
            message (str, optional): The log message. Defaults to None.
        """
        level = level.lower()
        message = message or str(e)

        if level == 'info':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ℹ️', message, 'ℹ️')

        if level == 'warning':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ⚠️', message, '⚠️')

        if level == 'error':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ❌', message, '❌')

        if level == 'exception':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ❌', message, '❌')