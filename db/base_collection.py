from motor.motor_asyncio import AsyncIOMotorDatabase


class BaseCollection:

    def __init__(self, collection_name: str, db: AsyncIOMotorDatabase):
        self.db = db
        self.collection = self.db[collection_name]

        self.logging_name = collection_name.replace('_', ' ').title().replace(' ', '')

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
            print(f'[Storage] [{self.logging_name}]: ℹ️', message, 'ℹ️')

        if level == 'warning':
            print(f'[Storage] [{self.logging_name}]: ⚠️', message, '⚠️')

        if level == 'error':
            print(f'[Storage] [{self.logging_name}]: ❌', message, '❌')

        if level == 'exception':
            print(f'[Storage] [{self.logging_name}]: ❌', message, '❌')