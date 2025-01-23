


class BaseManager:

    def __init__(self, domain: str, configs: dict):
        self.domain = domain
        self.configs = configs

    def log_message(self, e: Exception = None, level: str = 'EXCEPTION', message: str = None):
        level = level.lower()

        message = message or str(e)

        if level == 'info':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ℹ️', message, 'ℹ️')

        if level == 'warning':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ⚠️', message, '⚠️')

        if level == 'error':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ❌', message, '❌')

        if level == 'exception':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ❌', message, '❌')