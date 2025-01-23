


class BaseManager:

    def __init__(self, domain: str, configs: dict):
        self.domain = domain
        self.configs = configs

    def log_message(self, message: str, level: str = 'EXCEPTION'):
        level = level.lower()

        if level == 'info':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ℹ️', message, 'ℹ️')

        if level == 'warning':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ⚠️', message, '⚠️')

        if level == 'error':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ‼️', message, '‼️')

        if level == 'exception':
            print(f'[{self.domain}Pipeline] [{self.domain}Manager]: ❌', message, '❌')