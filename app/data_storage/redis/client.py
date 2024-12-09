import redis


class Client:
    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.r = redis.Redis(host=host, port=port)

    def reset_auto_ids(self, name: str) -> None:
        self.r.set(f'{name}:auto:id', 0)