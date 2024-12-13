import redis


class AutoId:
    def __init__(self, r: redis.Redis, name: str):
        self.__r = r
        self.name = f'{name}:auto:id'

    def reset(self) -> None:
        """Resets the unique id field back to 0"""
        self.__r.set(self.name, 0)

    def decrement(self) -> None:
        self.__r.decrby(self.name)

    def generate(self) -> str:
        new_id = self.__r.incrby(self.name)
        return f'{self.name.split(':')[0][:-1]}:{new_id}'