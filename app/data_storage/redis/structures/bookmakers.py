import redis


class Bookmakers:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def get(self, bookmaker: str) -> str:
        return self.__r.get(f'bookmakers:{bookmaker}')

    def store(self, bookmaker: str, default_odds: str) -> None:
        self.__r.set(f'bookmakers:{bookmaker}', default_odds)