
DOUBLES_STATS = ['points', 'rebounds', 'assists', 'steals', 'blocks']
SPECIAL_MARKET_TO_STAT_MAP = {
    'Double Doubles': lambda data: 1 if ([stat >= 10 for stat_name, stat in data.items()
                                    if stat_name in DOUBLES_STATS].count(True) >= 2) else 0,
    'Triple Doubles': lambda data: 1 if ([stat >= 10 for stat_name, stat in data.items()
                                    if stat_name in DOUBLES_STATS].count(True) >= 3) else 0,
}


class BoxScoreDict(dict):
    def __init__(self, iterable=None, **kwargs):
        super().__init__(iterable, **kwargs)

    def get(self, key: str) -> dict:
        if key in SPECIAL_MARKET_TO_STAT_MAP:
            special_market_stat = SPECIAL_MARKET_TO_STAT_MAP[key](self)
            return special_market_stat

        if ' + ' in key:
            compound_stat = 0
            markets = key.split(' + ')
            for market in markets:
                compound_stat += self.get(market)

            return compound_stat

        return super().get(key, {})