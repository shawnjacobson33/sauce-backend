def moneyline(data: dict, subject: str) -> int:
    """
    Determines the moneyline result for a given game and subject team.

    Args:
        data (dict): The game data containing scores and team information.
        subject (str): The team for which the moneyline is being calculated.

    Returns:
        int: 1 if the subject team wins, 0 otherwise.
    """
    if data['home_team'] == subject:
        return 1 if data['home_score'] > data['away_score'] else 0
    else:
        return 1 if data['away_score'] > data['home_score'] else 0


def spread(data: dict, subject: str) -> int:
    """
    Calculates the spread for a given game and subject team.

    Args:
        data (dict): The game data containing scores and team information.
        subject (str): The team for which the spread is being calculated.

    Returns:
        int: The spread value.
    """
    if data['home_team'] == subject:
        return data['home_score'] - data['away_score']
    else:
        return data['away_score'] - data['home_score']

PERIOD_MAP = {
    '1Q': [ 1 ],
    '2Q': [ 2 ],
    '1H': [ 1, 2 ],
    '3Q': [ 3 ],
    '4Q': [ 4 ],
    '2H': [ 3, 4 ],
}

DOUBLES_STATS = ['points', 'rebounds', 'assists', 'steals', 'blocks']

SPECIAL_MARKET_MAP = {
    'Moneyline': moneyline,
    'Spread': spread,
    'Total': lambda data, _: data['home_score'] + data['away_score'],
    'Double Doubles': lambda data, _: 1 if ([stat >= 10 for stat_name, stat in data.items()
                                    if stat_name in DOUBLES_STATS].count(True) >= 2) else 0,
    'Triple Doubles': lambda data, _: 1 if ([stat >= 10 for stat_name, stat in data.items()
                                    if stat_name in DOUBLES_STATS].count(True) >= 3) else 0,
}


class GameStatsDict(dict):
    """
    A dictionary subclass for handling box score data with special market calculations.
    """

    def __init__(self, iterable=None, **kwargs):
        """
        Initializes the BoxScoreDict with the given iterable and keyword arguments.

        Args:
            iterable (optional): An iterable to initialize the dictionary.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(iterable, **kwargs)

    def _get_compound_stat(self, key: str) -> int:
        """
        Calculates the compound stat for a given key.

        Args:
            key (str): The key to calculate the compound stat for.

        Returns:
            int: The compound stat value.
        """
        compound_stat = 0
        markets = key.split(' + ')
        for market in markets:
            if stat := self.get(market):
                compound_stat += stat

        return compound_stat

    def _get_period_stat_value(self, key: str) -> int:
        """
        Retrieves the value for a given key, with special handling for period stats.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            int: The value associated with the key.
        """
        # Ex: 1Q Moneyline
        if (stat_dict := super().get(key[2:])) in SPECIAL_MARKET_MAP:
            return SPECIAL_MARKET_MAP[stat_dict](self, key[:2])

        stat_value = 0
        for i in PERIOD_MAP[key[:2]]:
            stat_value += stat_dict['records'][i - 1]

        return stat_value

    def get(self, key: str, subject: str = None) -> int:
        """
        Retrieves the value for a given key, with special handling for market calculations.

        Args:
            key (str): The key to retrieve the value for.
            subject (str, optional): The subject team for special market calculations.

        Returns:
            dict: The value associated with the key, or the result of the special market calculation.
        """

        stat_val = 0
        for market in key.split(' + '):
            if market[:2] in PERIOD_MAP:
                stat_val += self._get_period_stat_value(market, game)
            else:
                stat_val += super().get(market)



        return super().get(key)