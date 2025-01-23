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


DOUBLES_STATS = ['points', 'rebounds', 'assists', 'steals', 'blocks']

SPECIAL_MARKET_TO_STAT_MAP = {
    'Moneyline': moneyline,
    'Spread': spread,
    'Total': lambda data, _: data['home_score'] + data['away_score'],
    'Double Doubles': lambda data, _: 1 if ([stat >= 10 for stat_name, stat in data.items()
                                    if stat_name in DOUBLES_STATS].count(True) >= 2) else 0,
    'Triple Doubles': lambda data, _: 1 if ([stat >= 10 for stat_name, stat in data.items()
                                    if stat_name in DOUBLES_STATS].count(True) >= 3) else 0,
}


class BoxScoreDict(dict):
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

    def get(self, key: str, subject: str = None) -> dict:
        """
        Retrieves the value for a given key, with special handling for market calculations.

        Args:
            key (str): The key to retrieve the value for.
            subject (str, optional): The subject team for special market calculations.

        Returns:
            dict: The value associated with the key, or the result of the special market calculation.
        """
        if key in SPECIAL_MARKET_TO_STAT_MAP:
            special_market_stat = SPECIAL_MARKET_TO_STAT_MAP[key](self, subject)
            return special_market_stat

        if ' + ' in key:
            compound_stat = 0
            markets = key.split(' + ')
            for market in markets:
                if stat := self.get(market):
                    compound_stat += stat

            return compound_stat

        return super().get(key)