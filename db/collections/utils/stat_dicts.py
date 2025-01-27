PERIOD_MAP = {
    'NBA': {
        '1Q': [1],
        '2Q': [2],
        '1H': [1, 2],
        '3Q': [3],
        '4Q': [4],
        '2H': [3, 4],
    },
    'NCAAM': {
        '1H': [1],
        '2H': [2],
    }
}


class GameStatsDict:
    """
    A class for calculating various game statistics based on the game data.
    """

    def __init__(self, game: dict):
        """
        Initializes the GameStatsDict with the given game data.

        Args:
            game (dict): The game data containing scores and team information.
        """
        self.game = game

    @staticmethod
    def _get_score(scores: dict, period_num: int) -> int:
        """
        Retrieves the score for a given period.

        Args:
            scores (dict): The game data containing scores and team information.
            period_num (int): The period number.

        Returns:
            int: The score for the given period.
        """
        if period_num:
            for period_score in scores['periods']:
                if period_score['period'] == period_num:
                    return period_score['score']
        else:
            return scores['total']

    def _get_moneyline(self, game: dict, subject: str, period_num: int = None, is_half: bool = False) -> int:
        """
        Determines the moneyline result for a given game and subject team.

        Args:
            game (dict): The game data containing scores and team information.
            subject (str): The team for which the moneyline is being calculated.
            period_num (int, optional): The period number for the moneyline calculation. Defaults to None.
            is_half (bool, optional): Whether the moneyline is for the first or second half. Defaults to False.

        Returns:
            int: 1 if the subject team wins, 0 otherwise.
        """
        scores = game['scores']
        home_scores, away_scores = scores['home'], scores['away']
        home_score = self._get_score(home_scores, period_num)
        away_score = self._get_score(away_scores, period_num)
        result = 1 if not is_half else 0.5  # for '1H Moneyline' and '2H Moneyline'
        if game['home_team'] == subject:
            return result if home_score > away_score else 0
        else:
            return result if away_score > home_score else 0

    def _get_spread(self, game: dict, subject: str, period_num: int = None) -> int:
        """
        Calculates the spread for a given game and subject team.

        Args:
            game (dict): The game data containing scores and team information.
            subject (str): The team for which the spread is being calculated.
            period_num (int, optional): The period number for the spread calculation. Defaults to None.

        Returns:
            int: The spread value.
        """
        scores = game['scores']
        home_scores, away_scores = scores['home'], scores['away']
        home_score = self._get_score(home_scores, period_num)
        away_score = self._get_score(away_scores, period_num)
        if game['home_team'] == subject:
            return home_score - away_score
        else:
            return away_score - home_score

    def _get_total(self, game: dict, period_num: int = None) -> int:
        """
        Calculates the total for a given game.

        Args:
            game (dict): The game data containing scores and team information.
            period_num (int, optional): The period number for the total calculation. Defaults to None.

        Returns:
            int: The total value.
        """
        scores = game['scores']
        home_scores, away_scores = scores['home'], scores['away']
        home_score = self._get_score(home_scores, period_num)
        away_score = self._get_score(away_scores, period_num)
        return home_score + away_score

    def _get_period_stat_value(self, market: str, period_nums: list[int], league: str = None) -> int:
        """
        Retrieves the value for a given market, with special handling for period stats.

        Args:
            market (str): The market to retrieve the value for.
            period_nums (list[int]): The list of period numbers to consider.

        Returns:
            int: The value associated with the market.
        """
        stat_value = 0
        for i in period_nums:
            if 'Moneyline' in market:
                is_half = (((len(period_nums) == 2) and (league in { 'NBA' })) or
                           ((len(period_nums) == 1) and (league in { 'NCAAM' })))  # because NCAAM only has halves
                stat_value += self._get_moneyline(self.game, market, i, is_half=is_half)
            elif 'Spread' in market:
                stat_value += self._get_spread(self.game, market, i)
            elif 'Total' in market:
                stat_value += self._get_total(self.game, i)

        return stat_value

    def get(self, market: str, league: str, subject: str = None) -> int | None:
        """
        Retrieves the value for a given market, with special handling for period stats.

        Args:
            market (str): The market to retrieve the value for.
            league (str): The league for the game.
            subject (str, optional): The subject team for special market calculations. Defaults to None.

        Returns:
            int | None: The value associated with the market, or None if the market is not found.
        """
        # TODO: TEMPORARY FIX
        market = market.replace(' Quarter', 'Q').replace(' Half', 'H').replace('1st', '1').replace(' 2nd', '2')
        if period_nums := PERIOD_MAP[league].get(market[:2]):
            return self._get_period_stat_value(market, period_nums, league=league)

        if 'Moneyline' in market:
            return self._get_moneyline(self.game, subject)
        elif 'Spread' in market:
            return self._get_spread(self.game, subject)
        elif 'Total' in market:
            return self._get_total(self.game)


DOUBLES_STATS = ['Points', 'Rebounds', 'Assists', 'Steals', 'Blocks']

PLAYER_PROPS_MARKET_MAP = {
    'Double Doubles': lambda box_score: 1 if ([stat_dict['total'] >= 10 for stat_label, stat_dict in box_score.items()
                                               if stat_label in DOUBLES_STATS].count(True) >= 2) else 0,
    'Triple Doubles': lambda box_score: 1 if ([stat_dict['total'] >= 10 for stat_label, stat_dict in box_score.items()
                                               if stat_label in DOUBLES_STATS].count(True) >= 3) else 0,
}


class PlayerStatsDict:
    """
    A class for handling player box score data with special market calculations.
    """

    def __init__(self, box_score: dict):
        """
        Initializes the PlayerStatsDict with the given box score data.

        Args:
            box_score (dict): The box score data.
        """
        self.box_score = box_score

    def _get_period_stat_value(self, key: str) -> int:
        """
        Retrieves the value for a given key, with special handling for period stats.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            int: The value associated with the key.
        """
        stat_value = 0
        stat_dict = self.box_score.get(key)
        for i in PERIOD_MAP[key[:2]]:
            stat_value += stat_dict['periods'][i - 1]

        return stat_value

    def get(self, key: str) -> int:
        """
        Retrieves the value for a given key, with special handling for market calculations.

        Args:
            key (str): The key to retrieve the value for.

        Returns:
            int: The value associated with the key, or the result of the special market calculation.
        """
        if key in PLAYER_PROPS_MARKET_MAP:
            return PLAYER_PROPS_MARKET_MAP[key](self.box_score)

        stat_val = 0
        for market in key.split(' + '):
            if key[:2] in PERIOD_MAP:
                stat_val += self._get_period_stat_value(market)
            else:
                stat_val += self.box_score[market]['total']

        return stat_val