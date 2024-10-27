import asyncio
from datetime import datetime

from app.product_data.data_collection.shared_data import PropLines
from app.product_data.data_collection.plugs.helpers.misc import run
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.constants import LEAGUE_SPORT_MAP, IN_SEASON_LEAGUES
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id, clean_market, \
    clean_subject, clean_league, clean_position

# pre-defined markets that will be used for url params
MARKETS = {
    'Football': [
        'Passing%2520Yards',
        'Pass%2520Completions',
        'Passing%2520TDs',
        'Pass%2520Attempts',
        'Rushing%2520Yards',
        'Carries',
        'Receiving%2520Yards',
        'Receptions',
        'Pass%2520Interceptions',
        'Tackles',
        'Sacks',
        'Interceptions'
    ],
    'Basketball': [
        'Points',
        'Rebounds',
        'Assists',
        'Pts%2520%252B%2520Reb%2520%252B%2520Ast',
        'Three%2520Point%2520Field%2520Goals%2520Made'
    ],
    'Baseball': [
        'Strikeouts',
        'Hits%2520Allowed',
        'Earned%2520Runs',
        'Hits',
        'Runs%2520%252B%2520RBIs',
        'Total%2520Bases',
        'Stolen%2520Bases'
    ],
    'Ice Hockey': [
        'Points',
        'Shots%2520On%2520Goal'
    ]
}
# formatting consistency
LEAGUE_MAP = {
    'NCAAF': 'NCAAFB'
}
# get in-season leagues
LEAGUES = [LEAGUE_MAP.get(league, league).lower() for league in IN_SEASON_LEAGUES]


class BetOnline(Plug):
    """
    A class to collect and process player prop lines from BetOnline.

    This class interacts with the BetOnline API, fetching games and parsing player
    prop betting data. The resulting data is cleaned, standardized, and stored in
    a shared data structure for further processing.

    Attributes:
        info (Bookmaker): The bookmaker object containing BetOnline details.
        batch_id (str): Identifier for the current data collection batch.
        req_mngr (RequestManager): Manages API requests and responses.
        data_size (int): Counter for the number of prop lines processed.

    Methods:
        start() -> None:
            Initiates the data collection process, fetching game data asynchronously.

        _parse_games(response, league: str) -> None:
            Processes game data and initiates requests for player prop lines.

        _parse_lines(response, league: str) -> None:
            Processes and cleans player prop line data, updating shared prop line structures.
    """

    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        super().__init__(info, batch_id, req_mngr)

    async def start(self) -> None:
        # tracks requests to complete
        tasks = []
        # these are valid in-season leagues
        for league in LEAGUES:
            # get url for request to get games
            url = self.req_packager.get_url(name='games')
            # get headers for request to get games
            headers = self.req_packager.get_headers(name='games')
            # get params for request to get games
            params = self.req_packager.get_params(name='games', var_1=league)
            # add the request task for this market to tasks
            tasks.append(self.req_mngr.get(url, self._parse_games, league, headers=headers, params=params))

        # asynchronously collect games
        await asyncio.gather(*tasks)

    async def _parse_games(self, response, league: str) -> None:
        # tracks requests to complete
        tasks = []
        # data about games
        for game in response.json():
            # get providers
            providers = game.get('providers', [])
            # will get the first occurrence of id for a provider with the name 'nix'
            if game_id := next((provider.get('id') for provider in providers if provider.get('name') == 'nix'), None):
                # clean the league
                league = clean_league(league)
                # get url for request to get prop lines
                url = self.req_packager.get_url()
                # get headers for request to get prop lines
                headers = self.req_packager.get_headers()
                # gets stored markets to include in the params of url
                league_markets = MARKETS.get(LEAGUE_SPORT_MAP.get(league))
                # for every market
                for market in league_markets:
                    # get params for request to get prop lines
                    params = self.req_packager.get_params(var_1=game_id, var_2=market)
                    # add the request task for this market to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=headers, params=params))

        # asynchronously collect prop lines
        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # for each prop line in the response data
        for prop_line in response.json():
            # gets and cleans the market if it exists
            if market := prop_line.get('statistic'):
                market = clean_market(market)

            # gets the market id if market exists
            market_id = get_market_id(Market(market, league)) if market else None
            # for every player in the prop line
            for player in prop_line.get('players', []):
                # gets the position of a player if it exists and the cleans it.
                if position := position_data.get('title') if (position_data := player.get('position')) else None:
                    position = clean_position(position)

                # gets a cleans player name if it exists
                if subject := player.get('name'):
                    subject = clean_subject(subject)

                # gets team that the player is on
                subject_team = player.get('team')
                # gets the subject id of the player
                subject_id = get_subject_id(Subject(subject, league, subject_team, position)) if subject else None
                # for all markets that the player has
                for market_data in player.get('markets', []):
                    # market must exist and active on BetOnline
                    if market_data.get('isActive') and market_data.get('isActual'):
                        # get numeric prop line
                        line = market_data.get('value')
                        # get the odds in decimal format of the prop line
                        decimal_odds = market_data.get('odds')
                        # get condition and if it exists and equals 1 then label type for the prop line is Over, else Under.
                        label = ('Over' if condition == 1 else 'Under') if (condition := market_data.get('condition')) else None
                        # update shared data...formatting bookmaker name for OddsShopper's contrasting formats
                        PropLines.update(''.join(self.info.name.split()).lower(), {
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market': market,
                            'subject_id': subject_id,
                            'subject': subject,
                            'bookmaker': self.info.name,
                            'label': label,
                            'line': line,
                            'odds': decimal_odds,
                            'implied_prob': round(1 / float(decimal_odds), 3)
                        })
                        # add one to the prop line count
                        self.data_size += 1


if __name__ == "__main__":
    asyncio.run(run(BetOnline))

