import asyncio
from datetime import datetime
from typing import Optional, Union

from bson import ObjectId

from app.product_data.data_collection.plugs.bookmakers import utils
from app.product_data.data_collection.plugs.utils import IN_SEASON_LEAGUES, LEAGUE_SPORT_MAP


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


def extract_game_id(data: dict) -> Optional[str]:
    # for each dictionary in data's providers, if they exist
    for provider_data in data.get('providers', []):
        # will get the first occurrence of id for a provider with the name 'nix'
        if provider_data.get('name') == 'nix':
            # return the game id from the provider data if it exists
            return provider_data.get('id')


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[Union[ObjectId, str], str]]:
    # gets and cleans the market if it exists
    if market_name := data.get('statistic'):
        # create a market object
        market_obj = utils.Market(market_name, league=league)
        # gets the market id or log message
        market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name


def extract_position(data: dict) -> Optional[str]:
    # gets the position of a player if it exists and the cleans it.
    if (position_data := data.get('position')) and (position := position_data.get('title')):
        # return the cleaned player's position
        return utils.clean_position(position)


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[Union[ObjectId, str], str]]:
    # gets the player name, if it exists then keep going
    if subject_name := data.get('name'):
        # extract some player attributes
        subject_team, position = data.get('team'), extract_position(data)
        # create a subject object
        subject_obj = utils.Subject(subject_name, league, team=subject_team, position=position)
        # gets the subject id or log message
        subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
        # return both subject id search result and cleaned subject
        return subject_id, subject_name


def extract_label(data: dict) -> Optional[str]:
    # get the condition for the prop line, if exists keep going
    if condition := data.get('condition'):
        # if equals 1 then label type for the prop line is Over, else Under.
        return 'Over' if condition == 1 else 'Under'


class BetOnline(utils.BookmakerPlug):
    """
    A class to collect and process player prop lines from BetOnline.

    This class interacts with the BetOnline API, fetching games and parsing player
    prop betting data. The resulting data is cleaned, standardized, and stored in
    a shared data structure for further processing.

    Attributes:
        bookmaker_info (utils.Bookmaker): The bookmaker object containing BetOnline details.
        batch_id (str): Identifier for the current data collection batch.

    Methods:
        start() -> None:
            Initiates the data collection process, fetching game data asynchronously.

        _parse_games(response, league: str) -> None:
            Processes game data and initiates requests for player prop lines.

        _parse_lines(response, league: str) -> None:
            Processes and cleans player prop line data, updating shared prop line structures.
    """

    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # tracks requests to complete
        tasks = []
        # these are valid in-season leagues
        for league in LEAGUES:
            # get url for request to get games
            url = utils.get_url(self.bookmaker_info.name, name='games')
            # get headers for request to get games
            headers = utils.get_headers(self.bookmaker_info.name, name='games')
            # get params for request to get games
            params = utils.get_params(self.bookmaker_info.name, name='games', var_1=league)
            # clean the league name after setting params
            cleaned_league = utils.clean_league(league)
            # add the request task for this market to tasks
            tasks.append(self.req_mngr.get(url, self._parse_games, cleaned_league, headers=headers, params=params))

        # asynchronously collect games
        await asyncio.gather(*tasks)

    async def _parse_games(self, response, league: str) -> None:
        # get the response data as json, if exists keep executing
        if json_data := response.json():
            # tracks requests to complete
            tasks = []
            # data about games
            for game_data in json_data:
                # extract the game id from the dictionary, if exists keep going
                if game_id := extract_game_id(game_data):
                    # get url for request to get prop lines
                    url = utils.get_url(self.bookmaker_info.name)
                    # get headers for request to get prop lines
                    headers = utils.get_headers(self.bookmaker_info.name)
                    # gets stored markets to include in the params of url
                    league_markets = MARKETS.get(LEAGUE_SPORT_MAP.get(league))
                    # for every market
                    for market in league_markets:
                        # get params for request to get prop lines
                        params = utils.get_params(self.bookmaker_info.name, var_1=game_id, var_2=market)
                        # add the request task for this market to tasks
                        tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=headers, params=params))

            # asynchronously collect prop lines
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # get the response data as json, if exists keep going
        if json_data := response.json():
            # to track the leagues being collected
            self.metrics.add_league(league)
            # for each prop line in the response data
            for prop_line_data in json_data:
                # get the market id from the db and extract the market name from the dictionary
                market_id, market_name = extract_market(self.bookmaker_info.name, prop_line_data, league)
                # if both exist then keep going
                if market_id and market_name:
                    # to track the markets being collected
                    self.metrics.add_market((league, market_name))
                    # for every player in the prop line
                    for player_data in prop_line_data.get('players', []):
                        # get the subject id from the db and extract the subject name from dictionary
                        subject_id, subject_name = extract_subject(self.bookmaker_info.name, player_data, league)
                        # if both exist then keep going
                        if subject_id and subject_name:
                            # to track the subjects being collected
                            self.metrics.add_subject((league, subject_name))
                            # for all markets that the player has
                            for market_data in player_data.get('markets', []):
                                # market must exist and active on BetOnline
                                if market_data.get('isActive') and market_data.get('isActual'):
                                    # get numeric over/under line and odds (decimal) from dictionary, if both exist keep going
                                    if (line := market_data.get('value')) and (odds := market_data.get('odds')):
                                        # extract the over/under label from the dictionary
                                        if label := extract_label(market_data):
                                            # calculate the implied probability for the prop line using the odds
                                            implied_prob = 1 / float(odds)
                                            # update shared data
                                            self.update_betting_lines({
                                                'batch_id': self.batch_id,
                                                'time_processed': str(datetime.now()),
                                                'league': league,
                                                'market_category': 'player_props',
                                                'market_id': str(market_id),
                                                'market': market_name,
                                                'subject_id': str(subject_id),
                                                'subject': subject_name,
                                                'bookmaker': self.bookmaker_info.name,
                                                'label': label,
                                                'line': line,
                                                'odds': odds,
                                                'implied_prob': implied_prob
                                            })


if __name__ == "__main__":
    utils.run(BetOnline)

