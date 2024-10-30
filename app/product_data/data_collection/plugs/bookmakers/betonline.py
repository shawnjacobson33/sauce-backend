import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.constants import IN_SEASON_LEAGUES, LEAGUE_SPORT_MAP
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.bookmakers.helpers import run, clean_market, clean_subject, clean_position, \
    clean_league

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


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # gets and cleans the market if it exists
    if market := data.get('statistic'):
        # clean the market name
        cleaned_market = clean_market(market, 'bet_online')
        # gets the market id if market exists
        if market_id := get_market_id(Market(cleaned_market, league)):
            # cast the market id to a string
            market_id = str(market_id)

        # return both market id and cleaned market
        return market_id, cleaned_market


def extract_position(data: dict) -> Optional[str]:
    # gets the position of a player if it exists and the cleans it.
    if (position_data := data.get('position')) and (position := position_data.get('title')):
        # return the cleaned player's position
        return clean_position(position)


def extract_subject(data: dict, league: str) -> Optional[tuple[str, str]]:
    # gets the player name, if it exists then keep going
    if subject := data.get('name'):
        # clean the subject name
        cleaned_subject = clean_subject(subject)
        # get the subject id from the db
        if subject_id := get_subject_id(Subject(cleaned_subject, league, team=data.get('team'), position=extract_position(data))):
            # cast the subject id to a string
            subject_id = str(subject_id)

        # returns the subject id from the db and the cleaned player name
        return subject_id, cleaned_subject


def extract_label(data: dict) -> Optional[str]:
    # get the condition for the prop line, if exists keep going
    if condition := data.get('condition'):
        # if equals 1 then label type for the prop line is Over, else Under.
        return 'Over' if condition == 1 else 'Under'


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
        # call parent class Plug
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
            tasks.append(self.req_mngr.get(url, self._parse_games, clean_league(league), headers=headers, params=params))

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
        # get the response data as json, if exists keep going
        if json_data := response.json():
            # for each prop line in the response data
            for prop_line_data in json_data:
                # get the market id from the db and extract the market name from the dictionary
                market_id, market = extract_market(prop_line_data, league)
                # if both exist then keep going
                if market_id and market:
                    # for every player in the prop line
                    for player_data in prop_line_data.get('players', []):
                        # get the subject id from the db and extract the subject name from dictionary
                        subject_id, subject = extract_subject(player_data, league)
                        # if both exist then keep going
                        if subject_id and subject:
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
                                            self.add_and_update({
                                                'batch_id': self.batch_id,
                                                'time_processed': str(datetime.now()),
                                                'league': league,
                                                'market_category': 'player_props',
                                                'market_id': market_id,
                                                'market': market,
                                                'subject_id': subject_id,
                                                'subject': subject,
                                                'bookmaker': self.info.name,
                                                'label': label,
                                                'line': line,
                                                'odds': odds,
                                                'implied_prob': implied_prob
                                            })


if __name__ == "__main__":
    asyncio.run(run(BetOnline))
    Plug.save_to_file()

