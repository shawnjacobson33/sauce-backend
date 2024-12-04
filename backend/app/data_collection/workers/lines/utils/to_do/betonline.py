import asyncio
from collections import deque
from typing import Optional

from backend.app.data_collection.workers import utils as dc_utils
from backend.app.data_collection.workers.lines import utils as ln_utils


UNAVAILABLE_LEAGUES = ['NCAAM', 'NCAAW']
# pre-defined markets that will be used for url params
MARKETS = {
    'Football': {
        'NFL': [
            'Passing%2520Yards',
            'Pass%2520Completions',
            'Passing%2520TDs',
            'Pass%2520Attempts',
            'Rushing%2520Yards',
            'Carries',
            'Receiving%2520Yards',
            'Receptions',
            'Pass%2520Interceptions',
            'Sacks',
            'Interceptions'
        ],
        'NCAAFB': [
            'Passing%2520Yards',
            'Passing%2520TDs',
            'Rushing%2520Yards',
            'Receiving%2520Yards',
        ]
    },
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
# get in-season and available leagues
LEAGUES = [LEAGUE_MAP.get(league, league).lower() for league in dc_utils.IN_SEASON_LEAGUES if league not in UNAVAILABLE_LEAGUES]


def extract_game_id(data: dict) -> Optional[str]:
    # for each dictionary in data's providers, if they exist
    for provider_data in data.get('providers', []):
        # will get the first occurrence of id for a provider with the name 'nix'
        if provider_data.get('name') == 'nix':
            # return the game id from the provider data if it exists
            return provider_data.get('id')


def get_league_markets(league: str) -> list[str]:
    # get the sport
    sport = dc_utils.LEAGUE_SPORT_MAP.get(league, league)
    # gets stored markets to include in the params of url
    league_markets = MARKETS.get(sport, [])
    # if the sport is football then get the specific markets associated with NFL or NCAAFB
    if sport == 'Football':
        # get the markets
        league_markets = league_markets.get(league, [])

    # return the list of league markets
    return league_markets


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # gets and cleans the market if it exists
    if market_name := data.get('statistic'):
        # gets the market id or log message
        market = dc_utils.get_market(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_position(data: dict) -> Optional[str]:
    # gets the position of a player if it exists and the cleans it.
    if (position_data := data.get('position')) and (position := position_data.get('title')):
        # return the cleaned player's position
        return dc_utils.clean_position(position)


def extract_subject_team(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the subject's team name from data
    if subject_team := data.get('team'):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team(bookmaker_name, league, subject_team):
            # return the team id and team name
            return team_data


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # gets the player name, if it exists then keep going
    if subject_name := data.get('name'):
        # extract a dictionary including the team id from database and the cleaned subject team name
        team = extract_subject_team(bookmaker_name, data, league)
        # extract some player attributes
        position = extract_position(data)
        # gets the subject id and subject name
        subject = dc_utils.get_subject(bookmaker_name, league, subject_name, {'team': team, 'position': position})
        # return both subject id search result and cleaned subject
        return subject


def extract_label(data: dict) -> Optional[str]:
    # get the condition for the prop line, if exists keep going
    if condition := data.get('condition'):
        # if equals 1 then label type for the prop line is Over, else Under.
        return 'Over' if condition == 1 else 'Under'


class BetOnline(ln_utils.LinesRetriever):
    """
    A class to collect and process player prop lines from BetOnline.

    This class interacts with the BetOnline API, fetching games and parsing player
    prop betting data. The resulting data is cleaned, standardized, and stored in
    a shared data structure for further processing.

    Attributes:
        bookmaker_info (ln_utils.LinesSource): The bookmaker object containing BetOnline details.
        batch_id (str): Identifier for the current data collection batch.

    Methods:
        start() -> None:
            Initiates the data collection process, fetching game data asynchronously.

        _parse_games(response, league: str) -> None:
            Processes game data and initiates requests for player prop lines.

        _parse_lines(response, league: str) -> None:
            Processes and cleans player prop line data, updating shared prop line structures.
    """

    def __init__(self, batch_id: str, bookmaker: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(batch_id, bookmaker)

    async def retrieve(self) -> None:
        # tracks requests to complete
        tasks = []
        # these are valid in-season leagues
        for league in LEAGUES:
            # get url for request to get games
            url = ln_utils.get_url(self.name, name='games')
            # get headers for request to get games
            headers = ln_utils.get_headers(self.name, name='games')
            # get params for request to get games
            params = ln_utils.get_params(self.name, name='games', var_1=league)
            # clean the league name after setting params
            cleaned_league = dc_utils.clean_league(league)
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
                    url = ln_utils.get_url(self.name)
                    # get headers for request to get prop lines
                    headers = ln_utils.get_headers(self.name)
                    # for every market
                    for market in get_league_markets(league):
                        # get params for request to get prop lines
                        params = ln_utils.get_params(self.name, var_1=game_id, var_2=market)
                        # add the request task for this market to tasks
                        tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=headers, params=params))

            # asynchronously collect prop lines
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # get the response data as json, if exists keep going
        if json_data := response.json():
            # update shared leagues data
            dc_utils.RelevantData.update_relevant_leagues(league, self.name)
            # for each prop line in the response data
            for prop_line_data in json_data:
                # get the market id from the db and extract the market name from the dictionary
                if market := extract_market(self.name, prop_line_data, league):
                    # for every player in the prop line
                    for player_data in prop_line_data.get('players', []):
                        # get the subject id from the db and extract the subject name from dictionary
                        if subject := extract_subject(self.name, player_data, league):
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
                                            self.store({
                                                'batch_ids': deque([self.batch_id]),
                                                's_tstamp': str(datetime.now()),
                                                'league': league,
                                                'market_category': 'player_props',
                                                'market_id': market['id'],
                                                'market': market['name'],
                                                'subject_id': subject['id'],
                                                'subject': subject['name'],
                                                'bookmaker': self.name,
                                                'label': label,
                                                'line': line,
                                                'odds': odds,
                                                'implied_prob': implied_prob
                                            })
