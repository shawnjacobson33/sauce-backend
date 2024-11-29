from datetime import datetime
import asyncio
from typing import Optional

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers import utils as bkm_utils


def extract_league(data: dict) -> Optional[str]:
    # get league name and the competition id, if both exist then execute
    if league := data.get('displayName'):
        # clean the league name
        cleaned_league = dc_utils.clean_league(league)
        # if league is not valid then skip
        if bkm_utils.is_league_valid(cleaned_league):
            # return league name
            return cleaned_league


def extract_market_map(data: dict) -> Optional[dict]:
    return {
        market.get('id'): market_data.get('name')
        for market_data in data.get('marketGroupMappings', [])
        if market_data.get('name')  # Ensure market_name exists
        for market in market_data.get('markets', [])
        if market.get('id')  # Ensure market_id exists
    }


def extract_market(bookmaker_name: str, data: dict, data_map: dict, league: str) -> Optional[dict[str, str]]:
    # get the market id from the response data, if that exists get the market name, if they both exist execute
    if (data_map_market_id := data.get('marketId')) and (market_name := data_map.get(data_map_market_id)):
        # check if the market is valid
        if bkm_utils.is_market_valid(market_name):
            # gets the market id or log message
            market = dc_utils.get_market(bookmaker_name, league, market_name)
            # return both market id search result and cleaned market
            return market


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists keep going
    if position := data.get('position'):
        # return the cleaned position
        return dc_utils.clean_position(position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the subject team name abbreviated, if exists keep going
    if abbr_team_name := data.get('teamAbbreviation'):
        # return the team id and team name
        return dc_utils.get_team(bookmaker_name, league, abbr_team_name.upper())


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict) -> Optional[dict[str, str]]:
    # get subject name from the data, if exists then execute
    if subject_name := data.get('playerName'):
        # # extract player attributes
        # position = extract_position(data)
        # gets the subject id or log message
        return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


def extract_label(data: dict) -> Optional[str]:
    # get the label, if exists keep going
    if label := data.get('lineType'):
        # return the label capitalized
        return label.title()


class Dabble(bkm_utils.LinesRetriever):
    """
    A class to collect and process player prop lines from the Dabble bookmaker.

    This class fetches competition and event data, as well as specific player prop lines, from the Dabble API.
    It asynchronously processes data in stages—cleaning, standardizing, and updating betting information into a
    shared structure for downstream use.

    Attributes:
        bookmaker_info (Bookmaker): Contains Dabble-specific information, such as the name and default payout odds.
        batch_id (str): An identifier for tracking the current data collection session.
        req_mngr (RequestManager): Manages asynchronous HTTP requests to the Dabble API, facilitating data retrieval
                                   and response handling.

    Methods:
        collect() -> None:
            Initiates the data collection process by requesting active competitions from the Dabble API.

        _parse_competitions(response) -> None:
            Processes competition data by identifying active competitions and initiating asynchronous event requests
            for each.

        _parse_events(response, league: str) -> None:
            Processes event data by fetching details for each displayed event, initiating prop line requests for each.

        _parse_lines(response, league: str, game_info: str) -> None:
            Processes player prop line data, extracting standardized details on subjects (players), markets,
            and prop line attributes. Updates shared betting line data if valid entries are found.

    Helper Functions:
        extract_league(data: dict) -> Optional[str]:
            Extracts and cleans the league name from competition data.

        extract_market_map(data: dict) -> Optional[dict]:
            Builds a map of market IDs to names for easier lookup in subsequent line processing.

        extract_market(bookmaker_name: str, data: dict, data_map: dict, league: str) -> Optional[tuple[Optional[str], str]]:
            Cleans and validates market names, returning the market ID and name if valid.

        extract_position(data: dict) -> Optional[str]:
            Extracts and cleans a player’s position information.

        extract_team(data: dict) -> Optional[str]:
            Extracts and standardizes the player's team abbreviation.

        extract_subject(bookmaker_name: str, data: dict, league: str) -> Union[tuple[Any, Any], tuple[None, None]]:
            Cleans player (subject) data, retrieving the player ID if it exists in the database.

        extract_label(data: dict) -> Optional[str]:
            Standardizes the label for player props (e.g., over/under).
    """

    def __init__(self, bookmaker: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)
        # gets universally used request headers
        self.headers = bkm_utils.get_headers(bookmaker.name)
        # gets universally used request cookies
        self.cookies = bkm_utils.get_cookies(bookmaker.name)

    async def retrieve(self) -> None:
        # gets the url required to request for the current competitions
        url = bkm_utils.get_url(self.name, name='competitions')
        # will make an asynchronous request for the competitions using valid request data
        await self.req_mngr.get(url, self._parse_competitions, headers=self.headers, cookies=self.cookies)

    async def _parse_competitions(self, response) -> None:
        # initializes a tasks list to store tasks that will be run asynchronously
        tasks = []
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # for each competition in the data's activeCompetitions if they exist
            for competition in data.get('activeCompetitions', []):
                # extract the league from competition and get the competition id, if both exist then execute
                if (league := extract_league(competition)) and (competition_id := competition.get('id')):
                    # to track the leagues being collected
                    dc_utils.RelevantData.update_relevant_leagues(league, self.name)
                    # get the url required to request the current events for each competition and insert comp id into it
                    url = bkm_utils.get_url(self.name, name='events').format(competition_id)
                    # get the params required to request the current events
                    params = bkm_utils.get_params(self.name)
                    # add the request task to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_events, league, params=params))

            # complete requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_events(self, response, league: str) -> None:
        # initializes a tasks list to store tasks that will be run asynchronously
        tasks = []
        # gets the json data from the response, if exists then execute
        if json_data := response.json():
            # for event in competition's events, if the events data exists
            for event in json_data.get('data', []):
                # gets the event id, game information, and checks whether this event is displayed, if all exist execute
                if (event_id := event.get('id')) and event.get('isDisplayed'):
                    # gets the url required to request for prop lines and inserts event id into url string
                    url = bkm_utils.get_url(self.name).format(event_id)
                    # add the request task to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_lines, league))

            # complete requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # get the sport for this league
            sport = dc_utils.LEAGUE_SPORT_MAP[league]
            # get market groups
            markets_map = extract_market_map(data)
            # for each prop line in data's player props if they exist
            for player_prop_data in data.get('playerProps', []):
                # get the market id from the db and extract the market name from the dictionary
                if market := extract_market(self.name, player_prop_data, markets_map, league):
                    # extract the player's team data
                    if team := extract_team(self.name, league, player_prop_data):
                        # use the team data to get game data
                        if game := dc_utils.get_game(league, team['abbr_name']):
                            # get the subject id from the db and extract the subject name from dictionary
                            if subject := extract_subject(self.name, player_prop_data, league, team):
                                # get over/under label for player prop and get numeric line, only execute if both exist
                                if (label := extract_label(player_prop_data)) and (line := player_prop_data.get('value')):
                                    # update shared data
                                    dc_utils.BettingLines.update({
                                        's_tstamp': str(datetime.now()),
                                        'bookmaker': self.name,
                                        'sport': sport,
                                        'league': league,
                                        'game_time': game['game_time'],
                                        'game': game['info'],
                                        'market_id': market['id'],
                                        'market': market['name'],
                                        'subject_id': subject['id'],
                                        'subject': subject['name'],
                                        'label': label,
                                        'line': line,
                                        'dflt_odds': self.dflt_odds,
                                        'dflt_im_prb': self.dflt_im_prb
                                    })
