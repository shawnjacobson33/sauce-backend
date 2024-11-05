from datetime import datetime
import asyncio
from typing import Optional, Union

from bson import ObjectId

from app.product_data.data_collection.plugs.bookmakers import utils


def extract_league(data: dict) -> Optional[str]:
    # get league name and the competition id, if both exist then execute
    if league := data.get('displayName'):
        # clean the league name
        cleaned_league = utils.clean_league(league)
        # if league is not valid then skip
        if utils.is_league_valid(cleaned_league):
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


def extract_market(bookmaker_name: str, data: dict, data_map: dict, league: str) -> Optional[tuple[Union[ObjectId, str], str]]:
    # get the market id from the response data, if that exists get the market name, if they both exist execute
    if (data_map_market_id := data.get('marketId')) and (market_name := data_map.get(data_map_market_id)):
        # check if the market is valid
        if utils.is_market_valid(market_name):
            # create a market object
            market_obj = utils.Market(market_name, league)
            # gets the market id or log message
            market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
            # return both market id search result and cleaned market
            return market_id, market_name

    return None, None


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists keep going
    if position := data.get('position'):
        # return the cleaned position
        return utils.clean_position(position)


def extract_team(data: dict) -> Optional[str]:
    # get the subject team name abbreviated, if exists keep going
    if subject_team := data.get('teamAbbreviation'):
        # return the team in all uppercase
        return subject_team.upper()


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[Union[ObjectId, str], str]]:
    # get subject name from the data, if exists then execute
    if subject_name := data.get('playerName'):
        # extract player attributes
        subject_team, position = extract_team(data), extract_position(data)
        # create a subject object
        subject_obj = utils.Subject(subject_name, league, team=subject_team, position=position)
        # gets the subject id or log message
        subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
        # return both subject id search result and cleaned subject
        return subject_id, subject_name

    return None, None


def extract_label(data: dict) -> Optional[str]:
    # get the label, if exists keep going
    if label := data.get('lineType'):
        # return the label capitalized
        return label.title()


class Dabble(utils.BookmakerPlug):
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

        extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[Union[ObjectId, str], str]]:
            Cleans player (subject) data, retrieving the player ID if it exists in the database.

        extract_label(data: dict) -> Optional[str]:
            Standardizes the label for player props (e.g., over/under).
    """

    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)
        # gets universally used request headers
        self.headers = utils.get_headers(bookmaker_info.name)
        # gets universally used request cookies
        self.cookies = utils.get_cookies(bookmaker_info.name)

    async def collect(self) -> None:
        # gets the url required to request for the current competitions
        url = utils.get_url(self.bookmaker_info.name, name='competitions')
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
                    self.metrics.add_league(league)
                    # get the url required to request the current events for each competition and insert comp id into it
                    url = utils.get_url(self.bookmaker_info.name, name='events').format(competition_id)
                    # get the params required to request the current events
                    params = utils.get_params(self.bookmaker_info.name)
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
                    if game_info := event.get('name'):
                        # gets the url required to request for prop lines and inserts event id into url string
                        url = utils.get_url(self.bookmaker_info.name).format(event_id)
                        # add the request task to tasks
                        tasks.append(self.req_mngr.get(url, self._parse_lines, league, game_info))

            # complete requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str, game_info: str) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # get market groups
            markets_map = extract_market_map(data)
            # for each prop line in data's player props if they exist
            for player_prop_data in data.get('playerProps', []):
                # get the market id from the db and extract the market name from the dictionary
                market_id, market_name = extract_market(self.bookmaker_info.name, player_prop_data, markets_map, league)
                # if both exist then keep going
                if market_id and market_name:
                    # to track the markets being collected
                    self.metrics.add_market((league, market_name))
                    # get the subject id from the db and extract the subject name from dictionary
                    subject_id, subject_name = extract_subject(self.bookmaker_info.name, player_prop_data, league)
                    # if both exist then keep going
                    if subject_id and subject_name:
                        # to track the subjects being collected
                        self.metrics.add_subject((league, subject_name))
                        # get over/under label for player prop and get numeric line, only execute if both exist
                        if (label := extract_label(player_prop_data)) and (line := player_prop_data.get('value')):
                            # update shared data
                            self.update_betting_lines({
                                'batch_id': self.batch_id,
                                'time_processed': str(datetime.now()),
                                'league': league,
                                'game_info': game_info,
                                'market_category': 'player_props',
                                'market_id': str(market_id),
                                'market': market_name,
                                'subject_id': str(subject_id),
                                'subject': subject_name,
                                'bookmaker': self.bookmaker_info.name,
                                'label': label,
                                'line': line,
                                'odds': self.bookmaker_info.default_payout.odds
                            })



if __name__ == "__main__":
    utils.run(Dabble)
