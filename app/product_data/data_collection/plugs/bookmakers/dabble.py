from datetime import datetime
import asyncio
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.bookmakers.helpers import run, is_league_valid, is_market_valid, clean_market, \
    clean_subject, clean_league, clean_position


def extract_league(data: dict) -> Optional[str]:
    # get league name and the competition id, if both exist then execute
    if league := data.get('displayName'):
        # clean the league name
        cleaned_league = clean_league(league)
        # if league is not valid then skip
        if is_league_valid(cleaned_league):
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


def extract_market(data: dict, data_map: dict, league: str) -> Optional[tuple[Optional[str], str]]:
    # get the market id from the response data, if that exists get the market name, if they both exist execute
    if (data_map_market_id := data.get('marketId')) and (market := data_map.get(data_map_market_id)):
        # check if the market is valid
        if is_market_valid(market):
            # clean the market name
            cleaned_market = clean_market(market, 'dabble')
            # get the market id from the db
            if market_id := get_market_id(Market(cleaned_market, league)):
                # cast the market id to a string
                market_id = str(market_id)

            # clean and return the market name and get the market id from the database
            return market_id, cleaned_market


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists keep going
    if position := data.get('position'):
        # return the cleaned position
        return clean_position(position)


def extract_team(data: dict) -> Optional[str]:
    # get the subject team name abbreviated, if exists keep going
    if subject_team := data.get('teamAbbreviation'):
        # return the team in all uppercase
        return subject_team.upper()


def extract_subject(data: dict, league: str) -> Optional[tuple[Optional[str], str]]:
    # get subject name from the data, if exists then execute
    if subject := data.get('playerName'):
        # clean the subject
        cleaned_subject = clean_subject(subject)
        # get the subject id from the database, if exists then execute
        if subject_id := get_subject_id(Subject(cleaned_subject, league, extract_team(data), extract_position(data))):
             # cast to a string
             subject_id = str(subject_id)

        # return the subject id from the database and extract attributes 'team' and 'position' from the data if they exist
        return subject_id, cleaned_subject


def extract_label(data: dict) -> Optional[str]:
    # get the label, if exists keep going
    if label := data.get('lineType'):
        # return the label capitalized
        return label.title()


class Dabble(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)
        # gets universally used request headers
        self.headers = self.req_packager.get_headers()
        # gets universally used request cookies
        self.cookies = self.req_packager.get_cookies()

    async def start(self) -> None:
        # gets the url required to request for the current competitions
        url = self.req_packager.get_url(name='competitions')
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
                    # get the url required to request the current events for each competition and insert comp id into it
                    url = self.req_packager.get_url(name='events').format(competition_id)
                    # get the params required to request the current events
                    params = self.req_packager.get_params()
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
                if (event_id := event.get('id')) and (game_info := event.get('name')) and event.get('isDisplayed'):
                    # gets the url required to request for prop lines and inserts event id into url string
                    url = self.req_packager.get_url().format(event_id)
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
                # extract the market name from player_prop and look up in markets_map and get market id from db
                market_id, market = extract_market(player_prop_data, markets_map, league)
                # only execute if both exist
                if market_id and market:
                    # extract subject name from player prop and get subject id from db
                    subject_id, subject = extract_subject(player_prop_data, league)
                    # only execute if both exist
                    if subject_id and subject:
                        # get over/under label for player prop and get numeric line, only execute if both exist
                        if (label := extract_label(player_prop_data)) and (line := player_prop_data.get('value')):
                            # update shared data
                            self.add_and_update({
                            'batch_id': self.batch_id,
                            'time_processed': str(datetime.now()),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market': market,
                            'subject_id': subject_id,
                            'subject': subject,
                            'bookmaker': self.info.name,
                            'label': label,
                            'line': line,
                            'odds': self.info.default_payout.odds
                        })



if __name__ == "__main__":
    asyncio.run(run(Dabble))
    Plug.save_to_file()
