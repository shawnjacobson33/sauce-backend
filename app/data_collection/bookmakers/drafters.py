import asyncio
from datetime import datetime
from typing import Optional

from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import utils as bkm_utils


def is_event_valid(data: dict) -> bool:
    # do not want futures
    return 'Season' not in data.get('_id')


def extract_game_info(data: dict) -> Optional[str]:
    # get the home team and away team from data, if they both exist then execute
    if (home_team := data.get('home')) and (away_team := data.get('away')):
        # returning game information
        return ' @ '.join([away_team, home_team])


def extract_subject_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the team the player is on and only return if it exists and doesn't equal MMA
    if (subject_team := data.get('own')) and (subject_team != 'MMA'):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team_id(bookmaker_name, league, subject_team):
            # return the team id and team name
            return team_data


def extract_subject(bookmaker_name: str, data: dict, league: str, team: Optional[dict]) -> Optional[dict[str, str]]:
    # get the player's name, if exists then execute
    if subject_name := data.get('player_name'):
        # get player attributes
        position = extract_position(data)
        # gets the subject id or log message
        subject = bkm_utils.get_subject_id(bookmaker_name, league, subject_name, team=team, position=position)
        # return both subject id search result and cleaned subject
        return subject


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get market name, execute if it exists
    if market_name := data.get('bid_stats_name'):
        # check if the market is valid...watching out for MMA markets
        if bkm_utils.is_market_valid(market_name):
            # gets the market id or log message
            market = bkm_utils.get_market_id(bookmaker_name, league, market_name)
            # return both market id search result and cleaned market
            return market


def extract_position(data: dict) -> Optional[str]:
    # get position from data if it exists and doesn't equal 'G'
    if (position := data.get('player_position')) and (position != 'G'):
        # return the position cleaned
        return bkm_utils.clean_position(position.strip())


class Drafters(bkm_utils.LinesRetriever):
    def __init__(self, bookmaker: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)

    async def retrieve(self) -> None:
        # get url to make a request for leagues
        url = bkm_utils.get_url(self.source.name, name='leagues')
        # get headers to make a request for prop lines
        headers = bkm_utils.get_headers(self.source.name, name='leagues')
        # make asynchronous request for prop lines
        await self.req_mngr.get(url, self._parse_leagues, headers=headers)

    async def _parse_leagues(self, response) -> None:
        # get the response data as json and get another dictionary
        if (json_data := response.json()) and (data := json_data.get('data')):
            # create a list to store requests
            tasks = list()
            # for each dictionary of data in entities
            for entity_data in data.get('entities', []):
                # get the league name and id
                if (league_name := entity_data.get('name')) and (league_id := entity_data.get('id')):
                    # clean the league name
                    cleaned_league = bkm_utils.clean_league(league_name)
                    # if the league is valid keep going
                    if bkm_utils.is_league_valid(cleaned_league):
                        # get the url to get prop lines data and insert the league id
                        url = bkm_utils.get_url(self.source.name).format(league_id)
                        # get the headers associated with prop lines requests
                        headers = bkm_utils.get_headers(self.source.name)
                        # store some params
                        params = {
                            'page_no': '1'
                        }
                        # add the request to the list of requests
                        tasks.append(self.req_mngr.get(url, self._parse_lines, cleaned_league, headers=headers, params=params))

            # start requesting asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str):
        # get response data, if exists execute
        if json_data := response.json():
            # to track the leagues being collected
            bkm_utils.Leagues.update_valid_leagues(self.source.name, league)
            # for each event in the data's entities
            for event_data in json_data.get('entities', []):
                # check if the event is valid before executing
                if is_event_valid(event_data):
                    # extract the player's team and some general game info from 'event'
                    game_info = extract_game_info(event_data)
                    # extract the player's team
                    team = extract_subject_team(self.source.name, league, event_data)
                    # for each player in event's players if they exist
                    for player_data in event_data.get('players', []):
                        # extract the subject id from db and get subject from player dict
                        if subject := extract_subject(self.source.name, player_data, league, team):
                            # get market id from db and extract market from player dict
                            if market := extract_market(self.source.name, player_data, league):
                                # get numeric over/under line and execute if exists
                                if line := player_data.get('bid_stats_value'):
                                    # for each label Over and Under update shared data
                                    for label in ['Over', 'Under']:
                                        # update shared data
                                        self.update_betting_lines({
                                            'batch_id': self.batch_id,
                                            'time_processed': str(datetime.now()),
                                            'league': league,
                                            'game_info': game_info,
                                            'market_category': 'player_props',
                                            'market_id': market['id'],
                                            'market': market['name'],
                                            'subject_id': subject['id'],
                                            'subject': subject['name'],
                                            'bookmaker': self.source.name,
                                            'label': label,
                                            'line': line,
                                            'odds': self.source.default_payout.odds
                                        })
